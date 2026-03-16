/**
 * Calendar & Contacts Migration Module v2
 *
 * v2 fixes vs v1:
 * - Dedup on events (by sourceEventId MAPI property, fallback subject+start)
 * - Dedup on contacts (by sourceContactId property, fallback email+name)
 * - Attendees now included (were excluded, breaking meeting context)
 * - Online meeting / Teams links preserved (isOnlineMeeting, onlineMeetingProvider)
 * - Categories migrated
 * - Quota retry: 60s pause + 3 retries (same as fixDrafts)
 * - 60s between users, 5s between calendars
 */

'use strict';

const QUOTA_RETRY_WAIT_MS = 60000;
const MAX_RETRIES         = 3;
const SOURCE_EVENT_PROP   = 'String {8ECCC264-6880-4EBE-992F-8888D2EEAA1D} Name SourceEventId';
const SOURCE_CONTACT_PROP = 'String {8ECCC264-6880-4EBE-992F-8888D2EEAA1D} Name SourceContactId';

class CalendarMigrator {
  constructor(sourceClient, targetClient, config, logger) {
    this.src    = sourceClient;
    this.tgt    = targetClient;
    this.config = config;
    this.logger = logger;
  }

  _sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

  // ── Retry wrapper for quota errors ────────────────────────────────────────
  async _withQuotaRetry(label, fn) {
    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      try {
        return await fn();
      } catch (err) {
        const msg = err.message || '';
        const isQuota = msg.includes('Request limit') || msg.includes('MailboxConcurrency') ||
                        msg.includes('Too Many') || msg.includes('throttl') || msg.includes('429');
        if (isQuota && attempt < MAX_RETRIES) {
          this.logger.warn(`   ⏸️  Quota hit on "${label}" — pausing 60s (attempt ${attempt+1}/${MAX_RETRIES})...`);
          await this._sleep(QUOTA_RETRY_WAIT_MS);
        } else {
          throw err;
        }
      }
    }
  }

  // ── Build dedup index for events ──────────────────────────────────────────
  async _buildEventIndex(userEmail, calendarId) {
    const bySourceId   = new Set();
    const byFallback   = new Set(); // subject|startDateTime
    try {
      const expand = `singleValueExtendedProperties($filter=id eq '${SOURCE_EVENT_PROP}')`;
      for await (const evt of this.tgt.paginate(
        `/users/${userEmail}/calendars/${calendarId}/events`,
        { '$expand': expand, '$select': 'id,subject,start', '$top': 100 }
      )) {
        const prop = evt.singleValueExtendedProperties?.find(p => p.id === SOURCE_EVENT_PROP);
        if (prop?.value) {
          bySourceId.add(prop.value);
        } else if (evt.subject && evt.start?.dateTime) {
          byFallback.add(`${evt.subject}|${evt.start.dateTime}`);
        }
      }
    } catch (e) {
      this.logger.warn(`   Could not build event dedup index: ${e.message}`);
    }
    return { bySourceId, byFallback };
  }

  // ── Build dedup index for contacts ────────────────────────────────────────
  async _buildContactIndex(userEmail) {
    const bySourceId = new Set();
    const byFallback = new Set(); // email|name
    try {
      const expand = `singleValueExtendedProperties($filter=id eq '${SOURCE_CONTACT_PROP}')`;
      for await (const c of this.tgt.paginate(
        `/users/${userEmail}/contacts`,
        { '$expand': expand, '$select': 'id,displayName,emailAddresses', '$top': 100 }
      )) {
        const prop = c.singleValueExtendedProperties?.find(p => p.id === SOURCE_CONTACT_PROP);
        if (prop?.value) {
          bySourceId.add(prop.value);
        } else {
          const email = c.emailAddresses?.[0]?.address || '';
          byFallback.add(`${email}|${c.displayName}`);
        }
      }
    } catch (e) {
      this.logger.warn(`   Could not build contact dedup index: ${e.message}`);
    }
    return { bySourceId, byFallback };
  }

  // ── Migrate calendars + events ────────────────────────────────────────────
  async migrateCalendar(sourceEmail, targetEmail, checkpoint = {}) {
    this.logger.info(`Starting calendar migration: ${sourceEmail} → ${targetEmail}`);
    const stats = { total: 0, migrated: 0, skipped: 0, failed: 0 };

    try {
      const calendars = [];
      for await (const cal of this.src.paginate(`/users/${sourceEmail}/calendars`)) {
        calendars.push(cal);
      }
      this.logger.info(`Found ${calendars.length} calendars`);

      for (const calendar of calendars) {
        // Find or create matching calendar in target
        let tgtCalendarId;
        if (calendar.isDefaultCalendar) {
          const tgtDefault  = await this.tgt.get(`/users/${targetEmail}/calendar`);
          tgtCalendarId = tgtDefault.id;
        } else {
          tgtCalendarId = await this._ensureCalendar(targetEmail, calendar.name);
        }

        // Build dedup index for this calendar
        const idx = await this._buildEventIndex(targetEmail, tgtCalendarId);
        this.logger.info(`   📅 ${calendar.name}: ${idx.bySourceId.size} events already in target`);

        let calMigrated = 0, calSkipped = 0, calFailed = 0;

        for await (const event of this.src.paginate(
          `/users/${sourceEmail}/calendars/${calendar.id}/events`,
          {
            '$select': 'id,subject,body,start,end,location,attendees,isAllDay,recurrence,' +
                       'importance,sensitivity,isReminderOn,reminderMinutesBeforeStart,' +
                       'organizer,categories,isOnlineMeeting,onlineMeetingProvider,onlineMeeting'
          }
        )) {
          const evtKey = `cal_event_${event.id}`;
          stats.total++;

          if (checkpoint[evtKey]) { stats.skipped++; calSkipped++; continue; }

          // Dedup check
          const fallbackKey = `${event.subject}|${event.start?.dateTime}`;
          if (idx.bySourceId.has(event.id) || idx.byFallback.has(fallbackKey)) {
            checkpoint[evtKey] = 'done';
            stats.skipped++; calSkipped++;
            continue;
          }

          if (this.config.dry_run) {
            this.logger.info(`[DRY RUN] Event: ${event.subject}`);
            stats.migrated++; calMigrated++;
            continue;
          }

          try {
            await this._withQuotaRetry(event.subject, () =>
              this._createEvent(targetEmail, tgtCalendarId, event)
            );
            checkpoint[evtKey] = 'done';
            idx.bySourceId.add(event.id);
            stats.migrated++; calMigrated++;
          } catch (err) {
            this.logger.error(`   ✗ Event "${event.subject}": ${err.message}`);
            stats.failed++; calFailed++;
          }
        }

        this.logger.info(`   ✅ ${calendar.name}: ${calMigrated} migrated, ${calSkipped} skipped, ${calFailed} failed`);
        await this._sleep(5000); // 5s between calendars
      }

      this.logger.success(`Calendar done: ${stats.migrated} migrated, ${stats.skipped} skipped, ${stats.failed} failed`);
      return { success: true, stats };

    } catch (err) {
      this.logger.error(`Calendar migration failed: ${err.message}`);
      return { success: false, error: err.message, stats };
    }
  }

  // ── Migrate contacts ──────────────────────────────────────────────────────
  async migrateContacts(sourceEmail, targetEmail, checkpoint = {}) {
    this.logger.info(`Starting contacts migration: ${sourceEmail} → ${targetEmail}`);
    const stats = { total: 0, migrated: 0, skipped: 0, failed: 0 };

    try {
      const idx = await this._buildContactIndex(targetEmail);
      this.logger.info(`   ${idx.bySourceId.size} contacts already in target (dedup protection)`);

      for await (const contact of this.src.paginate(
        `/users/${sourceEmail}/contacts`,
        {
          '$select': 'id,displayName,emailAddresses,phones,businessAddress,homeAddress,' +
                     'jobTitle,companyName,department,birthday,personalNotes,categories,nickName'
        }
      )) {
        const contactKey = `contact_${contact.id}`;
        stats.total++;

        if (checkpoint[contactKey]) { stats.skipped++; continue; }

        // Dedup
        const primaryEmail  = contact.emailAddresses?.[0]?.address || '';
        const fallbackKey   = `${primaryEmail}|${contact.displayName}`;
        if (idx.bySourceId.has(contact.id) || idx.byFallback.has(fallbackKey)) {
          checkpoint[contactKey] = 'done';
          stats.skipped++;
          continue;
        }

        if (this.config.dry_run) {
          this.logger.info(`[DRY RUN] Contact: ${contact.displayName}`);
          stats.migrated++;
          continue;
        }

        try {
          const payload = {
            displayName:     contact.displayName,
            emailAddresses:  contact.emailAddresses  || [],
            phones:          contact.phones          || [],
            businessAddress: contact.businessAddress,
            homeAddress:     contact.homeAddress,
            jobTitle:        contact.jobTitle,
            companyName:     contact.companyName,
            department:      contact.department,
            birthday:        contact.birthday,
            personalNotes:   contact.personalNotes,
            nickName:        contact.nickName,
            categories:      contact.categories?.length ? contact.categories : undefined,
            singleValueExtendedProperties: [
              { id: SOURCE_CONTACT_PROP, value: contact.id }
            ]
          };
          Object.keys(payload).forEach(k => payload[k] === undefined && delete payload[k]);

          await this._withQuotaRetry(contact.displayName, () =>
            this.tgt.post(`/users/${targetEmail}/contacts`, payload)
          );
          checkpoint[contactKey] = 'done';
          idx.bySourceId.add(contact.id);
          stats.migrated++;
        } catch (err) {
          this.logger.error(`   ✗ Contact "${contact.displayName}": ${err.message}`);
          stats.failed++;
        }
      }

      this.logger.success(`Contacts done: ${stats.migrated} migrated, ${stats.skipped} skipped, ${stats.failed} failed`);
      return { success: true, stats };

    } catch (err) {
      this.logger.error(`Contacts migration failed: ${err.message}`);
      return { success: false, error: err.message, stats };
    }
  }

  // ── Find or create calendar ───────────────────────────────────────────────
  async _ensureCalendar(userEmail, calendarName) {
    for await (const cal of this.tgt.paginate(`/users/${userEmail}/calendars`)) {
      if (cal.name === calendarName) return cal.id;
    }
    const newCal = await this.tgt.post(`/users/${userEmail}/calendars`, { name: calendarName });
    return newCal.id;
  }

  // ── Create event with full fields ─────────────────────────────────────────
  async _createEvent(userEmail, calendarId, event) {
    const payload = {
      subject:                      event.subject || '(sem título)',
      body:                         event.body || { contentType: 'text', content: '' },
      start:                        event.start,
      end:                          event.end,
      location:                     event.location,
      isAllDay:                     event.isAllDay || false,
      recurrence:                   event.recurrence,
      importance:                   event.importance   || 'normal',
      sensitivity:                  event.sensitivity  || 'normal',
      isReminderOn:                 event.isReminderOn,
      reminderMinutesBeforeStart:   event.reminderMinutesBeforeStart,
      attendees:                    event.attendees    || [],  // included — preserves meeting context
      categories:                   event.categories?.length ? event.categories : undefined,
      isOnlineMeeting:              event.isOnlineMeeting,
      onlineMeetingProvider:        event.onlineMeetingProvider,
      singleValueExtendedProperties: [
        { id: SOURCE_EVENT_PROP, value: event.id }   // source ID for dedup
      ]
    };

    Object.keys(payload).forEach(k =>
      (payload[k] === undefined || payload[k] === null) && delete payload[k]
    );

    return await this.tgt.post(`/users/${userEmail}/calendars/${calendarId}/events`, payload);
  }
}

module.exports = CalendarMigrator;
