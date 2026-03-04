/**
 * Calendar & Contacts Migration Module
 */

class CalendarMigrator {
  constructor(sourceClient, targetClient, config, logger) {
    this.src = sourceClient;
    this.tgt = targetClient;
    this.config = config;
    this.logger = logger;
  }

  async migrateCalendar(sourceEmail, targetEmail, checkpoint = {}) {
    this.logger.info(`Starting calendar migration: ${sourceEmail} → ${targetEmail}`);
    const stats = { total: 0, migrated: 0, skipped: 0, failed: 0 };

    try {
      // Get all calendars
      const calendars = [];
      for await (const cal of this.src.paginate(`/users/${sourceEmail}/calendars`)) {
        calendars.push(cal);
      }

      this.logger.info(`Found ${calendars.length} calendars`);

      for (const calendar of calendars) {
        const calKey = `calendar_${calendar.id}`;

        // Find or create matching calendar in target
        let tgtCalendarId;
        if (calendar.isDefaultCalendar) {
          const tgtDefault = await this.tgt.get(`/users/${targetEmail}/calendar`);
          tgtCalendarId = tgtDefault.id;
        } else {
          tgtCalendarId = await this._ensureCalendar(targetEmail, calendar.name);
        }

        // Migrate events
        for await (const event of this.src.paginate(
          `/users/${sourceEmail}/calendars/${calendar.id}/events`,
          { '$select': 'id,subject,body,start,end,location,attendees,isAllDay,recurrence,importance,sensitivity,isReminderOn,reminderMinutesBeforeStart,organizer,bodyPreview' }
        )) {
          const evtKey = `cal_event_${event.id}`;

          if (checkpoint[evtKey]) {
            stats.skipped++;
            continue;
          }

          stats.total++;

          if (this.config.dry_run) {
            this.logger.info(`[DRY RUN] Event: ${event.subject}`);
            stats.migrated++;
            continue;
          }

          try {
            await this._createEvent(targetEmail, tgtCalendarId, event);
            checkpoint[evtKey] = 'done';
            stats.migrated++;
          } catch (err) {
            this.logger.error(`Failed event "${event.subject}": ${err.message}`);
            stats.failed++;
          }
        }
      }

      this.logger.success(`Calendar done: ${stats.migrated} migrated, ${stats.failed} failed`);
      return { success: true, stats };

    } catch (err) {
      this.logger.error(`Calendar migration failed: ${err.message}`);
      return { success: false, error: err.message, stats };
    }
  }

  async migrateContacts(sourceEmail, targetEmail, checkpoint = {}) {
    this.logger.info(`Starting contacts migration: ${sourceEmail} → ${targetEmail}`);
    const stats = { total: 0, migrated: 0, skipped: 0, failed: 0 };

    try {
      for await (const contact of this.src.paginate(
        `/users/${sourceEmail}/contacts`,
        { '$select': 'id,displayName,emailAddresses,phones,businessAddress,homeAddress,jobTitle,companyName,department,birthday,notes,personalNotes' }
      )) {
        const contactKey = `contact_${contact.id}`;

        if (checkpoint[contactKey]) {
          stats.skipped++;
          continue;
        }

        stats.total++;

        if (this.config.dry_run) {
          this.logger.info(`[DRY RUN] Contact: ${contact.displayName}`);
          stats.migrated++;
          continue;
        }

        try {
          const payload = {
            displayName: contact.displayName,
            emailAddresses: contact.emailAddresses || [],
            phones: contact.phones || [],
            businessAddress: contact.businessAddress,
            homeAddress: contact.homeAddress,
            jobTitle: contact.jobTitle,
            companyName: contact.companyName,
            department: contact.department,
            birthday: contact.birthday,
            personalNotes: contact.personalNotes
          };

          // Remove undefined fields
          Object.keys(payload).forEach(k => payload[k] === undefined && delete payload[k]);

          await this.tgt.post(`/users/${targetEmail}/contacts`, payload);
          checkpoint[contactKey] = 'done';
          stats.migrated++;
        } catch (err) {
          this.logger.error(`Failed contact "${contact.displayName}": ${err.message}`);
          stats.failed++;
        }
      }

      this.logger.success(`Contacts done: ${stats.migrated} migrated, ${stats.failed} failed`);
      return { success: true, stats };

    } catch (err) {
      this.logger.error(`Contacts migration failed: ${err.message}`);
      return { success: false, error: err.message, stats };
    }
  }

  async _ensureCalendar(userEmail, calendarName) {
    // Check if exists
    for await (const cal of this.tgt.paginate(`/users/${userEmail}/calendars`)) {
      if (cal.name === calendarName) return cal.id;
    }

    // Create it
    const newCal = await this.tgt.post(`/users/${userEmail}/calendars`, {
      name: calendarName
    });
    return newCal.id;
  }

  async _createEvent(userEmail, calendarId, event) {
    const payload = {
      subject: event.subject || '(sem título)',
      body: event.body || { contentType: 'text', content: '' },
      start: event.start,
      end: event.end,
      location: event.location,
      isAllDay: event.isAllDay || false,
      recurrence: event.recurrence,
      importance: event.importance || 'normal',
      sensitivity: event.sensitivity || 'normal',
      isReminderOn: event.isReminderOn,
      reminderMinutesBeforeStart: event.reminderMinutesBeforeStart
      // Note: attendees are excluded to avoid sending invites to all attendees
    };

    // Remove null/undefined fields
    Object.keys(payload).forEach(k =>
      (payload[k] === undefined || payload[k] === null) && delete payload[k]
    );

    return await this.tgt.post(
      `/users/${userEmail}/calendars/${calendarId}/events`,
      payload
    );
  }
}

module.exports = CalendarMigrator;
