const axios = require('axios');

const GRAPH_BASE = 'https://graph.microsoft.com/v1.0';
const REQUEST_TIMEOUT = 120000; // 120s — large mailboxes need time

class GraphClient {
  constructor(authInstance, config, logger = null) {
    this.auth          = authInstance;
    this.retryAttempts = config.retry_attempts   || 5;
    this.retryDelay    = config.retry_delay_ms   || 2000;
    this.throttleDelay = config.throttle_delay_ms || 150;
    this.logger        = logger;
    this.requestCount  = 0;
  }

  _sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

  _log(level, msg) { if (this.logger) this.logger[level](msg); }

  async request(method, url, data = null, extraHeaders = {}, attempt = 1, responseType = 'json') {
    try {
      const headers = await this.auth.getHeaders();
      const fullUrl = url.startsWith('http') ? url : `${GRAPH_BASE}${url}`;

      this.requestCount++;
      const reqId     = this.requestCount;
      const startTime = Date.now();

      const response = await axios({
        method,
        url: fullUrl,
        headers: { ...headers, ...extraHeaders },
        data:    data || undefined,
        responseType,
        timeout:            REQUEST_TIMEOUT,
        maxContentLength:   Infinity,
        maxBodyLength:      Infinity,
        validateStatus:     null
      });

      const duration = Date.now() - startTime;

      // Log every 5th request or slow ones (>3s)
      if (response.status >= 200 && response.status < 300) {
        if (reqId % 5 === 0 || duration > 3000) {
          const op = { GET: '📥', POST: '📤', DELETE: '🗑️', PATCH: '🔧' }[method] || '🔄';
          this._log('info', `   ${op} API: HTTP ${response.status} (${duration}ms)`);
        }
      }

      // 429 — rate limited: respect Retry-After exactly
      if (response.status === 429) {
        const retryAfter = parseInt(response.headers['retry-after'] || '10') * 1000;
        this._log('warn', `   ⏸️  Rate limited! Waiting ${retryAfter/1000}s...`);
        await this._sleep(retryAfter);
        return this.request(method, url, data, extraHeaders, attempt, responseType);
      }

      // 503/504 — server overloaded: exponential backoff
      if ((response.status === 503 || response.status === 504) && attempt <= this.retryAttempts) {
        const backoff = this.retryDelay * Math.pow(2, attempt - 1); // 2s, 4s, 8s, 16s, 32s
        this._log('warn', `   🔄 Server error ${response.status}, retry ${attempt}/${this.retryAttempts} in ${backoff/1000}s...`);
        await this._sleep(backoff);
        return this.request(method, url, data, extraHeaders, attempt + 1, responseType);
      }

      if (response.status >= 400) {
        let errMsg;
        if (response.data) {
          if (typeof response.data === 'string') {
            try { errMsg = JSON.parse(response.data)?.error?.message || response.data; }
            catch { errMsg = response.data; }
          } else {
            errMsg = response.data?.error?.message || JSON.stringify(response.data);
          }
        } else {
          errMsg = `HTTP ${response.status} with no body`;
        }
        throw new Error(`HTTP ${response.status}: ${errMsg}`);
      }

      // Polite throttle between successful requests
      await this._sleep(this.throttleDelay);

      if (responseType === 'text' && typeof response.data === 'string') {
        try { return JSON.parse(response.data); } catch { return response.data; }
      }
      return response.data;

    } catch (err) {
      // Network-level errors: exponential backoff
      if (['ECONNABORTED', 'ECONNRESET', 'ETIMEDOUT', 'ENOTFOUND', 'EAI_AGAIN'].includes(err.code)) {
        if (attempt <= this.retryAttempts) {
          const backoff = this.retryDelay * Math.pow(2, attempt - 1);
          this._log('warn', `   🔌 Network error ${err.code}, retry ${attempt}/${this.retryAttempts} in ${backoff/1000}s...`);
          await this._sleep(backoff);
          return this.request(method, url, data, extraHeaders, attempt + 1, responseType);
        }
      }
      throw err;
    }
  }

  async get(url, params = {}) {
    const qs = Object.keys(params).length ? '?' + new URLSearchParams(params).toString() : '';
    return this.request('GET', url + qs);
  }

  async post(url, data)                    { return this.request('POST',   url, data); }
  async patch(url, data)                   { return this.request('PATCH',  url, data); }
  async put(url, data, extraHeaders = {})  { return this.request('PUT',    url, data, extraHeaders); }
  async delete(url)                        { return this.request('DELETE', url); }
  async getRaw(url, extraHeaders = {})     { return this.request('GET',    url, null, extraHeaders, 1, 'arraybuffer'); }
  async postRaw(url, data, extraHeaders={}) { return this.request('POST',  url, data, extraHeaders, 1, 'text'); }

  // Paginate through Graph API collections
  async *paginate(url, params = {}, context = 'items') {
    let nextUrl   = url;
    let isFirst   = true;
    let pageNum   = 0;
    let totalItems = 0;

    while (nextUrl) {
      pageNum++;
      const result = isFirst
        ? await this.get(url, params)
        : await this.request('GET', nextUrl);

      isFirst = false;

      if (result.value) {
        totalItems += result.value.length;
        yield* result.value;
      }

      nextUrl = result['@odata.nextLink'] || null;
    }

    if (totalItems > 0) {
      this._log('info', `   📦 Loaded ${totalItems} ${context} (${pageNum} page${pageNum > 1 ? 's' : ''})`);
    }
  }
}

module.exports = GraphClient;
