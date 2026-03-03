const axios = require('axios');

const GRAPH_BASE = 'https://graph.microsoft.com/v1.0';
const REQUEST_TIMEOUT = 120000; // 120 segundos

class GraphClient {
  constructor(authInstance, config, logger = null) {
    this.auth = authInstance;
    this.retryAttempts = config.retry_attempts || 5;
    this.retryDelay = config.retry_delay_ms || 2000;
    this.throttleDelay = config.throttle_delay_ms || 1000;
    this.logger = logger;
    this.requestCount = 0;
  }

  async _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  _log(level, message) {
    if (this.logger) {
      this.logger[level](message);
    }
  }

  async request(method, url, data = null, extraHeaders = {}, attempt = 1, responseType = 'json') {
    try {
      const headers = await this.auth.getHeaders();
      const fullUrl = url.startsWith('http') ? url : `${GRAPH_BASE}${url}`;
      
      this.requestCount++;

      const startTime = Date.now();
      const response = await axios({
        method,
        url: fullUrl,
        headers: { ...headers, ...extraHeaders },
        data: data || undefined,
        responseType,
        timeout: REQUEST_TIMEOUT,
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
        validateStatus: null
      });

      const duration = Date.now() - startTime;
      
      // Log apenas responses bem-sucedidas de forma compacta
      if (response.status >= 200 && response.status < 300) {
        const operation = method === 'GET' ? '📥' : method === 'POST' ? '📤' : '🔄';
        this._log('info', `   ${operation} API OK: HTTP ${response.status} (${duration}ms)`);
      }

      // 429 = Too Many Requests (throttled)
      if (response.status === 429) {
        const retryAfter = parseInt(response.headers['retry-after'] || '10') * 1000;
        this._log('warn', `   ⏸️  Rate limited! Waiting ${retryAfter/1000}s...`);
        await this._sleep(retryAfter);
        return this.request(method, url, data, extraHeaders, attempt, responseType);
      }

      // 503 or 504 = retry
      if ((response.status === 503 || response.status === 504) && attempt <= this.retryAttempts) {
        this._log('warn', `   🔄 Server error ${response.status}, retrying (${attempt}/${this.retryAttempts})...`);
        await this._sleep(this.retryDelay * attempt);
        return this.request(method, url, data, extraHeaders, attempt + 1, responseType);
      }

      if (response.status >= 400) {
        let errMsg;
        if (response.data) {
          if (typeof response.data === 'string') {
            try {
              const parsed = JSON.parse(response.data);
              errMsg = parsed?.error?.message || response.data;
            } catch (e) {
              errMsg = response.data;
            }
          } else {
            errMsg = response.data?.error?.message || JSON.stringify(response.data);
          }
        } else {
          errMsg = `HTTP ${response.status} with no error message`;
        }
        throw new Error(`HTTP ${response.status}: ${errMsg}`);
      }

      await this._sleep(this.throttleDelay);

      // For calls that expect text, try to parse as JSON for convenience of callers
      if (responseType === 'text' && typeof response.data === 'string') {
        try {
          return JSON.parse(response.data);
        } catch (e) {
          return response.data;
        }
      }
      return response.data;

    } catch (err) {
      // Timeout específico
      if (err.code === 'ECONNABORTED' && err.message.includes('timeout')) {
        this._log('error', `   ⏱️  Request timeout after ${REQUEST_TIMEOUT/1000}s!`);
        if (attempt <= this.retryAttempts) {
          this._log('warn', `   🔄 Retrying (${attempt}/${this.retryAttempts})...`);
          await this._sleep(this.retryDelay * attempt);
          return this.request(method, url, data, extraHeaders, attempt + 1, responseType);
        }
        throw new Error(`Request timeout after ${this.retryAttempts} attempts`);
      }
      
      // Network errors
      if (err.code === 'ECONNRESET' || err.code === 'ETIMEDOUT') {
        this._log('warn', `   🔌 Network error: ${err.code}`);
        if (attempt <= this.retryAttempts) {
          this._log('warn', `   🔄 Retrying (${attempt}/${this.retryAttempts})...`);
          await this._sleep(this.retryDelay * attempt);
          return this.request(method, url, data, extraHeaders, attempt + 1, responseType);
        }
      }
      throw err;
    }
  }

  async get(url, params = {}) {
    const queryString = Object.keys(params).length
      ? '?' + new URLSearchParams(params).toString()
      : '';
    return this.request('GET', url + queryString);
  }

  async post(url, data) {
    return this.request('POST', url, data);
  }

  async getRaw(url, extraHeaders = {}) {
    return this.request('GET', url, null, extraHeaders, 1, 'arraybuffer');
  }

  async postRaw(url, data, extraHeaders = {}) {
    return this.request('POST', url, data, extraHeaders, 1, 'text');
  }

  async put(url, data, extraHeaders = {}) {
    return this.request('PUT', url, data, extraHeaders);
  }

  async patch(url, data) {
    return this.request('PATCH', url, data);
  }

  // Paginate through all pages of a Graph API collection
  async *paginate(url, params = {}) {
    let nextUrl = url;
    let isFirst = true;
    let pageNum = 0;
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
    
    // Log apenas resumo final
    if (totalItems > 0) {
      this._log('info', `   📦 Loaded ${totalItems} item(s) from ${pageNum} page(s)`);
    }
  }
}

module.exports = GraphClient;