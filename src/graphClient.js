const axios = require('axios');

const GRAPH_BASE = 'https://graph.microsoft.com/v1.0';

class GraphClient {
  constructor(authInstance, config) {
    this.auth = authInstance;
    this.retryAttempts = config.retry_attempts || 5;
    this.retryDelay = config.retry_delay_ms || 2000;
    this.throttleDelay = config.throttle_delay_ms || 1000;
  }

  async _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  async request(method, url, data = null, extraHeaders = {}, attempt = 1, responseType = 'json') {
    try {
      const headers = await this.auth.getHeaders();
      const fullUrl = url.startsWith('http') ? url : `${GRAPH_BASE}${url}`;

      const response = await axios({
        method,
        url: fullUrl,
        headers: { ...headers, ...extraHeaders },
        data: data || undefined,
        responseType,
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
        validateStatus: null
      });

      // 429 = Too Many Requests (throttled)
      if (response.status === 429) {
        const retryAfter = parseInt(response.headers['retry-after'] || '10') * 1000;
        await this._sleep(retryAfter);
        return this.request(method, url, data, extraHeaders, attempt, responseType);
      }

      // 503 or 504 = retry
      if ((response.status === 503 || response.status === 504) && attempt <= this.retryAttempts) {
        await this._sleep(this.retryDelay * attempt);
        return this.request(method, url, data, extraHeaders, attempt + 1, responseType);
      }

      if (response.status >= 400) {
        const errMsg = response.data?.error?.message || JSON.stringify(response.data);
        throw new Error(`HTTP ${response.status}: ${errMsg}`);
      }

      await this._sleep(this.throttleDelay);
      return response.data;

    } catch (err) {
      if (err.code === 'ECONNRESET' || err.code === 'ETIMEDOUT') {
        if (attempt <= this.retryAttempts) {
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
    return this.request('GET', url, null, extraHeaders, 1, 'text');
  }

  async postRaw(url, data, extraHeaders = {}) {
    return this.request('POST', url, data, extraHeaders, 1, 'json');
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

    while (nextUrl) {
      const result = isFirst
        ? await this.get(url, params)
        : await this.request('GET', nextUrl);

      isFirst = false;
      if (result.value) yield* result.value;
      nextUrl = result['@odata.nextLink'] || null;
    }
  }
}

module.exports = GraphClient;
