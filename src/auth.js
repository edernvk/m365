const msal = require('@azure/msal-node');

const GRAPH_SCOPES = ['https://graph.microsoft.com/.default'];

class TenantAuth {
  constructor(tenantConfig, label) {
    this.label = label;
    this.config = tenantConfig;
    this.tokenCache = null;
    this.tokenExpiry = null;

    this.msalApp = new msal.ConfidentialClientApplication({
      auth: {
        clientId: tenantConfig.client_id,
        authority: `https://login.microsoftonline.com/${tenantConfig.tenant_id}`,
        clientSecret: tenantConfig.client_secret,
      },
      system: {
        loggerOptions: {
          loggerCallback: () => {},
          piiLoggingEnabled: false,
          logLevel: msal.LogLevel.Error
        }
      }
    });
  }

  async getToken() {
    // Return cached token if still valid (with 5min buffer)
    if (this.tokenCache && this.tokenExpiry && Date.now() < this.tokenExpiry - 300000) {
      return this.tokenCache;
    }

    try {
      const result = await this.msalApp.acquireTokenByClientCredential({
        scopes: GRAPH_SCOPES
      });

      if (!result || !result.accessToken) {
        throw new Error(`No access token returned for ${this.label}`);
      }

      this.tokenCache = result.accessToken;
      this.tokenExpiry = result.expiresOn ? result.expiresOn.getTime() : Date.now() + 3600000;
      return this.tokenCache;
    } catch (err) {
      throw new Error(`Auth failed for ${this.label}: ${err.message}`);
    }
  }

  async getHeaders() {
    const token = await this.getToken();
    return {
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json'
    };
  }
}

module.exports = TenantAuth;
