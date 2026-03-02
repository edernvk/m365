# M365 Tenant-to-Tenant Migrator

Ferramenta containerizada para migrar **Email + OneDrive + Calendário + Contatos** entre dois tenants Microsoft 365, usando a Graph API com suporte a retry, throttling, checkpoint (resume) e log por usuário.

---

## ✅ O que migra

| Workload       | O que é copiado                                          |
|----------------|----------------------------------------------------------|
| **email**      | Todos e-mails, pastas, subpastas, flags, read/unread     |
| **onedrive**   | Todos arquivos e pastas (estrutura preservada, chunks)   |
| **calendar**   | Eventos, recorrências, lembretes, múltiplos calendários  |
| **contacts**   | Contatos pessoais, telefones, endereços, notas           |

---

## 📋 Pré-requisitos

### 1. Criar App Registration no Tenant A (ORIGEM)

1. Acesse: https://portal.azure.com → **Microsoft Entra ID → App registrations → New registration**
2. Nome: `m365-migrator-source`
3. **API permissions → Add permission → Microsoft Graph → Application permissions:**
   - `Mail.Read`
   - `Calendars.Read`
   - `Contacts.Read`
   - `Files.Read.All`
   - `User.Read.All`
4. Clique em **Grant admin consent**
5. **Certificates & secrets → New client secret** → copie o valor
6. Copie o **Application (client) ID** e o **Directory (tenant) ID**

### 2. Criar App Registration no Tenant B (DESTINO)

1. Mesmo processo no tenant B, nome: `m365-migrator-target`
2. **API permissions → Application permissions:**
   - `Mail.ReadWrite`
   - `Calendars.ReadWrite`
   - `Contacts.ReadWrite`
   - `Files.ReadWrite.All`
   - `User.Read.All`
3. **Grant admin consent** e copie as credenciais

---

## ⚙️ Configuração

### config.json
```json
{
  "source_tenant": {
    "tenant_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "client_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "client_secret": "seu_secret_aqui",
    "domain": "empresa-antiga.com.br"
  },
  "target_tenant": {
    "tenant_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "client_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "client_secret": "seu_secret_aqui",
    "domain": "empresa-nova.com.br"
  },
  "migration": {
    "workloads": ["email", "calendar", "contacts", "onedrive"],
    "batch_size": 20,
    "concurrent_users": 3,
    "retry_attempts": 5,
    "retry_delay_ms": 2000,
    "throttle_delay_ms": 1000,
    "email_page_size": 100,
    "onedrive_page_size": 200,
    "resume_on_restart": true,
    "dry_run": false
  }
}
```

### users.csv
```csv
source_email,target_email,display_name
joao@antiga.com.br,joao@nova.com.br,João Silva
maria@antiga.com.br,maria@nova.com.br,Maria Souza
```

---

## 🚀 Como usar

### Com Docker (recomendado)
```bash
# Build
docker-compose build

# Rodar migração completa (todos os workloads)
docker-compose up migrator

# Só e-mails
docker-compose --profile email up migrate-email

# Só OneDrive
docker-compose --profile onedrive up migrate-onedrive

# Ver status/progresso
docker-compose --profile status up status
```

### Sem Docker (Node.js direto)
```bash
npm install

# Migração completa
npm run migrate:all

# Só e-mail
npm run migrate:email

# Só OneDrive
npm run migrate:onedrive

# Dry-run (simulação sem escrever nada)
node src/migrator.js --workload all --dry-run

# Só um usuário específico
node src/migrator.js --workload all --user joao@antiga.com.br

# Ver status
npm run status
```

---

## 🔄 Resume (continuação automática)

O progresso é salvo em `resume.json` após cada item migrado. Se a migração for interrompida (erro, queda de rede, etc), **basta rodar novamente** — ela continua do ponto onde parou, sem reprocessar o que já foi migrado.

Para reiniciar do zero:
```bash
node src/migrator.js --reset
```

---

## 📁 Estrutura de arquivos

```
m365-migrator/
├── config.json          ← Credenciais dos dois tenants
├── users.csv            ← Lista de usuários (origem → destino)
├── resume.json          ← Checkpoint automático (gerado em runtime)
├── Dockerfile
├── docker-compose.yml
├── package.json
├── logs/
│   ├── joao_antiga_com_br.log   ← Log por usuário
│   ├── maria_antiga_com_br.log
│   └── summary.json             ← Resumo geral
└── src/
    ├── migrator.js          ← Orquestrador principal
    ├── auth.js              ← OAuth2 via MSAL
    ├── graphClient.js       ← HTTP client com retry/throttle
    ├── emailMigrator.js     ← Migração de e-mails
    ├── onedriveMigrator.js  ← Migração de OneDrive
    ├── calendarMigrator.js  ← Calendário + contatos
    ├── checkpoint.js        ← Gerenciamento de progresso
    ├── userLoader.js        ← Leitor do CSV
    ├── logger.js            ← Sistema de logs
    └── status.js            ← Relatório de status
```

---

## ⚠️ Limitações conhecidas

| Limitação | Detalhe |
|-----------|---------|
| **Teams/Chat** | Histórico de conversas do Teams não é acessível via Graph API pública |
| **Versões de arquivos** | Apenas a versão atual do arquivo é migrada (versões anteriores não) |
| **Throttling** | Graph API limita requisições — o tool respeita o `Retry-After` automaticamente |
| **Caixas grandes** | Caixas acima de 50GB podem levar dias — execute em horário de menor uso |
| **Permissões de sharepoint** | Arquivos compartilhados no SharePoint precisam ser migrados separadamente |

---

## 📊 Estimativa de tempo

| Volume | E-mail | OneDrive |
|--------|--------|----------|
| 50 usuários / 5GB cada | ~4–8 horas | ~6–12 horas |
| 100 usuários / 10GB cada | ~12–24 horas | ~1–3 dias |

Execute de preferência **fora do horário comercial** para evitar throttling.

---

## 🔐 Segurança

- As credenciais ficam apenas no `config.json` local — nunca suba este arquivo para git
- Adicione `config.json` ao `.gitignore`
- O container usa as credenciais apenas via variável em memória, sem persistência
