═══════════════════════════════════════════════════════════════
 📁 ONEDRIVE MIGRATOR - INSTALAÇÃO
═══════════════════════════════════════════════════════════════

🎯 OBJETIVO:

Adicionar migração de OneDrive SEM modificar nada do Email que está
funcionando perfeitamente.

═══════════════════════════════════════════════════════════════

📦 ARQUIVO PARA SUBSTITUIR:

APENAS 1 arquivo:
✅ onedriveMigrator-COMPLETO.js → src/onedriveMigrator.js

NÃO TOQUE em:
❌ emailMigrator.js (deixe como está!)
❌ migrator.js (não precisa modificar)
❌ checkpoint.js (não precisa modificar)

═══════════════════════════════════════════════════════════════

🚀 INSTALAÇÃO (2 Passos):

PASSO 1: Substituir arquivo
────────────────────────────────────────────
Copy-Item onedriveMigrator-COMPLETO.js .\src\onedriveMigrator.js


PASSO 2: Rebuild
────────────────────────────────────────────
docker-compose down
docker-compose build --no-cache

═══════════════════════════════════════════════════════════════

✅ FUNCIONALIDADES DO ONEDRIVE:

1. ✅ DEDUPLICAÇÃO INTELIGENTE
   - Por path + name + size
   - Não duplica arquivos existentes
   - Cache de arquivos do destino

2. ✅ SYNC MODE
   - Detecta arquivos novos
   - Detecta arquivos apagados
   - Re-migra se necessário

3. ✅ ESTRUTURA DE PASTAS
   - Mantém hierarquia completa
   - Não duplica pastas
   - Cria apenas o necessário

4. ✅ CHECKPOINT GRANULAR
   - Salva progresso a cada 5 arquivos
   - Resume de onde parou
   - Compatível com CheckpointManager

5. ✅ UPLOAD OTIMIZADO
   - Small files (<4MB): Upload direto
   - Large files (>4MB): Chunks de 10MB
   - Timeout configurado
   - Retry automático

6. ✅ LOGS DETALHADOS
   - Progresso em tempo real
   - Speed (files/min)
   - ETA (tempo restante)
   - Tamanho de dados migrados

═══════════════════════════════════════════════════════════════

📊 USO:

MIGRAR APENAS EMAIL (como antes):
────────────────────────────────────────────
docker-compose run --rm migrator node src/migrator.js --workload=email


MIGRAR APENAS ONEDRIVE:
────────────────────────────────────────────
docker-compose run --rm migrator node src/migrator.js --workload=onedrive


MIGRAR TUDO (Email + OneDrive):
────────────────────────────────────────────
docker-compose up migrator
# OU
docker-compose run --rm migrator node src/migrator.js --workload=all


SYNC MODE ONEDRIVE (apenas arquivos novos):
────────────────────────────────────────────
docker-compose run --rm migrator node src/migrator.js --workload=onedrive --sync

═══════════════════════════════════════════════════════════════

📈 EXEMPLO DE LOGS:

Starting OneDrive migration: user@origem.com → user@destino.com
📊 Building file index for deduplication...
✅ Found 1,234 existing files in target - will skip duplicates
📊 Scanning OneDrive...
📊 Scan complete: 5,678 files | 234 folders | 12.4 GB
⏱️  Estimated time: ~1h 54min (at ~50 files/min)

📂 /Documents (45 items) | Global: 23%
   ✓ Report_2024.pdf (2.3 MB) | Speed: 52 files/min | ETA: 89min
   ✓ Proposal.docx (854.2 KB) | Speed: 53 files/min | ETA: 87min

📂 /Pictures (128 items) | Global: 67%
   ✓ photo_001.jpg (4.1 MB) | Speed: 51 files/min | ETA: 34min

📊 OneDrive Migration Summary:
   Files: 3,456 migrated, 2,222 skipped, 0 failed
   Data: 8.7 GB
   Folders: 180 created
   Speed: 52 files/min
   Time: 67 minutes

═══════════════════════════════════════════════════════════════

🔧 CONFIGURAÇÃO (config.json):

Já está configurado! Mas se quiser ajustar:

"migration": {
  "workloads": ["email"],  ← Adicione "onedrive" aqui se quiser
  "onedrive_page_size": 200,  ← Tamanho de página (opcional)
  ...
}

═══════════════════════════════════════════════════════════════

⚠️  IMPORTANTE:

1. ✅ NÃO modifica Email (totalmente independente)
2. ✅ Usa mesmo CheckpointManager
3. ✅ Usa mesmo sistema de autenticação
4. ✅ Logs no mesmo formato
5. ✅ Sync mode funciona igual

═══════════════════════════════════════════════════════════════

💡 DICAS:

1. TESTE PRIMEIRO:
   Migre um usuário com poucos arquivos primeiro

2. SYNC PERIÓDICO:
   Configure para rodar diariamente com --sync

3. APENAS NOVOS:
   Use --workload=onedrive --sync para copiar só novos

4. INTERROMPER:
   Ctrl+C a qualquer momento
   Resume.json salva progresso

═══════════════════════════════════════════════════════════════

🚀 PRONTO!

Agora você tem:
✅ Email funcionando perfeitamente
✅ OneDrive com deduplicação
✅ Sync mode em ambos
✅ Logs detalhados
✅ Checkpoint compartilhado

Simples e poderoso! 🎯

═══════════════════════════════════════════════════════════════