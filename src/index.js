import {
  makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  downloadMediaMessage
} from '@whiskeysockets/baileys';
import qrcodeTerminal from 'qrcode-terminal';
import QRCode from 'qrcode';
import pino from 'pino';
import fs from 'fs';
import path from 'path';
import cron from 'node-cron';
import http from 'http';

import { QueueManager } from './queueManager.js';
import { joinGroup } from './groupManager.js';
import { generateReport } from './reportManager.js';
import { extractInviteCodes, getRandomDelay, sleep, formatDate } from './utils.js';
import {
  initDb,
  addLinks,
  getPendingLinks,
  getPendingCount,
  updateLinkStatus,
  deleteAllLinks,
  getAllLinks,
  getStats
} from './database.js';

// Carregar arquivo de configuração
const CONFIG_PATH = path.resolve(process.cwd(), 'config.json');
let config = {
  prefix: '!entrar',
  minDelaySeconds: 2,
  maxDelaySeconds: 5,
  batchLimit: 0, // 0 = Processamento contínuo sem limites artificiais
  rescheduleHours: 2,
  adminJids: [],
  allowAllAdmins: true
};

if (fs.existsSync(CONFIG_PATH)) {
  try {
    const rawConfig = fs.readFileSync(CONFIG_PATH, 'utf-8');
    config = { ...config, ...JSON.parse(rawConfig) };
  } catch (e) {
    console.error('Erro ao ler config.json, usando valores padrão:', e.message);
  }
}

const queueManager = new QueueManager();
let isProcessing = false;
let latestQR = null;
let isConnected = false;
let globalSock = null;

// Estado para Modo Real-Time DB (RDB) e Confirmação de Exclusão
let rdbModeEnabled = false;
let rdbTargetJid = null;
let deletePendingMap = new Map(); // targetJid -> timestamp limite

// ---------------------------------------------------------
// Servidor HTTP & API REST para Render
// ---------------------------------------------------------
const PORT = process.env.PORT || 3000;

const server = http.createServer(async (req, res) => {
  const urlObj = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  const pathname = urlObj.pathname;
  const method = req.method || 'GET';

  // Configurar cabeçalhos CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  if (method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  // Endpoint: Health Check
  if (pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', connected: isConnected, rdbMode: rdbModeEnabled }));
    return;
  }

  // Endpoint REST API: POST /api/links (Cadastrar novos links no Banco de Dados)
  if (pathname === '/api/links' && method === 'POST') {
    let bodyText = '';
    req.on('data', (chunk) => {
      bodyText += chunk;
    });

    req.on('end', async () => {
      try {
        let textToParse = bodyText;
        if (req.headers['content-type']?.includes('application/json')) {
          const json = JSON.parse(bodyText || '{}');
          if (Array.isArray(json.links)) {
            textToParse = json.links.join('\n');
          } else if (json.text) {
            textToParse = json.text;
          } else if (json.url) {
            textToParse = json.url;
          }
        }

        const extracted = extractInviteCodes(textToParse);
        const { addedCount, totalPending } = await addLinks(extracted);

        // Se o modo RDB estiver ativo e houver socket do bot, dispara o processamento em tempo real!
        if (rdbModeEnabled && addedCount > 0 && globalSock && rdbTargetJid) {
          console.log(`⚡ [RDB Real-Time] ${addedCount} novos links detectados via API. Processando em tempo real...`);
          setImmediate(() => {
            processDatabaseQueue(globalSock, rdbTargetJid);
          });
        }

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(
          JSON.stringify({
            success: true,
            extractedCount: extracted.length,
            addedCount: addedCount,
            totalPending: totalPending,
            rdbTriggered: rdbModeEnabled
          })
        );
      } catch (err) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, error: err.message }));
      }
    });
    return;
  }

  // Endpoint REST API: GET /api/links (Listar links cadastrados)
  if (pathname === '/api/links' && method === 'GET') {
    const statusFilter = urlObj.searchParams.get('status');
    const links = await getAllLinks(statusFilter);
    const stats = await getStats();

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: true, stats, count: links.length, data: links }));
    return;
  }

  // Endpoint REST API: DELETE /api/links (Limpar banco via API)
  if (pathname === '/api/links' && method === 'DELETE') {
    const count = await deleteAllLinks();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: true, deletedCount: count }));
    return;
  }

  // Página Principal Web: QR Code e Status Painel
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });

  if (isConnected) {
    const stats = await getStats();
    res.end(`
      <!DOCTYPE html>
      <html>
        <head>
          <title>Whabot Painel</title>
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <style>
            body { font-family: system-ui, sans-serif; background: #0f172a; color: #f8fafc; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; padding: 1rem; }
            .card { background: #1e293b; padding: 2rem; border-radius: 12px; box-shadow: 0 10px 25px rgba(0,0,0,0.5); max-width: 480px; width: 100%; text-align: center; }
            .badge { background: #22c55e; color: #fff; padding: 6px 14px; border-radius: 20px; font-weight: bold; display: inline-block; margin-bottom: 1rem; }
            .rdb-badge { background: ${rdbModeEnabled ? '#3b82f6' : '#64748b'}; color: #fff; padding: 4px 10px; border-radius: 12px; font-size: 0.85rem; margin-left: 6px; }
            .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-top: 1.5rem; text-align: left; }
            .box { background: #0f172a; padding: 12px; border-radius: 8px; font-size: 0.9rem; }
            .box span { display: block; font-size: 1.3rem; font-weight: bold; color: #38bdf8; }
          </style>
        </head>
        <body>
          <div class="card">
            <div class="badge">✅ WhatsApp Conectado</div>
            <span class="rdb-badge">${rdbModeEnabled ? '⚡ RDB Ativo' : '⏹️ RDB Inativo'}</span>
            <h2>Whabot Group Joiner</h2>
            <p>Bot operando via PostgreSQL e API REST (${config.minDelaySeconds}-${config.maxDelaySeconds}s delay).</p>
            
            <div class="grid">
              <div class="box">Pendentes no DB: <span>${stats.pending}</span></div>
              <div class="box">Sucessos: <span>${stats.success}</span></div>
              <div class="box">Falhas: <span>${stats.failed}</span></div>
              <div class="box">Total Geral: <span>${stats.total}</span></div>
            </div>
          </div>
        </body>
      </html>
    `);
    return;
  }

  if (latestQR) {
    try {
      const qrImageDataUrl = await QRCode.toDataURL(latestQR);
      res.end(`
        <!DOCTYPE html>
        <html>
          <head>
            <title>Whabot - Escanear QR Code</title>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <meta http-equiv="refresh" content="6">
            <style>
              body { font-family: system-ui, sans-serif; background: #0f172a; color: #f8fafc; display: flex; align-items: center; justify-content: center; height: 100vh; margin: 0; text-align: center; }
              .card { background: #1e293b; padding: 2rem; border-radius: 12px; box-shadow: 0 10px 25px rgba(0,0,0,0.5); max-width: 420px; }
              img { border-radius: 8px; margin: 1rem 0; width: 260px; height: 260px; }
            </style>
          </head>
          <body>
            <div class="card">
              <h2>📲 Escaneie o QR Code</h2>
              <p>Abra o WhatsApp > Aparelhos Conectados > Conectar um Aparelho</p>
              <img src="${qrImageDataUrl}" alt="QR Code WhatsApp" />
              <p style="font-size: 0.85rem; color: #94a3b8;">A página atualiza automaticamente a cada 6 segundos.</p>
            </div>
          </body>
        </html>
      `);
      return;
    } catch (err) {
      console.error('Erro ao gerar QR em DataURL:', err);
    }
  }

  res.end(`
    <!DOCTYPE html>
    <html>
      <head>
        <title>Whabot - Inicializando...</title>
        <meta http-equiv="refresh" content="3">
        <style>
          body { font-family: system-ui, sans-serif; background: #0f172a; color: #f8fafc; display: flex; align-items: center; justify-content: center; height: 100vh; margin: 0; text-align: center; }
        </style>
      </head>
      <body>
        <div>
          <h2>⏳ Inicializando Conexão...</h2>
          <p>Aguarde enquanto o QR Code é gerado.</p>
        </div>
      </body>
    </html>
  `);
});

server.listen(PORT, async () => {
  console.log(`🌐 Servidor Web & API REST rodando na porta ${PORT}`);
  await initDb();
});

async function startBot() {
  console.log('--------------------------------------------------');
  console.log('🚀 Inicializando Bot WhatsApp de Entrada Rápida em Grupos');
  console.log('--------------------------------------------------');

  const baseDataDir = process.env.DATA_DIR ? path.resolve(process.env.DATA_DIR) : process.cwd();
  const authPath = process.env.AUTH_DIR
    ? path.resolve(process.env.AUTH_DIR)
    : path.resolve(baseDataDir, 'auth_info');

  if (!fs.existsSync(authPath)) {
    fs.mkdirSync(authPath, { recursive: true });
  }

  const { state, saveCreds } = await useMultiFileAuthState(authPath);

  const sock = makeWASocket({
    logger: pino({ level: 'silent' }),
    printQRInTerminal: false,
    auth: state,
    browser: ['Whabot Group Joiner', 'Chrome', '1.0.0']
  });

  globalSock = sock;

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      latestQR = qr;
      isConnected = false;
      console.log('\n📲 ESCANEIE O QR CODE ABAIXO NO SEU WHATSAPP OU ACESSE O LINK NA WEB:\n');
      qrcodeTerminal.generate(qr, { small: true });
      console.log('\nAbra o WhatsApp > Aparelhos Conectados > Conectar um Aparelho.\n');
    }

    if (connection === 'close') {
      isConnected = false;
      const shouldReconnect =
        (lastDisconnect?.error)?.output?.statusCode !== DisconnectReason.loggedOut;
      console.log(
        '⚠️ Conexão fechada. Motivo:',
        lastDisconnect?.error?.message || 'Desconhecido',
        'Reconectando:',
        shouldReconnect
      );
      if (shouldReconnect) {
        setTimeout(startBot, 5000);
      } else {
        console.log('❌ Sessão encerrada. Exclua a pasta de autenticação e escaneie o QR Code novamente.');
      }
    } else if (connection === 'open') {
      isConnected = true;
      latestQR = null;
      console.log('✅ Conexão estabelecida com sucesso com o WhatsApp!');
      console.log(`🤖 Bot ativo com banco de dados PostgreSQL.`);

      // Cron Job para verificar pendentes no banco a cada 3 minutos
      cron.schedule('*/3 * * * *', () => {
        checkScheduledDbQueue(sock);
      });
    }
  });

  // Listener de Mensagens Recebidas
  sock.ev.on('messages.upsert', async (m) => {
    try {
      if (m.type !== 'notify') return;

      for (const msg of m.messages) {
        if (!msg.message || (msg.key.fromMe && !config.allowAllAdmins)) continue;

        const fromJid = msg.key.remoteJid;
        const senderJid = msg.key.participant || msg.key.remoteJid;

        if (config.adminJids.length > 0 && !config.adminJids.includes(senderJid.split('@')[0])) {
          continue;
        }

        const textContent =
          msg.message.conversation ||
          msg.message.extendedTextMessage?.text ||
          msg.message.documentMessage?.caption ||
          msg.message.imageMessage?.caption ||
          '';

        const trimmedText = textContent.trim();
        const lowerText = trimmedText.toLowerCase();

        // ---------------------------------------------------------
        // 0. COMANDO: !menu / !ajuda / !help
        // ---------------------------------------------------------
        if (lowerText === '!menu' || lowerText === '!ajuda' || lowerText === '!help') {
          const stats = await getStats();
          const menuText = `🤖 *WHABOT - MENU DE COMANDOS*

📥 *Entrada em Grupos:*
• \`!entrar <links>\` : Cadastra os links (ou anexo .txt) e entra em velocidade rápida (${config.minDelaySeconds}-${config.maxDelaySeconds}s).
• \`!entrar db\` : Processa todos os grupos pendentes no Banco de Dados.
• \`!entrar rdb\` : Alterna o modo **Tempo Real (RDB)**. Cada link inserido via API REST entra imediatamente!

📊 *Estatísticas no Banco de Dados:*
• \`!status\` / \`!stats\` : Exibe resumo dos grupos no PostgreSQL.
  - Pendentes: *${stats.pending}*
  - Sucessos: *${stats.success}*
  - Falhas: *${stats.failed}*
  - Total: *${stats.total}*

🗑️ *Gerenciamento de Dados:*
• \`!delete\` / \`!deletar db\` : Solicita a exclusão dos links do banco.
• \`!confirmar delete\` : Confirma a exclusão definitiva (válido por 60s).

🌐 *API REST (Render):*
• \`POST https://whabot-gemini.onrender.com/api/links\`
• \`GET https://whabot-gemini.onrender.com/api/links\`

_Modo RDB Atual:_ *${rdbModeEnabled ? '⚡ ATIVADO' : '⏹️ DESATIVADO'}*`;

          await sock.sendMessage(fromJid, { text: menuText });
          continue;
        }

        // ---------------------------------------------------------
        // 0.1 COMANDO: !status / !stats
        // ---------------------------------------------------------
        if (lowerText === '!status' || lowerText === '!stats') {
          const stats = await getStats();
          const statusText = `📊 *ESTATÍSTICAS DO BANCO DE DADOS*

• ⏳ Pendentes: *${stats.pending}*
• ✅ Sucessos: *${stats.success}*
• ❌ Falhas: *${stats.failed}*
• 🛑 Limite Temporário: *${stats.rate_limited}*
• 📁 Total Cadastrado: *${stats.total}*

⚡ Modo Real-Time DB (RDB): *${rdbModeEnabled ? 'ATIVADO' : 'DESATIVADO'}*`;

          await sock.sendMessage(fromJid, { text: statusText });
          continue;
        }

        // ---------------------------------------------------------
        // 1. COMANDO: !confirmar delete (Executa a limpeza no DB)
        // ---------------------------------------------------------
        if (lowerText === '!confirmar delete') {
          const limitTime = deletePendingMap.get(fromJid);
          if (limitTime && Date.now() <= limitTime) {
            deletePendingMap.delete(fromJid);
            const deletedCount = await deleteAllLinks();
            await sock.sendMessage(fromJid, {
              text: `🗑️ *Banco de Dados Limpo!*\n\nForam excluídos *${deletedCount}* registros do banco de dados PostgreSQL.`
            });
          } else {
            deletePendingMap.delete(fromJid);
            await sock.sendMessage(fromJid, {
              text: `⚠️ Nenhuma solicitação de exclusão pendente ou o tempo de 60 segundos expirou. Use \`!delete\` para solicitar novamente.`
            });
          }
          continue;
        }

        // ---------------------------------------------------------
        // 2. COMANDO: !delete ou !deletar db (Trava com Confirmação)
        // ---------------------------------------------------------
        if (lowerText === '!delete' || lowerText === '!deletar db') {
          deletePendingMap.set(fromJid, Date.now() + 60000); // Válido por 60s
          await sock.sendMessage(fromJid, {
            text: `⚠️ *ATENÇÃO: Confirmação de Exclusão*\n\nVocê solicitou apagar TODOS os links salvos no banco de dados.\n\nPara confirmar a exclusão definitiva, envie o comando abaixo em até *60 segundos*:\n👉 *\`!confirmar delete\`*`
          });
          continue;
        }

        // ---------------------------------------------------------
        // 3. COMANDO: !entrar rdb (Alternar Modo Real-Time DB)
        // ---------------------------------------------------------
        if (lowerText === '!entrar rdb') {
          rdbModeEnabled = !rdbModeEnabled;
          rdbTargetJid = fromJid;

          if (rdbModeEnabled) {
            const pendingCount = await getPendingCount();
            await sock.sendMessage(fromJid, {
              text: `⚡ *Modo Real-Time DB (RDB) ATIVADO!*\n\nCada novo link cadastrado no banco de dados via API REST (\`POST /api/links\`) será processado **imediatamente** em tempo real.\n\n• Links pendentes no banco atualmente: *${pendingCount}*\nOs relatórios serão enviados nesta conversa.`
            });

            // Se já houver pendentes no banco, inicia o processamento imediato
            if (pendingCount > 0) {
              await processDatabaseQueue(sock, fromJid);
            }
          } else {
            await sock.sendMessage(fromJid, {
              text: `⏹️ *Modo Real-Time DB (RDB) DESATIVADO.*\nO bot não processará novos links da API automaticamente.`
            });
          }
          continue;
        }

        // ---------------------------------------------------------
        // 4. COMANDO: !entrar db (Processar Links Pendentes do DB)
        // ---------------------------------------------------------
        if (lowerText === '!entrar db') {
          const pendingCount = await getPendingCount();
          if (pendingCount === 0) {
            await sock.sendMessage(fromJid, {
              text: `ℹ️ *Banco de Dados sem Links Pendentes*\n\nNão há grupos com status \`pending\` no banco de dados no momento.`
            });
            continue;
          }

          await sock.sendMessage(fromJid, {
            text: `📊 *Processamento de Banco de Dados*\n\n• Links pendentes no DB: *${pendingCount}*\nIniciando entradas em velocidade máxima (${config.minDelaySeconds}-${config.maxDelaySeconds}s delay)...`
          });

          await processDatabaseQueue(sock, fromJid);
          continue;
        }

        // ---------------------------------------------------------
        // 5. COMANDO: !entrar <links> (Modo padrão: cadastra no DB e processa)
        // ---------------------------------------------------------
        if (trimmedText.startsWith(config.prefix)) {
          console.log(`\n📩 Comando ${config.prefix} recebido de: ${senderJid}`);
          let linksToProcess = [];

          const linksFromText = extractInviteCodes(trimmedText);
          linksToProcess.push(...linksFromText);

          // Anexo .txt
          if (msg.message.documentMessage) {
            const doc = msg.message.documentMessage;
            const fileName = doc.fileName || '';
            const mimeType = doc.mimetype || '';

            if (mimeType.includes('text') || fileName.endsWith('.txt')) {
              try {
                console.log(`📎 Baixando arquivo anexado: ${fileName}...`);
                const buffer = await downloadMediaMessage(
                  msg,
                  'buffer',
                  {},
                  {
                    logger: pino({ level: 'silent' }),
                    reconnect: sock.type
                  }
                );
                const fileText = buffer.toString('utf-8');
                const linksFromFile = extractInviteCodes(fileText);
                linksToProcess.push(...linksFromFile);
              } catch (err) {
                console.error('Erro ao ler arquivo anexado:', err.message);
              }
            }
          }

          // Arquivo local .txt
          const args = trimmedText.slice(config.prefix.length).trim().split(/\s+/);
          if (args.length > 0 && args[0].endsWith('.txt')) {
            const localFilePath = path.resolve(process.cwd(), args[0]);
            if (fs.existsSync(localFilePath)) {
              try {
                const content = fs.readFileSync(localFilePath, 'utf-8');
                const linksFromLocal = extractInviteCodes(content);
                linksToProcess.push(...linksFromLocal);
              } catch (err) {
                console.error('Erro ao ler arquivo local:', err.message);
              }
            }
          }

          const uniqueLinksMap = new Map();
          for (const item of linksToProcess) {
            uniqueLinksMap.set(item.code, item);
          }
          const uniqueLinks = Array.from(uniqueLinksMap.values());

          if (uniqueLinks.length === 0) {
            await sock.sendMessage(fromJid, {
              text: `⚠️ Nenhum link de grupo válido do WhatsApp foi encontrado.\n\nDigite \`!menu\` para ver a lista de comandos disponíveis.`
            });
            continue;
          }

          // Adiciona os links no banco PostgreSQL
          const { addedCount, totalPending } = await addLinks(uniqueLinks);

          await sock.sendMessage(fromJid, {
            text: `📥 *Links Registrados no Banco de Dados*\n\n• Novos links inseridos: *${addedCount}*\n• Total pendente no DB: *${totalPending}*\n\nIniciando o processamento...`
          });

          await processDatabaseQueue(sock, fromJid);
        }
      }
    } catch (err) {
      console.error('Erro no processador de mensagens:', err);
    }
  });
}

/**
 * Processa os links com status 'pending' diretamente do banco de dados PostgreSQL
 * @param {import('@whiskeysockets/baileys').WASocket} sock 
 * @param {string} targetJid 
 */
async function processDatabaseQueue(sock, targetJid) {
  if (isProcessing) {
    console.log('⏳ O processamento da fila já está em execução.');
    return;
  }

  // Buscar todos os links pendentes no PostgreSQL
  const pendingLinks = await getPendingLinks();
  if (pendingLinks.length === 0) {
    console.log('✅ Nenhum grupo pendente no PostgreSQL.');
    return;
  }

  isProcessing = true;
  console.log(`\n⚡ Processando ${pendingLinks.length} links pendentes do Banco de Dados PostgreSQL...`);

  const batchResults = [];
  let limitReached = false;
  let scheduledNextRun = null;

  for (let i = 0; i < pendingLinks.length; i++) {
    const item = pendingLinks[i];

    if (i > 0) {
      const delayMs = getRandomDelay(config.minDelaySeconds, config.maxDelaySeconds);
      console.log(`⏱️ Delay (${(delayMs / 1000).toFixed(1)}s)...`);
      await sleep(delayMs);
    }

    console.log(`[${i + 1}/${pendingLinks.length}] Entrando no grupo (código: ${item.code})...`);

    const result = await joinGroup(sock, item.code);

    if (result.success) {
      console.log(`   ✅ Sucesso! Entrou no grupo: "${result.groupName}"`);
      await updateLinkStatus(item.code, 'success', result.groupName, result.reason || 'Entrada efetuada');
      batchResults.push({
        code: item.code,
        url: item.url,
        status: 'success',
        reason: result.reason,
        groupName: result.groupName
      });
    } else {
      console.log(`   ❌ Falha! Motivo: ${result.reason}`);

      if (result.isRateLimited) {
        console.log('⚠️ Detectado Rate Limit do WhatsApp. Mantendo link no DB e pausando...');
        await updateLinkStatus(item.code, 'rate_limited', result.groupName, result.reason);
        batchResults.push({
          code: item.code,
          url: item.url,
          status: 'failed',
          reason: result.reason,
          groupName: result.groupName
        });
        limitReached = true;
        scheduledNextRun = queueManager.scheduleNextBatch(config.rescheduleHours);
        break;
      } else {
        // Falha definitiva (ex: link inválido, revogado, grupo cheio)
        await updateLinkStatus(item.code, 'failed', result.groupName, result.reason);
        batchResults.push({
          code: item.code,
          url: item.url,
          status: 'failed',
          reason: result.reason,
          groupName: result.groupName
        });
      }
    }
  }

  const remainingPending = await getPendingCount();

  if (targetJid) {
    const reportText = generateReport({
      results: batchResults,
      totalPending: remainingPending,
      nextRunTime: scheduledNextRun,
      limitReached: limitReached
    });

    try {
      await sock.sendMessage(targetJid, { text: reportText });
      console.log('📊 Relatório do banco de dados enviado com sucesso.');
    } catch (err) {
      console.error('Erro ao enviar relatório:', err.message);
    }
  }

  isProcessing = false;
}

/**
 * Função executada pelo Cron para verificar se há links pendentes no DB
 */
async function checkScheduledDbQueue(sock) {
  if (isProcessing) return;
  const pendingCount = await getPendingCount();
  if (pendingCount > 0 && !queueManager.isScheduledWaitActive()) {
    console.log(`⏰ Cron detectou ${pendingCount} links pendentes no DB. Processando...`);
    if (rdbTargetJid) {
      await processDatabaseQueue(sock, rdbTargetJid);
    }
  }
}

// Iniciar o bot
startBot().catch((err) => {
  console.error('Erro fatal na inicialização do bot:', err);
});
