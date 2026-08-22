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

  // Endpoint REST API: POST /api/links (Cadastrar novos links no Banco de Dados + Fila Local)
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
        
        // Salva na fila local sempre como garantia
        const localRes = queueManager.addToQueue(extracted, rdbTargetJid || 'api@system');

        // Tenta salvar também no PostgreSQL se disponível
        let dbAddedCount = 0;
        let dbTotalPending = 0;
        try {
          const dbRes = await addLinks(extracted);
          dbAddedCount = dbRes.addedCount;
          dbTotalPending = dbRes.totalPending;
        } catch (dbErr) {
          console.warn('⚠️ Falha ao salvar no PostgreSQL (usando fila local):', dbErr.message);
        }

        const addedCount = dbAddedCount || localRes.addedCount;
        const totalPending = dbTotalPending || localRes.totalPending;

        // Se o modo RDB estiver ativo e NÃO estiver em pausa por rate limit, dispara o processamento
        if (rdbModeEnabled && addedCount > 0 && globalSock && rdbTargetJid && !queueManager.isScheduledWaitActive()) {
          console.log(`⚡ [RDB Real-Time] ${addedCount} novos links detectados via API. Processando em tempo real...`);
          setImmediate(() => {
            processHybridQueue(globalSock, rdbTargetJid);
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

  // Endpoint REST API: GET /api/links (Suporta inserção via Query Param ?add=... para burlar CSP de navegadores)
  if (pathname === '/api/links' && method === 'GET') {
    const addParam = urlObj.searchParams.get('add') || urlObj.searchParams.get('text') || urlObj.searchParams.get('link');

    if (addParam) {
      const extracted = extractInviteCodes(addParam);
      
      const localRes = queueManager.addToQueue(extracted, rdbTargetJid || 'api@system');
      let dbAddedCount = 0;
      let dbTotalPending = 0;

      try {
        const dbRes = await addLinks(extracted);
        dbAddedCount = dbRes.addedCount;
        dbTotalPending = dbRes.totalPending;
      } catch (dbErr) {
        console.warn('⚠️ Falha no DB GET Ping (usando fila local):', dbErr.message);
      }

      const addedCount = dbAddedCount || localRes.addedCount;
      const totalPending = dbTotalPending || localRes.totalPending;

      if (rdbModeEnabled && addedCount > 0 && globalSock && rdbTargetJid && !queueManager.isScheduledWaitActive()) {
        console.log(`⚡ [RDB Real-Time] ${addedCount} novos links recebidos via Image Ping. Processando...`);
        setImmediate(() => {
          processHybridQueue(globalSock, rdbTargetJid);
        });
      }

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: true, addedCount, totalPending }));
      return;
    }

    const statusFilter = urlObj.searchParams.get('status');
    let links = [];
    let stats = { total: 0, pending: 0, success: 0, failed: 0, rate_limited: 0 };

    try {
      links = await getAllLinks(statusFilter);
      stats = await getStats();
    } catch (err) {
      links = queueManager.getPendingItems();
      stats = { total: links.length, pending: links.length, success: 0, failed: 0, rate_limited: 0 };
    }

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: true, stats, count: links.length, data: links }));
    return;
  }

  // Endpoint REST API: DELETE /api/links (Limpar banco e fila via API)
  if (pathname === '/api/links' && method === 'DELETE') {
    let count = 0;
    try {
      count = await deleteAllLinks();
    } catch (e) {}
    queueManager.data.pending = [];
    queueManager.saveQueue();

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: true, deletedCount: count }));
    return;
  }

  // Página Principal Web: QR Code e Status Painel
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });

  if (isConnected) {
    let stats = { total: 0, pending: 0, success: 0, failed: 0, rate_limited: 0 };
    try {
      stats = await getStats();
    } catch (e) {
      const pending = queueManager.getPendingItems().length;
      stats = { total: pending, pending: pending, success: 0, failed: 0, rate_limited: 0 };
    }

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
            <p>Bot operando via Híbrido (PostgreSQL + Fallback Fila Local) (${config.minDelaySeconds}-${config.maxDelaySeconds}s delay).</p>
            
            <div class="grid">
              <div class="box">Pendentes: <span>${stats.pending}</span></div>
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
  try {
    await initDb();
  } catch (err) {
    console.warn('⚠️ PostgreSQL offline na inicialização, usando fila local de contingência.');
  }
});

async function startBot() {
  console.log('--------------------------------------------------');
  console.log('🚀 Inicializando Bot WhatsApp de Entrada Rápida em Grupos (Modo Híbrido)');
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
      console.log(`🤖 Bot ativo (Suporte duplo PostgreSQL + Fila Local).`);

      // Cron Job para verificar pendentes a cada 3 minutos
      cron.schedule('*/3 * * * *', () => {
        checkScheduledHybridQueue(sock);
      });
    }
  });

  // Listener de Mensagens Recebidas
  sock.ev.on('messages.upsert', async (m) => {
    try {
      if (m.type !== 'notify') return;

      for (const msg of m.messages) {
        if (!msg.message) continue;

        // Ignora mensagens enviadas pelo próprio bot
        if (msg.key.fromMe) continue;

        const fromJid = msg.key.remoteJid;
        const senderJid = msg.key.participant || msg.key.remoteJid;

        const textContent =
          msg.message.conversation ||
          msg.message.extendedTextMessage?.text ||
          msg.message.documentMessage?.caption ||
          msg.message.imageMessage?.caption ||
          msg.message.videoMessage?.caption ||
          msg.message.buttonsResponseMessage?.selectedDisplayText ||
          msg.message.listResponseMessage?.title ||
          '';

        const trimmedText = textContent.trim();
        const lowerText = trimmedText.toLowerCase();

        // =========================================================
        // 🔥 AUTO-CAPTURA: Detecta links de grupo em TODA mensagem
        // (grupos, PV, qualquer conversa) — entra imediatamente!
        // =========================================================
        const autoLinks = extractInviteCodes(trimmedText);
        if (autoLinks.length > 0) {
          console.log(`\n🔗 [AUTO-CAPTURA] ${autoLinks.length} link(s) detectado(s) em mensagem de ${senderJid}`);

          for (const item of autoLinks) {
            // Evita entrar no mesmo grupo duas vezes (verifica DB + fila)
            let jaExiste = false;
            try {
              const { addedCount } = await addLinks([item]);
              if (addedCount === 0) {
                jaExiste = true; // Já estava no banco (UNIQUE constraint)
              }
            } catch (e) {
              const localRes = queueManager.addToQueue([item], fromJid);
              if (localRes.addedCount === 0) jaExiste = true;
            }

            if (jaExiste) {
              console.log(`   ⏭️ Grupo já registrado, ignorando: ${item.url}`);
              continue;
            }

            console.log(`   ⚡ Entrando automaticamente: ${item.url}`);
            const result = await joinGroup(sock, item.code);

            if (result.success) {
              console.log(`   ✅ Entrou: "${result.groupName}"`);
              try { await updateLinkStatus(item.code, 'success', result.groupName, 'Auto-captura'); } catch(e) {}
              queueManager.markProcessed(item.code, 'success', 'Auto-captura', result.groupName);

            } else if (result.isRateLimited) {
              console.log(`   ⏳ Rate limit ao entrar: ${item.url}. Colocado na fila.`);
              try { await updateLinkStatus(item.code, 'rate_limited', '', result.reason); } catch(e) {}

            } else {
              console.log(`   ❌ Falha ao entrar: ${result.reason}`);
              try { await updateLinkStatus(item.code, 'failed', '', result.reason); } catch(e) {}
              queueManager.markProcessed(item.code, 'failed', result.reason, '');
            }

            // Pequeno delay entre links se vieram vários juntos
            if (autoLinks.length > 1) {
              await sleep(getRandomDelay(config.minDelaySeconds, config.maxDelaySeconds));
            }
          }

          // Após auto-captura, continua para verificar se também é um comando
        }

        // Filtro de administradores para comandos
        if (config.adminJids.length > 0 && !config.adminJids.includes(senderJid.split('@')[0])) {
          continue;
        }

        // ---------------------------------------------------------
        // 0. COMANDO: !menu / !ajuda / !help
        // ---------------------------------------------------------
        if (lowerText === '!menu' || lowerText === '!ajuda' || lowerText === '!help') {
          let stats = { total: 0, pending: 0, success: 0, failed: 0, rate_limited: 0 };
          try {
            stats = await getStats();
          } catch (e) {
            const pending = queueManager.getPendingItems().length;
            stats = { total: pending, pending: pending, success: 0, failed: 0, rate_limited: 0 };
          }

          const menuText = `🤖 *WHABOT - MENU DE COMANDOS*

📥 *Entrada em Grupos:*
• \`!entrar <links>\` : Cadastra os links (ou anexo .txt) e entra em velocidade rápida (${config.minDelaySeconds}-${config.maxDelaySeconds}s).
• \`!entrar db\` : Processa todos os grupos pendentes no Banco/Fila.
• \`!entrar rdb\` : Alterna o modo **Tempo Real (RDB)**. Cada link inserido via API REST entra imediatamente!

📢 *Disparo & Menção Invisível:*
• \`!divulgar <mensagem>\` : Dispara a mensagem para todos os grupos com **chat aberto**, com **menção invisível** (notificação para todos os membros!).
• \`!tagall <mensagem>\` ou \`!marcar <mensagem>\` : Envia mensagem no grupo marcando TODOS os membros com **menção invisível**.

📊 *Estatísticas no Banco de Dados:*
• \`!status\` / \`!stats\` : Exibe resumo dos grupos.
  - Pendentes: *${stats.pending}*
  - Sucessos: *${stats.success}*
  - Falhas: *${stats.failed}*
  - Total: *${stats.total}*

🗑️ *Gerenciamento de Dados:*
• \`!delete\` / \`!deletar db\` : Solicita a exclusão dos links.
• \`!confirmar delete\` : Confirma a exclusão definitiva (válido por 60s).

🌐 *API REST (Render):*
• \`POST https://whabot-gemini-48ty.onrender.com/api/links\`
• \`GET https://whabot-gemini-48ty.onrender.com/api/links\`

_Modo RDB Atual:_ *${rdbModeEnabled ? '⚡ ATIVADO' : '⏹️ DESATIVADO'}*`;

          await sock.sendMessage(fromJid, { text: menuText });
          continue;
        }

        // ---------------------------------------------------------
        // 0.1 COMANDO: !status / !stats
        // ---------------------------------------------------------
        if (lowerText === '!status' || lowerText === '!stats') {
          let stats = { total: 0, pending: 0, success: 0, failed: 0, rate_limited: 0 };
          try {
            stats = await getStats();
          } catch (e) {
            const pending = queueManager.getPendingItems().length;
            stats = { total: pending, pending: pending, success: 0, failed: 0, rate_limited: 0 };
          }

          const statusText = `📊 *ESTATÍSTICAS DO SISTEMA*

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
        // 1. COMANDO: !confirmar delete (Executa a limpeza)
        // ---------------------------------------------------------
        if (lowerText === '!confirmar delete') {
          const limitTime = deletePendingMap.get(fromJid);
          if (limitTime && Date.now() <= limitTime) {
            deletePendingMap.delete(fromJid);
            let deletedCount = 0;
            try {
              deletedCount = await deleteAllLinks();
            } catch (e) {}
            queueManager.data.pending = [];
            queueManager.saveQueue();

            await sock.sendMessage(fromJid, {
              text: `🗑️ *Banco de Dados e Fila Limpos!*\n\nForam excluídos os registros do banco de dados e da fila local.`
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
          deletePendingMap.set(fromJid, Date.now() + 60000);
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
            let pendingCount = 0;
            try {
              pendingCount = await getPendingCount();
            } catch (e) {
              pendingCount = queueManager.getPendingItems().length;
            }

            await sock.sendMessage(fromJid, {
              text: `⚡ *Modo Real-Time DB (RDB) ATIVADO!*\n\nCada novo link cadastrado no banco de dados ou via API REST será processado **imediatamente** em tempo real.\n\n• Links pendentes atualmente: *${pendingCount}*\nOs relatórios serão enviados nesta conversa.`
            });

            if (pendingCount > 0) {
              await processHybridQueue(sock, fromJid);
            }
          } else {
            await sock.sendMessage(fromJid, {
              text: `⏹️ *Modo Real-Time DB (RDB) DESATIVADO.*\nO bot não processará novos links da API automaticamente.`
            });
          }
          continue;
        }

        // ---------------------------------------------------------
        // 3.1 COMANDO: !tagall / !mencionar / !marcar <mensagem> (Menção Invisível no Grupo)
        // ---------------------------------------------------------
        if (lowerText.startsWith('!tagall') || lowerText.startsWith('!mencionar') || lowerText.startsWith('!marcar')) {
          const msgTexto = trimmedText.replace(/^!(tagall|mencionar|marcar)\s*/i, '').trim();
          if (!msgTexto) {
            await sock.sendMessage(fromJid, {
              text: `⚠️ *Uso do Comando:*\n\n\`!tagall Minha mensagem aqui\`\nEnvia uma mensagem no grupo marcando todos os membros com **menção invisível** (notificação para todos os membros!).`
            });
            continue;
          }

          if (!fromJid.endsWith('@g.us')) {
            await sock.sendMessage(fromJid, {
              text: `⚠️ Este comando deve ser enviado dentro de um grupo do WhatsApp.`
            });
            continue;
          }

          try {
            const groupMetadata = await sock.groupMetadata(fromJid);
            const participants = groupMetadata.participants.map(p => p.id);
            await sock.sendMessage(fromJid, {
              text: msgTexto,
              mentions: participants
            });
            console.log(`📢 [TagAll] Mensagem enviada com menção invisível para ${participants.length} membros no grupo.`);
          } catch (err) {
            await sock.sendMessage(fromJid, {
              text: `❌ Falha ao obter membros do grupo: ${err.message}`
            });
          }
          continue;
        }

        // ---------------------------------------------------------
        // 3.2 COMANDO: !divulgar / !broadcast <mensagem> (Disparo em Grupos Abertos com Menção Invisível)
        // ---------------------------------------------------------
        if (lowerText.startsWith('!divulgar') || lowerText.startsWith('!broadcast')) {
          const msgTexto = trimmedText.replace(/^!(divulgar|broadcast)\s*/i, '').trim();
          if (!msgTexto) {
            await sock.sendMessage(fromJid, {
              text: `⚠️ *Uso do Comando:*\n\n\`!divulgar Minha mensagem de divulgação\`\nDispara a mensagem para **todos os grupos com chat aberto**, usando **menção invisível** em todos os membros!`
            });
            continue;
          }

          await sock.sendMessage(fromJid, {
            text: `📢 *Iniciando Disparo de Divulgação...*\n\nBuscando grupos com chat aberto...`
          });

          try {
            const groupsDict = await sock.groupFetchAllParticipating();
            const allGroups = Object.values(groupsDict);
            // Filtra grupos onde o chat é aberto para membros (!announce)
            const openGroups = allGroups.filter(g => !g.announce);

            if (openGroups.length === 0) {
              await sock.sendMessage(fromJid, {
                text: `⚠️ Nenhum grupo com chat aberto encontrado (${allGroups.length} grupos verificados).`
              });
              continue;
            }

            await sock.sendMessage(fromJid, {
              text: `🚀 *Disparando mensagem para ${openGroups.length} grupos abertos...*\n(Com menção invisível para notificar todos os membros. Aguarde um intervalo seguro de 3s entre envios).`
            });

            let sucessos = 0;
            let falhas = 0;

            for (const group of openGroups) {
              try {
                const participants = group.participants ? group.participants.map(p => p.id) : [];
                await sock.sendMessage(group.id, {
                  text: msgTexto,
                  mentions: participants
                });
                sucessos++;
              } catch (e) {
                falhas++;
              }
              // Delay seguro de 3 segundos entre envios de grupos
              await new Promise(r => setTimeout(r, 3000));
            }

            await sock.sendMessage(fromJid, {
              text: `✅ *Divulgação Concluída com Sucesso!*\n\n• Enviados com sucesso: *${sucessos}*\n• Falhas/Bloqueados: *${falhas}*\n• Total de Grupos Abertos: *${openGroups.length}*`
            });
          } catch (err) {
            await sock.sendMessage(fromJid, {
              text: `❌ Erro ao buscar lista de grupos: ${err.message}`
            });
          }
          continue;
        }

        // ---------------------------------------------------------
        // 4. COMANDO: !entrar db (Processar Links Pendentes)
        // ---------------------------------------------------------
        if (lowerText === '!entrar db') {
          let pendingCount = 0;
          try {
            pendingCount = await getPendingCount();
          } catch (e) {
            pendingCount = queueManager.getPendingItems().length;
          }

          if (pendingCount === 0 && queueManager.getPendingItems().length === 0) {
            await sock.sendMessage(fromJid, {
              text: `ℹ️ *Sem Links Pendentes*\n\nNão há grupos pendentes no banco de dados ou na fila local no momento.`
            });
            continue;
          }

          const totalShow = Math.max(pendingCount, queueManager.getPendingItems().length);

          await sock.sendMessage(fromJid, {
            text: `📊 *Processamento de Grupos*\n\n• Links pendentes: *${totalShow}*\nIniciando entradas em velocidade máxima (${config.minDelaySeconds}-${config.maxDelaySeconds}s delay)...`
          });

          await processHybridQueue(sock, fromJid);
          continue;
        }

        // ---------------------------------------------------------
        // 5. COMANDO: !entrar <links> (Modo padrão: cadastra e processa)
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

          // Salva na fila local sempre como contingência
          const localRes = queueManager.addToQueue(uniqueLinks, fromJid);

          // Tenta salvar também no PostgreSQL
          let dbAdded = 0;
          let dbPending = 0;
          try {
            const dbRes = await addLinks(uniqueLinks);
            dbAdded = dbRes.addedCount;
            dbPending = dbRes.totalPending;
          } catch (dbErr) {
            console.warn('⚠️ Falha ao salvar no PostgreSQL (usando fila local):', dbErr.message);
          }

          const addedCount = dbAdded || localRes.addedCount;
          const totalPending = dbPending || localRes.totalPending;

          await sock.sendMessage(fromJid, {
            text: `📥 *Links Registrados*\n\n• Novos links inseridos: *${addedCount}*\n• Total pendente: *${totalPending}*\n\nIniciando o processamento...`
          });

          await processHybridQueue(sock, fromJid);
        }
      }
    } catch (err) {
      console.error('Erro no processador de mensagens:', err);
    }
  });
}

/**
 * Processa os links pendentes usando modo Híbrido: tenta PostgreSQL primeiro,
 * e se falhar/estiver offline, usa a fila local queue.json sem interromper a execução!
 * @param {import('@whiskeysockets/baileys').WASocket} sock 
 * @param {string} targetJid 
 */
async function processHybridQueue(sock, targetJid) {
  if (isProcessing) {
    console.log('⏳ O processamento da fila já está em execução.');
    return;
  }

  // Se o rate limit estiver ativo, não tenta entrar e não envia relatórios parciais
  if (queueManager.isScheduledWaitActive()) {
    console.log('⏳ Pausa por Rate Limit ativa. Agendamento em vigor.');
    if (targetJid) {
      const nextRunStr = formatDate(queueManager.getNextScheduledRun());
      try {
        await sock.sendMessage(targetJid, {
          text: `⏳ *Limite do WhatsApp Ativo*\n\nO bot atingiu o limite de entradas temporário do WhatsApp. Os links estão guardados com segurança na fila.\n\n• Próxima tentativa agendada para: *${nextRunStr}*`
        });
      } catch (e) {}
    }
    return;
  }

  let pendingLinks = [];
  let isUsingLocalFallback = false;

  // 1. Tentar buscar do PostgreSQL
  try {
    pendingLinks = await getPendingLinks();
  } catch (err) {
    console.warn('⚠️ Falha ao consultar PostgreSQL. Alternando para fila local de contingência:', err.message);
    isUsingLocalFallback = true;
  }

  // 2. Se o DB falhar ou retornar vazio mas houver itens na fila local
  if (pendingLinks.length === 0) {
    const localItems = queueManager.getPendingItems();
    if (localItems.length > 0) {
      pendingLinks = localItems;
      isUsingLocalFallback = true;
    }
  }

  if (pendingLinks.length === 0) {
    console.log('✅ Nenhum grupo pendente no Banco ou na Fila Local.');
    return;
  }

  isProcessing = true;
  console.log(`\n⚡ Processando ${pendingLinks.length} links pendentes (${isUsingLocalFallback ? 'Fila Local' : 'PostgreSQL'})...`);

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

      // Atualiza DB se possível
      try {
        await updateLinkStatus(item.code, 'success', result.groupName, result.reason || 'Entrada efetuada');
      } catch (e) {}

      // Atualiza fila local
      queueManager.markProcessed(item.code, 'success', result.reason, result.groupName);

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
        console.log('⚠️ Detectado Rate Limit do WhatsApp. Mantendo link e pausando...');
        try {
          await updateLinkStatus(item.code, 'rate_limited', result.groupName, result.reason);
        } catch (e) {}

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
        try {
          await updateLinkStatus(item.code, 'failed', result.groupName, result.reason);
        } catch (e) {}
        queueManager.markProcessed(item.code, 'failed', result.reason, result.groupName);

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

  let remainingPending = 0;
  try {
    remainingPending = await getPendingCount();
  } catch (e) {
    remainingPending = queueManager.getPendingItems().length;
  }

  if (targetJid) {
    const reportText = generateReport({
      results: batchResults,
      totalPending: remainingPending,
      nextRunTime: scheduledNextRun,
      limitReached: limitReached
    });

    try {
      await sock.sendMessage(targetJid, { text: reportText });
      console.log('📊 Relatório enviado com sucesso.');
    } catch (err) {
      console.error('Erro ao enviar relatório:', err.message);
    }
  }

  isProcessing = false;
}

/**
 * Função executada pelo Cron para verificar se há links pendentes
 */
async function checkScheduledHybridQueue(sock) {
  if (isProcessing || !rdbModeEnabled) return;
  let pendingCount = 0;
  try {
    pendingCount = await getPendingCount();
  } catch (e) {
    pendingCount = queueManager.getPendingItems().length;
  }

  if (pendingCount > 0 && !queueManager.isScheduledWaitActive()) {
    console.log(`⏰ Cron detectou ${pendingCount} links pendentes. Processando...`);
    if (rdbTargetJid) {
      await processHybridQueue(sock, rdbTargetJid);
    }
  }
}

// Iniciar o bot
startBot().catch((err) => {
  console.error('Erro fatal na inicialização do bot:', err);
});
