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

// Carregar arquivo de configuração
const CONFIG_PATH = path.resolve(process.cwd(), 'config.json');
let config = {
  prefix: '!entrar',
  minDelaySeconds: 2,
  maxDelaySeconds: 5,
  batchLimit: 0, // 0 = Sem limite artificial do bot (processa tudo continuo até o WhatsApp barrar)
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

// ---------------------------------------------------------
// Servidor HTTP para Render (Health Check & QR Code na Web)
// ---------------------------------------------------------
const PORT = process.env.PORT || 3000;

const server = http.createServer(async (req, res) => {
  const url = req.url || '/';

  if (url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', connected: isConnected }));
    return;
  }

  // Página principal que exibe o QR Code ou status de conectado
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  
  if (isConnected) {
    res.end(`
      <!DOCTYPE html>
      <html>
        <head>
          <title>Whabot Status</title>
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <style>
            body { font-family: system-ui, sans-serif; background: #0f172a; color: #f8fafc; display: flex; align-items: center; justify-content: center; height: 100vh; margin: 0; text-align: center; }
            .card { background: #1e293b; padding: 2rem; border-radius: 12px; box-shadow: 0 10px 25px rgba(0,0,0,0.5); max-width: 400px; }
            .badge { background: #22c55e; color: #fff; padding: 6px 12px; border-radius: 20px; font-weight: bold; display: inline-block; margin-bottom: 1rem; }
          </style>
        </head>
        <body>
          <div class="card">
            <div class="badge">✅ WhatsApp Conectado</div>
            <h2>Whabot Group Joiner</h2>
            <p>O bot está ativo e processando grupos sem limites artificiais (${config.minDelaySeconds}-${config.maxDelaySeconds}s delay).</p>
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

server.listen(PORT, () => {
  console.log(`🌐 Servidor Web rodando na porta ${PORT} (Painel e Health Check ativos)`);
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
      console.log(`🤖 Bot em modo alta velocidade (${config.minDelaySeconds}-${config.maxDelaySeconds}s). Sem limite artificial de lote.`);

      // Iniciar Cron Job para verificar fila a cada 3 minutos
      cron.schedule('*/3 * * * *', () => {
        checkAndProcessScheduledQueue(sock);
      });
    }
  });

  // Listener de Mensagens Recebidas
  sock.ev.on('messages.upsert', async (m) => {
    try {
      if (m.type !== 'notify') return;

      for (const msg of m.messages) {
        // Ignorar mensagens de sistema ou de status
        if (!msg.message || (msg.key.fromMe && !config.allowAllAdmins)) continue;

        const fromJid = msg.key.remoteJid;
        const senderJid = msg.key.participant || msg.key.remoteJid;

        // Se houver restrição de JIDs de admin no config
        if (config.adminJids.length > 0 && !config.adminJids.includes(senderJid.split('@')[0])) {
          continue;
        }

        // Extrair texto da mensagem (suporta texto direto ou legenda de mídia)
        const textContent =
          msg.message.conversation ||
          msg.message.extendedTextMessage?.text ||
          msg.message.documentMessage?.caption ||
          msg.message.imageMessage?.caption ||
          '';

        const trimmedText = textContent.trim();

        // Verificar se a mensagem inicia com o comando !entrar
        if (trimmedText.startsWith(config.prefix)) {
          console.log(`\n📩 Comando ${config.prefix} recebido de: ${senderJid}`);
          let linksToProcess = [];

          // 1. Extrair links do texto da mensagem
          const linksFromText = extractInviteCodes(trimmedText);
          linksToProcess.push(...linksFromText);

          // 2. Verificar se há um arquivo .txt anexado à mensagem
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
                console.log(`📄 Links extraídos do arquivo ${fileName}: ${linksFromFile.length}`);
                linksToProcess.push(...linksFromFile);
              } catch (err) {
                console.error('Erro ao baixar/ler arquivo anexado:', err.message);
                await sock.sendMessage(fromJid, {
                  text: `⚠️ Erro ao ler o arquivo anexado: ${err.message}`
                });
              }
            }
          }

          // 3. Verificar se indicou o nome de um arquivo local (ex: !entrar links.txt)
          const args = trimmedText.slice(config.prefix.length).trim().split(/\s+/);
          if (args.length > 0 && args[0].endsWith('.txt')) {
            const localFilePath = path.resolve(process.cwd(), args[0]);
            if (fs.existsSync(localFilePath)) {
              try {
                console.log(`📄 Lendo arquivo local: ${args[0]}...`);
                const content = fs.readFileSync(localFilePath, 'utf-8');
                const linksFromLocal = extractInviteCodes(content);
                console.log(`📄 Links extraídos de ${args[0]}: ${linksFromLocal.length}`);
                linksToProcess.push(...linksFromLocal);
              } catch (err) {
                console.error('Erro ao ler arquivo local:', err.message);
              }
            }
          }

          // Remover duplicados da lista capturada nesta requisição
          const uniqueLinksMap = new Map();
          for (const item of linksToProcess) {
            uniqueLinksMap.set(item.code, item);
          }
          const uniqueLinks = Array.from(uniqueLinksMap.values());

          if (uniqueLinks.length === 0) {
            await sock.sendMessage(fromJid, {
              text: `⚠️ Nenhum link de grupo válido do WhatsApp foi encontrado na mensagem ou arquivo enviado.\n\n*Exemplo de Uso:*\n\`${config.prefix} https://chat.whatsapp.com/ExemploCodigo...\` ou envie um arquivo .txt com os links.`
            });
            continue;
          }

          // Adicionar à fila persistente
          const { addedCount, totalPending } = queueManager.addToQueue(uniqueLinks, fromJid);

          await sock.sendMessage(fromJid, {
            text: `⚡ *Convites Registrados para Entrada Rápida*\n\n• Adicionados nesta chamada: *${addedCount}*\n• Novos links únicos: *${uniqueLinks.length}*\n• Total pendente na fila: *${totalPending}*\n\nIniciando entradas em modo de alta velocidade (${config.minDelaySeconds}-${config.maxDelaySeconds}s por grupo) sem limite artificial...`
          });

          // Iniciar o processamento contínuo
          await processQueueBatch(sock, fromJid);
        }
      }
    } catch (err) {
      console.error('Erro no processador de mensagens:', err);
    }
  });
}

/**
 * Processa a fila de grupos pendentes sem limites artificiais (até o WhatsApp bloquear)
 * @param {import('@whiskeysockets/baileys').WASocket} sock 
 * @param {string} defaultTargetJid 
 */
async function processQueueBatch(sock, defaultTargetJid) {
  if (isProcessing) {
    console.log('⏳ O processamento da fila já está em execução no momento.');
    return;
  }

  // Verificar se há uma pausa agendada ativa por bloqueio direto do WhatsApp
  if (queueManager.isScheduledWaitActive()) {
    const nextRun = queueManager.getNextScheduledRun();
    console.log(`🛑 Aguardando tempo de bloqueio do WhatsApp até: ${formatDate(nextRun)}`);
    if (defaultTargetJid) {
      await sock.sendMessage(defaultTargetJid, {
        text: `🛑 *Bloqueio de Frequência do WhatsApp Detectado*\n\nO WhatsApp bloqueou temporariamente novas entradas. A fila continuará em:\n👉 *${formatDate(nextRun)}*\n\nOs grupos permanecem salvos na fila.`
      });
    }
    return;
  }

  const pendingItems = queueManager.getPendingItems();
  if (pendingItems.length === 0) {
    console.log('✅ Nenhum grupo pendente na fila para processar.');
    return;
  }

  isProcessing = true;
  console.log(`\n⚡ Iniciando entradas em alta velocidade. Grupos na fila: ${pendingItems.length}`);

  // Se batchLimit <= 0, processa todos os pendentes de uma vez só!
  const batchSize = (config.batchLimit && config.batchLimit > 0)
    ? Math.min(pendingItems.length, config.batchLimit)
    : pendingItems.length;

  const currentBatch = pendingItems.slice(0, batchSize);

  const batchResults = [];
  let limitReached = false;
  let scheduledNextRun = null;

  for (let i = 0; i < currentBatch.length; i++) {
    const item = currentBatch[i];
    const targetJid = item.targetJid || defaultTargetJid;

    // Sorteia delay ultra rápido entre as requisições (2-5s)
    if (i > 0) {
      const delayMs = getRandomDelay(config.minDelaySeconds, config.maxDelaySeconds);
      console.log(`⏱️ Delay rápido (${(delayMs / 1000).toFixed(1)}s)...`);
      await sleep(delayMs);
    }

    console.log(`[${i + 1}/${currentBatch.length}] Entrando no grupo (código: ${item.code})...`);

    const result = await joinGroup(sock, item.code);

    if (result.success) {
      console.log(`   ✅ Sucesso! Entrou no grupo: "${result.groupName}"`);
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
      queueManager.markProcessed(item.code, 'failed', result.reason, result.groupName);
      batchResults.push({
        code: item.code,
        url: item.url,
        status: 'failed',
        reason: result.reason,
        groupName: result.groupName
      });

      // Se a falha foi imposta diretamente pelo WhatsApp (Rate Limit 429/463/overload)
      if (result.isRateLimited) {
        console.log('⚠️ O WhatsApp retornou restrição de limite (Rate Limit). Pausando a fila por segurança.');
        limitReached = true;
        scheduledNextRun = queueManager.scheduleNextBatch(config.rescheduleHours);
        break;
      }
    }
  }

  const remainingPending = queueManager.getPendingItems().length;

  // Se limite de lote artificial esteve ativo e sobrou algo
  if (!limitReached && remainingPending > 0 && config.batchLimit > 0 && currentBatch.length === config.batchLimit) {
    limitReached = true;
    scheduledNextRun = queueManager.scheduleNextBatch(config.rescheduleHours);
  }

  // Gerar e enviar o relatório final do lote para o solicitante
  const reportJid = defaultTargetJid || currentBatch[0]?.targetJid;
  if (reportJid) {
    const reportText = generateReport({
      results: batchResults,
      totalPending: remainingPending,
      nextRunTime: scheduledNextRun,
      limitReached: limitReached
    });

    try {
      await sock.sendMessage(reportJid, { text: reportText });
      console.log('📊 Relatório enviado com sucesso para o solicitante.');
    } catch (err) {
      console.error('Erro ao enviar relatório:', err.message);
    }
  }

  isProcessing = false;
}

/**
 * Função chamada pelo Cron para verificar se há itens agendados a serem processados
 */
async function checkAndProcessScheduledQueue(sock) {
  if (isProcessing) return;
  if (queueManager.getPendingItems().length > 0 && !queueManager.isScheduledWaitActive()) {
    console.log('⏰ Reprocessando fila de grupos pendentes...');
    const firstPending = queueManager.getPendingItems()[0];
    await processQueueBatch(sock, firstPending?.targetJid);
  }
}

// Iniciar o bot
startBot().catch((err) => {
  console.error('Erro fatal na inicialização do bot:', err);
});
