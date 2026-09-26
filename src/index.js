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
let abortProcessing = false;
let latestQR = null;
let isConnected = false;
let globalSock = null;

// Estado para Modo Real-Time DB (RDB) e Confirmação de Exclusão
let rdbModeEnabled = false;
let rdbTargetJid = null;
let deletePendingMap = new Map(); // targetJid -> timestamp limite

// ---------------------------------------------------------
// MÓDULO AUTO-RESPONDER INTELIGENTE (PV / GP / ALL)
// ---------------------------------------------------------
let autoRespMode = 'off'; // 'off' | 'pv' | 'gp' | 'all'
let autoRespMsg = 'Olá! Para mais informações, ofertas e catálogo completo de produtos, acesse o nosso grupo oficial do WhatsApp.';
let autoRespCooldowns = new Map(); // targetJid -> timestamp (1 hora de cooldown por conversa)
const AUTO_RESP_COOLDOWN_MS = 60 * 60 * 1000; // 3600000ms (1 hora)
let autoRespProcessing = false;
let autoRespQueue = []; // Fila de disparos assíncronos com delay seguro (anti-ban)

// Mídia do auto-responder (Vídeo, Imagem, Áudio, Documento/PDF, etc.)
const DATA_DIR = process.env.DATA_DIR ? path.resolve(process.env.DATA_DIR) : process.cwd();
if (!fs.existsSync(DATA_DIR)) {
  try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch (e) {}
}

const AUTO_RESP_MEDIA_PATH = path.join(DATA_DIR, 'autoresp_media');
const AUTO_RESP_META_PATH = path.join(DATA_DIR, 'autoresp_media_meta.json');

let autoRespMediaMeta = {
  hasMedia: false,
  fileName: '',
  mimeType: '',
  size: 0
};

if (fs.existsSync(AUTO_RESP_META_PATH)) {
  try {
    const raw = fs.readFileSync(AUTO_RESP_META_PATH, 'utf-8');
    autoRespMediaMeta = { ...autoRespMediaMeta, ...JSON.parse(raw) };
    autoRespMediaMeta.hasMedia = fs.existsSync(AUTO_RESP_MEDIA_PATH);
  } catch (e) {}
} else if (fs.existsSync(path.join(DATA_DIR, 'autoresp_video.mp4'))) {
  const legacyVideoPath = path.join(DATA_DIR, 'autoresp_video.mp4');
  try {
    fs.renameSync(legacyVideoPath, AUTO_RESP_MEDIA_PATH);
    const size = fs.statSync(AUTO_RESP_MEDIA_PATH).size;
    autoRespMediaMeta = { hasMedia: true, fileName: 'autoresp_video.mp4', mimeType: 'video/mp4', size };
    fs.writeFileSync(AUTO_RESP_META_PATH, JSON.stringify(autoRespMediaMeta, null, 2));
  } catch (e) {}
}

// ---------------------------------------------------------
// MÓDULO DISPARO / DIVULGAÇÃO EM MASSA (MÍDIAS + TEXTO + MENÇÃO INVISÍVEL)
// ---------------------------------------------------------
const DIVULGAR_MEDIA_PATH = path.join(DATA_DIR, 'divulgar_media');
const DIVULGAR_META_PATH = path.join(DATA_DIR, 'divulgar_media_meta.json');

let divulgarMediaMeta = {
  hasMedia: false,
  fileName: '',
  mimeType: '',
  mediaType: '',
  size: 0
};

if (fs.existsSync(DIVULGAR_META_PATH)) {
  try {
    const raw = fs.readFileSync(DIVULGAR_META_PATH, 'utf-8');
    divulgarMediaMeta = { ...divulgarMediaMeta, ...JSON.parse(raw) };
    divulgarMediaMeta.hasMedia = fs.existsSync(DIVULGAR_MEDIA_PATH);
  } catch (e) {}
}

let isBroadcasting = false;
let abortBroadcast = false;
let broadcastProgress = {
  active: false,
  total: 0,
  current: 0,
  success: 0,
  failed: 0,
  mediaType: 'text',
  message: '',
  startedAt: null
};

// Funções auxiliares para inferir tipo e parse de multipart
function inferMimeAndType(filename, rawMime) {
  let mimeType = (rawMime || '').trim().toLowerCase();
  const ext = path.extname(filename || '').toLowerCase();

  if (!mimeType || mimeType === 'application/octet-stream') {
    if (['.mp4', '.mkv', '.webm', '.avi', '.mov', '.3gp'].includes(ext)) mimeType = 'video/mp4';
    else if (['.jpg', '.jpeg', '.png', '.webp', '.gif', '.bmp'].includes(ext)) mimeType = 'image/jpeg';
    else if (['.mp3', '.ogg', '.wav', '.m4a', '.aac', '.opus'].includes(ext)) mimeType = 'audio/mp4';
    else if (ext === '.pdf') mimeType = 'application/pdf';
    else if (['.doc', '.docx'].includes(ext)) mimeType = 'application/msword';
    else if (['.xls', '.xlsx'].includes(ext)) mimeType = 'application/vnd.ms-excel';
    else if (['.zip', '.rar', '.7z'].includes(ext)) mimeType = 'application/zip';
    else if (['.txt'].includes(ext)) mimeType = 'text/plain';
    else mimeType = 'application/octet-stream';
  }

  let mediaType = 'document';
  if (mimeType.startsWith('image/')) mediaType = 'image';
  else if (mimeType.startsWith('video/')) mediaType = 'video';
  else if (mimeType.startsWith('audio/')) mediaType = 'audio';
  else mediaType = 'document';

  return { mimeType, mediaType };
}

function parseMultipartBuffer(body, contentType) {
  const boundaryMatch = contentType.match(/boundary=(?:"([^"]+)"|([^;]+))/i);
  const boundary = boundaryMatch ? (boundaryMatch[1] || boundaryMatch[2]) : null;
  if (!boundary) return null;

  const boundaryBuf = Buffer.from('--' + boundary);
  let fileData = null;
  let filename = 'arquivo';
  let mimeType = 'application/octet-stream';
  const fields = {};

  let start = 0;
  while (start < body.length) {
    const idx = body.indexOf(boundaryBuf, start);
    if (idx === -1) break;
    const nextStart = idx + boundaryBuf.length;
    if (body[nextStart] === 45 && body[nextStart + 1] === 45) break;
    const headerEnd = body.indexOf(Buffer.from('\r\n\r\n'), nextStart);
    if (headerEnd === -1) break;
    const header = body.slice(nextStart + 2, headerEnd).toString();
    const dataStart = headerEnd + 4;
    const nextBoundary = body.indexOf(boundaryBuf, dataStart);
    const dataEnd = nextBoundary === -1 ? body.length : nextBoundary - 2;

    const nameMatch = header.match(/name=["']?([^"';\r\n]+)["']?/i);
    const fieldName = nameMatch ? nameMatch[1] : null;

    if (header.includes('filename=')) {
      fileData = body.slice(dataStart, dataEnd);
      const fnMatch = header.match(/filename=["']?([^"';\r\n]+)["']?/i);
      if (fnMatch && fnMatch[1]) filename = fnMatch[1].trim();
      const ctMatch = header.match(/Content-Type:\s*([^\r\n]+)/i);
      if (ctMatch && ctMatch[1]) mimeType = ctMatch[1].trim();
    } else if (fieldName) {
      fields[fieldName] = body.slice(dataStart, dataEnd).toString('utf-8');
    }

    start = nextBoundary === -1 ? body.length : nextBoundary;
  }

  return { fileData, filename, mimeType, fields };
}

// Função central de divulgação para todos os grupos abertos com menção invisível
async function executeBroadcast({
  sock,
  text = '',
  mediaBuffer = null,
  mediaType = null,
  mimeType = null,
  fileName = '',
  fromJid = null
}) {
  if (isBroadcasting) {
    if (fromJid) {
      await sock.sendMessage(fromJid, {
        text: `⚠️ Já existe uma divulgação em andamento (${broadcastProgress.current}/${broadcastProgress.total}). Aguarde a finalização.`
      });
    }
    return { success: false, error: 'Divulgação já em andamento' };
  }

  if (!sock || !isConnected) {
    if (fromJid) {
      await sock.sendMessage(fromJid, { text: '⚠️ WhatsApp não está conectado no momento.' });
    }
    return { success: false, error: 'WhatsApp desconectado' };
  }

  isBroadcasting = true;
  abortBroadcast = false;

  let allGroups = [];
  let openGroups = [];
  try {
    const groupsDict = await sock.groupFetchAllParticipating();
    allGroups = Object.values(groupsDict);
    openGroups = allGroups.filter(g => !g.announce);
  } catch (err) {
    console.error('❌ Erro ao buscar grupos para divulgação:', err.message);
    isBroadcasting = false;
    if (fromJid) {
      await sock.sendMessage(fromJid, { text: `❌ Erro ao buscar lista de grupos: ${err.message}` });
    }
    return { success: false, error: err.message };
  }

  if (openGroups.length === 0) {
    isBroadcasting = false;
    if (fromJid) {
      await sock.sendMessage(fromJid, {
        text: `⚠️ Nenhum grupo com chat aberto encontrado (${allGroups.length} grupos verificados no total).`
      });
    }
    return { success: false, error: 'Nenhum grupo aberto encontrado' };
  }

  const mediaLabel = mediaType === 'image' ? '🖼️ Imagem' :
                     mediaType === 'video' ? '📹 Vídeo' :
                     mediaType === 'audio' ? '🎵 Áudio' :
                     mediaType === 'document' ? '📄 Documento/PDF' : '📝 Texto';

  broadcastProgress = {
    active: true,
    total: openGroups.length,
    current: 0,
    success: 0,
    failed: 0,
    mediaType: mediaType || 'text',
    message: text || '',
    startedAt: new Date().toISOString()
  };

  if (fromJid) {
    await sock.sendMessage(fromJid, {
      text: `🚀 *Iniciando Disparo de Divulgação!*\n\n` +
            `• Tipo: *${mediaLabel}*\n` +
            `• Grupos Abertos: *${openGroups.length}*\n` +
            `• Menção Invisível: *Ativada (Notifica todos os membros)*\n` +
            `• Delay seguro: *3 segundos por grupo*\n\n` +
            `_O bot enviará um relatório final assim que concluir._`
    });
  }

  let sucessos = 0;
  let falhas = 0;

  for (let i = 0; i < openGroups.length; i++) {
    if (abortBroadcast) {
      console.log('🛑 [Divulgar] Divulgação cancelada pelo usuário.');
      break;
    }

    const group = openGroups[i];
    broadcastProgress.current = i + 1;

    try {
      const participants = group.participants ? group.participants.map(p => p.id) : [];

      if (mediaBuffer && mediaType === 'image') {
        await sock.sendMessage(group.id, {
          image: mediaBuffer,
          caption: text || '',
          mentions: participants,
          mimetype: mimeType || 'image/jpeg'
        });
      } else if (mediaBuffer && mediaType === 'video') {
        await sock.sendMessage(group.id, {
          video: mediaBuffer,
          caption: text || '',
          mentions: participants,
          mimetype: mimeType || 'video/mp4'
        });
      } else if (mediaBuffer && mediaType === 'audio') {
        await sock.sendMessage(group.id, {
          audio: mediaBuffer,
          mimetype: mimeType || 'audio/mp4',
          ptt: true,
          mentions: participants
        });
        if (text) {
          await sock.sendMessage(group.id, {
            text: text,
            mentions: participants
          });
        }
      } else if (mediaBuffer && mediaType === 'document') {
        await sock.sendMessage(group.id, {
          document: mediaBuffer,
          caption: text || '',
          fileName: fileName || 'documento.pdf',
          mimetype: mimeType || 'application/pdf',
          mentions: participants
        });
      } else {
        await sock.sendMessage(group.id, {
          text: text,
          mentions: participants
        });
      }

      sucessos++;
      broadcastProgress.success = sucessos;
      console.log(`📢 [Divulgar] (${i + 1}/${openGroups.length}) Enviado para "${group.subject || group.id}"`);
    } catch (e) {
      falhas++;
      broadcastProgress.failed = falhas;
      console.error(`❌ [Divulgar] Falha no grupo "${group.subject || group.id}": ${e.message}`);
    }

    if (i < openGroups.length - 1 && !abortBroadcast) {
      await new Promise(r => setTimeout(r, 3000));
    }
  }

  isBroadcasting = false;
  broadcastProgress.active = false;

  if (fromJid) {
    try {
      await sock.sendMessage(fromJid, {
        text: `✅ *Divulgação Concluída!*\n\n` +
              `• Tipo: *${mediaLabel}*\n` +
              `• ✅ Sucessos: *${sucessos}*\n` +
              `• ❌ Falhas: *${falhas}*\n` +
              `• 📊 Total grupos abertos: *${openGroups.length}*`
      });
    } catch (e) {}
  }

  return { success: true, total: openGroups.length, sucessos, falhas };
}

// Função para gerar variações únicas com emojis aleatórios (evita detecção de spam pelo WhatsApp)
function gerarMensagemAutoResposta(msgBase) {
  const emojis = ['✨', '🔥', '📌', '🚀', '📍', '⭐', '⚡', '💡', '✅', '📲', '🎯', '🛒', '💬', '🎁', '🔔', '📢'];
  const e1 = emojis[Math.floor(Math.random() * emojis.length)];
  const e2 = emojis[Math.floor(Math.random() * emojis.length)];
  const e3 = emojis[Math.floor(Math.random() * emojis.length)];

  const variacoes = [
    `${e1} ${msgBase}\n\n${e2} _Resposta automática_ ${e3}`,
    `${e2} ${msgBase}\n\n${e1} _Atendimento automático_`,
    `${msgBase}\n\n${e1}${e2} _Mensagem enviada automaticamente_ ${e3}`,
    `${e3} *Aviso:* ${msgBase}\n\n${e1} _Canal oficial_`
  ];

  return variacoes[Math.floor(Math.random() * variacoes.length)];
}

// Processador da fila de respostas com delay seguro entre mensagens
async function processAutoRespQueue() {
  if (autoRespProcessing || autoRespQueue.length === 0) return;
  autoRespProcessing = true;

  while (autoRespQueue.length > 0) {
    const item = autoRespQueue.shift();
    try {
      const { sock, fromJid, isGroup, msgObj } = item;
      const textoUnico = gerarMensagemAutoResposta(autoRespMsg);
      let mentions = [];

      if (isGroup) {
        try {
          const metadata = await sock.groupMetadata(fromJid);
          mentions = metadata.participants ? metadata.participants.map(p => p.id) : [];
        } catch (e) {}
      }

      // Envia mídia (vídeo, imagem, áudio ou documento) se houver arquivo configurado, caso contrário envia texto
      if (autoRespMediaMeta.hasMedia && fs.existsSync(AUTO_RESP_MEDIA_PATH)) {
        const mediaBuffer = fs.readFileSync(AUTO_RESP_MEDIA_PATH);
        const mime = (autoRespMediaMeta.mimeType || '').toLowerCase();

        if (mime.startsWith('video/')) {
          await sock.sendMessage(fromJid, {
            video: mediaBuffer,
            caption: textoUnico,
            mentions: isGroup ? mentions : [],
            mimetype: mime || 'video/mp4'
          }, { quoted: msgObj });
        } else if (mime.startsWith('image/')) {
          await sock.sendMessage(fromJid, {
            image: mediaBuffer,
            caption: textoUnico,
            mentions: isGroup ? mentions : [],
            mimetype: mime || 'image/jpeg'
          }, { quoted: msgObj });
        } else if (mime.startsWith('audio/')) {
          await sock.sendMessage(fromJid, {
            audio: mediaBuffer,
            mimetype: mime || 'audio/mp4',
            ptt: false
          }, { quoted: msgObj });
          if (textoUnico) {
            await sock.sendMessage(fromJid, {
              text: textoUnico,
              mentions: isGroup ? mentions : []
            }, { quoted: msgObj });
          }
        } else {
          // Documento (PDF, DOCX, ZIP, TXT, etc.)
          await sock.sendMessage(fromJid, {
            document: mediaBuffer,
            caption: textoUnico,
            fileName: autoRespMediaMeta.fileName || 'arquivo',
            mentions: isGroup ? mentions : [],
            mimetype: mime || 'application/octet-stream'
          }, { quoted: msgObj });
        }
      } else {
        await sock.sendMessage(fromJid, {
          text: textoUnico,
          mentions: isGroup ? mentions : []
        }, { quoted: msgObj });
      }

      console.log(`🤖 [Auto-Responder] Resposta enviada para ${fromJid} (${isGroup ? 'GP' : 'PV'}) com mídia/texto.`);
    } catch (err) {
      console.error('❌ Erro no envio do Auto-Responder:', err.message);
    }

    // Delay seguro de 5 a 8 segundos entre respostas para evitar ban do WhatsApp
    await new Promise(r => setTimeout(r, Math.floor(Math.random() * 3000) + 5000));
  }

  autoRespProcessing = false;
}

// ---------------------------------------------------------
// Servidor HTTP & API REST para Fly.io / Render
// ---------------------------------------------------------
const PORT = process.env.PORT || 3000;

const server = http.createServer(async (req, res) => {
  try {
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
      res.end(JSON.stringify({ status: 'ok', connected: isConnected, rdbMode: rdbModeEnabled, isBroadcasting }));
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

    // Endpoint REST API: DELETE /api/links (Limpar todos os links do banco de dados e da fila local)
    if (pathname === '/api/links' && method === 'DELETE') {
      abortProcessing = true;
      isProcessing = false;
      queueManager.clearSchedule();
      let deletedCount = 0;
      try {
        deletedCount = await deleteAllLinks();
      } catch (e) {
        console.warn('⚠️ Erro ao deletar no PostgreSQL, limpando fila local:', e.message);
      }
      queueManager.clearAll();
      console.log('🗑️ [API] Todos os links foram removidos do banco e da fila local.');
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: true, deletedCount }));
      return;
    }

    // Endpoint REST API: GET /api/status (Status ao vivo para a dashboard)
    if (pathname === '/api/status' && method === 'GET') {
      let stats = { total: 0, pending: 0, success: 0, failed: 0, rate_limited: 0 };
      try {
        stats = await getStats();
      } catch (e) {
        const pending = queueManager.getPendingItems().length;
        stats = { total: pending, pending: pending, success: 0, failed: 0, rate_limited: 0 };
      }

      let qrDataUrl = null;
      if (latestQR && !isConnected) {
        try {
          qrDataUrl = await QRCode.toDataURL(latestQR);
        } catch (e) {}
      }

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          success: true,
          isConnected,
          rdbModeEnabled,
          autoRespMode,
          autoRespMsg,
          autoRespHasVideo: autoRespMediaMeta.hasMedia,
          autoRespMedia: autoRespMediaMeta,
          autoRespCooldownCount: autoRespCooldowns.size,
          isBroadcasting,
          broadcastProgress,
          divulgarMedia: divulgarMediaMeta,
          isProcessing,
          isScheduledWaitActive: queueManager.isScheduledWaitActive(),
          nextScheduledRun: queueManager.getNextScheduledRun(),
          latestQR: qrDataUrl,
          stats
        })
      );
      return;
    }

    // Endpoint: POST /api/autoresp/media — Upload de mídia do Auto-Responder
    if ((pathname === '/api/autoresp/media' || pathname === '/api/autoresp/video') && method === 'POST') {
      const contentType = req.headers['content-type'] || '';
      if (!contentType.includes('multipart/form-data')) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, error: 'Envie o arquivo como multipart/form-data' }));
        return;
      }

      const chunks = [];
      req.on('data', c => chunks.push(c));
      req.on('end', () => {
        try {
          const body = Buffer.concat(chunks);
          const parsed = parseMultipartBuffer(body, contentType);

          if (!parsed || !parsed.fileData || parsed.fileData.length === 0) {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: 'Nenhum arquivo válido encontrado no envio' }));
            return;
          }

          const { mimeType } = inferMimeAndType(parsed.filename, parsed.mimeType);

          fs.writeFileSync(AUTO_RESP_MEDIA_PATH, parsed.fileData);
          autoRespMediaMeta = {
            hasMedia: true,
            fileName: parsed.filename,
            mimeType: mimeType,
            size: parsed.fileData.length
          };
          fs.writeFileSync(AUTO_RESP_META_PATH, JSON.stringify(autoRespMediaMeta, null, 2));

          console.log(`📎 [Auto-Responder] Mídia carregada: ${parsed.filename} (${mimeType}, ${parsed.fileData.length} bytes)`);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: true, size: parsed.fileData.length, meta: autoRespMediaMeta }));
        } catch (err) {
          console.error('❌ Erro ao salvar arquivo do auto-responder:', err.message);
          res.writeHead(500, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: false, error: err.message }));
        }
      });
      return;
    }

    // Endpoint: DELETE /api/autoresp/media — Remove mídia do Auto-Responder
    if ((pathname === '/api/autoresp/media' || pathname === '/api/autoresp/video') && method === 'DELETE') {
      try {
        if (fs.existsSync(AUTO_RESP_MEDIA_PATH)) fs.unlinkSync(AUTO_RESP_MEDIA_PATH);
        if (fs.existsSync(AUTO_RESP_META_PATH)) fs.unlinkSync(AUTO_RESP_META_PATH);
        autoRespMediaMeta = { hasMedia: false, fileName: '', mimeType: '', size: 0 };
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, message: 'Arquivo do auto-responder removido com sucesso' }));
      } catch (err) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, error: err.message }));
      }
      return;
    }

    // Endpoint: POST /api/divulgar/media — Upload de mídia para Divulgação / Broadcast
    if ((pathname === '/api/divulgar/media' || pathname === '/api/broadcast/media') && method === 'POST') {
      const contentType = req.headers['content-type'] || '';
      if (!contentType.includes('multipart/form-data')) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, error: 'Envie o arquivo como multipart/form-data' }));
        return;
      }

      const chunks = [];
      req.on('data', c => chunks.push(c));
      req.on('end', () => {
        try {
          const body = Buffer.concat(chunks);
          const parsed = parseMultipartBuffer(body, contentType);

          if (!parsed || !parsed.fileData || parsed.fileData.length === 0) {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: 'Nenhum arquivo válido encontrado no envio' }));
            return;
          }

          const { mimeType, mediaType } = inferMimeAndType(parsed.filename, parsed.mimeType);

          fs.writeFileSync(DIVULGAR_MEDIA_PATH, parsed.fileData);
          divulgarMediaMeta = {
            hasMedia: true,
            fileName: parsed.filename,
            mimeType: mimeType,
            mediaType: mediaType,
            size: parsed.fileData.length
          };
          fs.writeFileSync(DIVULGAR_META_PATH, JSON.stringify(divulgarMediaMeta, null, 2));

          console.log(`📢 [Divulgar] Mídia de divulgação carregada: ${parsed.filename} (${mimeType} - ${mediaType}, ${parsed.fileData.length} bytes)`);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: true, size: parsed.fileData.length, meta: divulgarMediaMeta }));
        } catch (err) {
          console.error('❌ Erro ao salvar arquivo de divulgação:', err.message);
          res.writeHead(500, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: false, error: err.message }));
        }
      });
      return;
    }

    // Endpoint: DELETE /api/divulgar/media — Remove mídia de divulgação
    if ((pathname === '/api/divulgar/media' || pathname === '/api/broadcast/media') && method === 'DELETE') {
      try {
        if (fs.existsSync(DIVULGAR_MEDIA_PATH)) fs.unlinkSync(DIVULGAR_MEDIA_PATH);
        if (fs.existsSync(DIVULGAR_META_PATH)) fs.unlinkSync(DIVULGAR_META_PATH);
        divulgarMediaMeta = { hasMedia: false, fileName: '', mimeType: '', mediaType: '', size: 0 };
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, message: 'Mídia de divulgação removida com sucesso' }));
      } catch (err) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, error: err.message }));
      }
      return;
    }

    // Endpoint REST API: POST /api/trigger (Ações da Dashboard)
    if (pathname === '/api/trigger' && method === 'POST') {
      let bodyText = '';
      req.on('data', chunk => { bodyText += chunk.toString(); });
      req.on('end', async () => {
        try {
          const body = JSON.parse(bodyText || '{}');
          if (body.action === 'setAutoResp') {
            if (body.mode) autoRespMode = body.mode;
            if (body.msg !== undefined) autoRespMsg = body.msg;
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true, autoRespMode, autoRespMsg }));
            return;
          }
          if (body.action === 'toggleRdb') {
            rdbModeEnabled = !rdbModeEnabled;
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true, rdbModeEnabled }));
            return;
          }
          if (body.action === 'process') {
            if (globalSock) {
              processHybridQueue(globalSock, rdbTargetJid || 'web@system');
            }
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true, message: 'Processamento iniciado' }));
            return;
          }
          if (body.action === 'stop') {
            abortProcessing = true;
            isProcessing = false;
            queueManager.clearSchedule();
            console.log('🛑 [API] Solicitada interrupção do processamento...');
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true, message: 'Processamento interrompido' }));
            return;
          }
          if (body.action === 'broadcast') {
            if (!globalSock || !isConnected) {
              res.writeHead(400, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({ success: false, error: 'WhatsApp desconectado' }));
              return;
            }
            if (isBroadcasting) {
              res.writeHead(400, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({ success: false, error: 'Já existe uma divulgação em andamento' }));
              return;
            }

            const msgTexto = (body.msg || '').trim();
            let mediaBuf = null;
            if (divulgarMediaMeta.hasMedia && fs.existsSync(DIVULGAR_MEDIA_PATH)) {
              mediaBuf = fs.readFileSync(DIVULGAR_MEDIA_PATH);
            }

            if (!msgTexto && !mediaBuf) {
              res.writeHead(400, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({ success: false, error: 'Insira uma mensagem ou anexe uma mídia para divulgar' }));
              return;
            }

            // Dispara a divulgação assincronamente em background
            setImmediate(() => {
              executeBroadcast({
                sock: globalSock,
                text: msgTexto,
                mediaBuffer: mediaBuf,
                mediaType: divulgarMediaMeta.mediaType,
                mimeType: divulgarMediaMeta.mimeType,
                fileName: divulgarMediaMeta.fileName
              });
            });

            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true, message: 'Divulgação iniciada com sucesso para todos os grupos abertos!' }));
            return;
          }
          if (body.action === 'stopBroadcast') {
            abortBroadcast = true;
            isBroadcasting = false;
            broadcastProgress.active = false;
            console.log('🛑 [API] Cancelamento de divulgação solicitado via dashboard.');
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true, message: 'Divulgação cancelada' }));
            return;
          }
          if (body.action === 'resetSession') {
            console.log('🔄 [API] Solicitado reset da sessão do WhatsApp...');
            isConnected = false;
            latestQR = null;
            if (globalSock) {
              try { globalSock.end(new Error('Reset manual de sessão')); } catch (e) {}
            }
            const baseDataDir = process.env.DATA_DIR ? path.resolve(process.env.DATA_DIR) : process.cwd();
            const authPath = process.env.AUTH_DIR
              ? path.resolve(process.env.AUTH_DIR)
              : path.resolve(baseDataDir, 'auth_info');
            
            try {
              if (fs.existsSync(authPath)) {
                fs.rmSync(authPath, { recursive: true, force: true });
              }
            } catch (e) {}

            setTimeout(startBot, 2000);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true, message: 'Sessão resetada com sucesso! Gerando novo QR Code...' }));
            return;
          }
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: false, error: 'Ação inválida' }));
        } catch (e) {
          res.writeHead(500, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: false, error: e.message }));
        }
      });
      return;
    }

    // Página Principal Web: DASHBOARD COMPLETA INTERATIVA
    let stats = { total: 0, pending: 0, success: 0, failed: 0, rate_limited: 0 };
    try {
      stats = await getStats();
    } catch (e) {
      const pending = queueManager.getPendingItems().length;
      stats = { total: pending, pending: pending, success: 0, failed: 0, rate_limited: 0 };
    }

    let initialQrUrl = null;
    if (latestQR && !isConnected) {
      try {
        initialQrUrl = await QRCode.toDataURL(latestQR);
      } catch (e) {}
    }

    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(`
<!DOCTYPE html>
<html lang="pt">
<head>
  <meta charset="UTF-8">
  <title>WhatsApp Whabot MZ Dashboard</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Segoe+UI:wght@400;600;700;800&display=swap" rel="stylesheet">
  <style>
    :root {
      --wa-bg: #111b21;
      --wa-panel: #202c33;
      --wa-panel-header: #182229;
      --wa-border: #2a3942;
      --wa-green: #00a884;
      --wa-green-hover: #06cf9c;
      --wa-header-bg: #008069;
      --wa-text: #e9edef;
      --wa-text-muted: #8696a0;
      --wa-card-inner: #111b21;
      --wa-red: #ea0038;
      --wa-red-hover: #f87171;
      --wa-yellow: #ffbc11;
      --wa-blue: #53bdeb;
      --wa-purple: #a855f7;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, Helvetica, Arial, sans-serif; }
    body { background: var(--wa-bg); color: var(--wa-text); min-height: 100vh; padding-bottom: 2rem; }
    
    /* WHATSAPP APP BAR HEADER */
    .top-bar { background: var(--wa-header-bg); padding: 1rem 1.5rem; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 1rem; box-shadow: 0 2px 8px rgba(0,0,0,0.4); }
    .brand-title { display: flex; align-items: center; gap: 10px; font-size: 1.35rem; font-weight: 800; color: #fff; letter-spacing: -0.3px; }
    .brand-icon { width: 34px; height: 34px; background: #fff; color: var(--wa-header-bg); border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 1.2rem; font-weight: 900; }
    
    .status-badges { display: flex; gap: 0.6rem; align-items: center; flex-wrap: wrap; }
    .badge { padding: 6px 14px; border-radius: 20px; font-size: 0.8rem; font-weight: 700; display: inline-flex; align-items: center; gap: 6px; text-transform: uppercase; letter-spacing: 0.5px; }
    .badge-success { background: rgba(37, 211, 102, 0.18); color: #25d366; border: 1px solid rgba(37, 211, 102, 0.4); }
    .badge-warning { background: rgba(255, 188, 17, 0.18); color: var(--wa-yellow); border: 1px solid rgba(255, 188, 17, 0.4); }
    .badge-info { background: rgba(83, 189, 235, 0.18); color: var(--wa-blue); border: 1px solid rgba(83, 189, 235, 0.4); }
    .badge-active { background: rgba(0, 168, 132, 0.2); color: var(--wa-green-hover); border: 1px solid var(--wa-green); }
    .badge-purple { background: rgba(168, 85, 247, 0.2); color: #c084fc; border: 1px solid rgba(168, 85, 247, 0.5); }

    .container { max-width: 1140px; margin: 1.5rem auto 0; padding: 0 1rem; }

    /* STATS GRID */
    .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 1rem; margin-bottom: 1.5rem; }
    .stat-card { background: var(--wa-panel); border: 1px solid var(--wa-border); border-radius: 12px; padding: 1.2rem; position: relative; overflow: hidden; }
    .stat-card::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 3px; background: var(--wa-border); }
    .stat-card.st-pend::before { background: var(--wa-yellow); }
    .stat-card.st-succ::before { background: #25d366; }
    .stat-card.st-fail::before { background: var(--wa-red); }
    .stat-card.st-tot::before { background: var(--wa-blue); }
    
    .stat-lbl { font-size: 0.78rem; color: var(--wa-text-muted); font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em; }
    .stat-val { font-size: 2.1rem; font-weight: 800; margin-top: 0.4rem; }

    /* MAIN PANELS GRID */
    .main-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem; margin-bottom: 1.5rem; }
    @media (max-width: 880px) { .main-grid { grid-template-columns: 1fr; } }

    .panel-card { background: var(--wa-panel); border: 1px solid var(--wa-border); border-radius: 12px; padding: 1.4rem; }
    .panel-header-title { font-size: 1.05rem; font-weight: 700; color: var(--wa-text); margin-bottom: 1rem; display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid var(--wa-border); padding-bottom: 0.75rem; }
    .panel-header-title span { display: flex; align-items: center; gap: 8px; }

    /* QR CONTAINER */
    .qr-container { text-align: center; padding: 1.2rem; background: var(--wa-card-inner); border-radius: 10px; border: 1px dashed var(--wa-border); min-height: 290px; display: flex; flex-direction: column; align-items: center; justify-content: center; }
    .qr-container img { width: 220px; height: 220px; border-radius: 10px; border: 4px solid #fff; }

    /* INPUTS & TEXTAREAS */
    textarea { width: 100%; height: 110px; background: var(--wa-card-inner); border: 1px solid var(--wa-border); border-radius: 8px; color: var(--wa-text); padding: 0.8rem; font-size: 0.9rem; resize: vertical; outline: none; margin-bottom: 0.8rem; transition: border-color 0.2s; }
    textarea:focus { border-color: var(--wa-green); }

    select { background: var(--wa-card-inner); border: 1px solid var(--wa-border); color: var(--wa-text); padding: 10px 12px; border-radius: 8px; font-weight: 600; outline: none; transition: border-color 0.2s; }
    select:focus { border-color: var(--wa-green); }

    /* BUTTONS */
    .btn-group { display: flex; gap: 0.6rem; flex-wrap: wrap; }
    button { background: var(--wa-green); color: #111b21; border: none; padding: 10px 16px; border-radius: 8px; font-weight: 700; font-size: 0.88rem; cursor: pointer; transition: all 0.2s; display: inline-flex; align-items: center; justify-content: center; gap: 6px; }
    button:hover { background: var(--wa-green-hover); transform: translateY(-1px); }
    button:active { transform: translateY(0); }
    button.btn-sec { background: var(--wa-panel-header); color: var(--wa-text); border: 1px solid var(--wa-border); }
    button.btn-sec:hover { background: #222e35; border-color: var(--wa-green); }
    button.btn-danger { background: rgba(234, 0, 56, 0.2); color: #f87171; border: 1px solid rgba(234, 0, 56, 0.4); }
    button.btn-danger:hover { background: var(--wa-red); color: #fff; }
    button.btn-purple { background: #7e22ce; color: #fff; border: 1px solid #a855f7; }
    button.btn-purple:hover { background: #9333ea; }

    /* TABLE */
    .table-card { background: var(--wa-panel); border: 1px solid var(--wa-border); border-radius: 12px; padding: 1.4rem; overflow-x: auto; }
    table { width: 100%; border-collapse: collapse; text-align: left; font-size: 0.88rem; }
    th { padding: 10px 14px; background: var(--wa-panel-header); color: var(--wa-text-muted); font-weight: 700; font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.5px; border-bottom: 1px solid var(--wa-border); }
    td { padding: 12px 14px; border-bottom: 1px solid var(--wa-border); word-break: break-all; }
    tr:hover td { background: rgba(255,255,255,0.02); }

    .status-tag { padding: 3px 10px; border-radius: 12px; font-size: 0.75rem; font-weight: 700; text-transform: uppercase; }
    .tag-pending { background: rgba(255, 188, 17, 0.18); color: var(--wa-yellow); border: 1px solid rgba(255, 188, 17, 0.3); }
    .tag-success { background: rgba(37, 211, 102, 0.18); color: #25d366; border: 1px solid rgba(37, 211, 102, 0.3); }
    .tag-failed { background: rgba(234, 0, 56, 0.18); color: #f87171; border: 1px solid rgba(234, 0, 56, 0.3); }
    
    .toast { position: fixed; bottom: 24px; right: 24px; background: var(--wa-panel-header); color: #fff; padding: 12px 22px; border-radius: 10px; border: 1px solid var(--wa-green); box-shadow: 0 10px 30px rgba(0,0,0,0.6); display: none; z-index: 99; font-weight: 600; font-size: 0.9rem; }

    .progress-bar-wrap { background: var(--wa-card-inner); border-radius: 8px; overflow: hidden; height: 10px; margin: 10px 0; border: 1px solid var(--wa-border); }
    .progress-bar-fill { background: var(--wa-green); height: 100%; width: 0%; transition: width 0.3s ease; }
  </style>
</head>
<body>

  <!-- TOP HEADER BAR -->
  <div class="top-bar">
    <div class="brand-title">
      <div class="brand-icon">💬</div>
      <span>Whabot MZ Dashboard</span>
    </div>

    <div class="status-badges">
      <div id="conn-badge" class="badge ${isConnected ? 'badge-success' : 'badge-warning'}">
        ${isConnected ? '🟢 Conectado' : '🟡 Desconectado'}
      </div>
      <div id="proc-badge" class="badge ${isProcessing ? 'badge-active' : 'badge-info'}">
        ${isProcessing ? '⚡ Processando Fila...' : '⏸️ Fila em Espera'}
      </div>
      <div id="rdb-badge" class="badge ${rdbModeEnabled ? 'badge-info' : 'badge-warning'}">
        ${rdbModeEnabled ? '⚡ RDB Real-Time Ativo' : '⏹️ RDB Inativo'}
      </div>
      <div id="bcast-badge" class="badge ${isBroadcasting ? 'badge-purple' : 'badge-info'}">
        ${isBroadcasting ? '📢 Divulgando...' : '📢 Divulgação Pronta'}
      </div>
    </div>
  </div>

  <div class="container">
    <!-- STATS CARDS -->
    <div class="stats-grid">
      <div class="stat-card st-pend">
        <div class="stat-lbl">Pendentes</div>
        <div class="stat-val" id="st-pending" style="color: var(--wa-yellow);">${stats.pending}</div>
      </div>
      <div class="stat-card st-succ">
        <div class="stat-lbl">Entrou (Sucessos)</div>
        <div class="stat-val" id="st-success" style="color: #25d366;">${stats.success}</div>
      </div>
      <div class="stat-card st-fail">
        <div class="stat-lbl">Falhas / Rate Limit</div>
        <div class="stat-val" id="st-failed" style="color: #f87171;">${stats.failed}</div>
      </div>
      <div class="stat-card st-tot">
        <div class="stat-lbl">Total Geral Cadastrado</div>
        <div class="stat-val" id="st-total" style="color: var(--wa-blue);">${stats.total}</div>
      </div>
    </div>

    <!-- MAIN GRID CONTROLS -->
    <div class="main-grid">
      <!-- CONEXÃO / QR CODE -->
      <div class="panel-card">
        <div class="panel-header-title">
          <span>📲 Sessão & Conexão WhatsApp</span>
          <button id="btn-reset-session" class="btn-danger" style="padding: 5px 12px; font-size: 0.78rem;">📲 Gerar Novo QR Code</button>
        </div>
        <div class="qr-container" id="qr-box">
          ${
            isConnected
              ? '<div style="color: #25d366; font-size: 1.25rem; font-weight: 700;">✅ WhatsApp Conectado & Ativo</div><p style="color: var(--wa-text-muted); font-size: 0.85rem; margin-top: 10px;">O bot está respondendo e pronto para processar grupos.</p>'
              : initialQrUrl
              ? `<img src="${initialQrUrl}" alt="QR Code WhatsApp" /><p style="color: var(--wa-text-muted); font-size: 0.85rem; margin-top: 10px;">Abra o WhatsApp > Aparelhos Conectados > Conectar um Aparelho</p>`
              : '<p style="color: var(--wa-text-muted);">⏳ Carregando código QR...</p>'
          }
        </div>
      </div>

      <!-- INSERIR LINKS & PAINEL DE CONTROLE -->
      <div class="panel-card">
        <div class="panel-header-title">
          <span>📥 Adicionar Links de Grupos</span>
        </div>
        <textarea id="links-input" placeholder="Cole aqui os links dos grupos (https://chat.whatsapp.com/...)"></textarea>
        
        <div class="btn-group" style="margin-bottom: 1.2rem;">
          <button id="btn-add">➕ Adicionar Links</button>
          <button id="btn-process" class="btn-sec">⚡ Iniciar Processamento</button>
          <button id="btn-stop" class="btn-danger">🛑 Parar Processamento</button>
          <button id="btn-toggle-rdb" class="btn-sec">🔄 Alternar RDB</button>
        </div>

        <div style="border-top: 1px solid var(--wa-border); padding-top: 1rem; margin-top: 0.5rem; display: flex; justify-content: flex-end;">
          <button id="btn-clear" class="btn-danger">🗑️ Limpar Todos os Links</button>
        </div>
      </div>
    </div>

    <!-- PAINEL DE DISPARO / DIVULGAÇÃO EM MASSA (MÍDIAS + TEXTO + MENÇÃO INVISÍVEL) -->
    <div class="panel-card" style="margin-bottom: 1.5rem; border: 1px solid rgba(168, 85, 247, 0.4);">
      <div class="panel-header-title">
        <span>📢 Disparo de Divulgação em Massa (Grupos Abertos + Menção Invisível)</span>
        <span id="divulgar-status-tag" class="badge badge-info" style="font-size:0.75rem;">Apenas Grupos Abertos</span>
      </div>

      <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem; align-items: start;">
        <div>
          <label style="font-size: 0.82rem; color: var(--wa-text-muted); font-weight: 700; display: block; margin-bottom: 6px;">Mensagem ou Legenda da Divulgação:</label>
          <textarea id="divulgar-msg" style="height: 110px; margin-bottom: 0.75rem;" placeholder="Digite aqui a mensagem que será disparada para todos os grupos abertos..."></textarea>
          
          <p style="font-size: 0.8rem; color: var(--wa-text-muted); line-height: 1.5; margin-bottom: 10px;">
            • <b>Menção Invisível:</b> Notifica todos os membros de cada grupo!<br>
            • <b>Filtro Automático:</b> Dispara somente onde o chat é aberto para membros.<br>
            • <b>Anti-Ban:</b> Delay seguro de 3s entre envios.
          </p>
        </div>

        <div>
          <label style="font-size: 0.82rem; color: var(--wa-text-muted); font-weight: 700; display: block; margin-bottom: 6px;">📎 Anexar Mídia (Imagem, Vídeo, Áudio, PDF, etc.):</label>
          <div id="divulgar-upload-area" style="border: 2px dashed var(--wa-border); border-radius: 10px; padding: 14px; text-align: center; cursor: pointer; margin-bottom: 0.75rem; transition: border-color 0.2s;" onclick="document.getElementById('divulgar-media-input').click()">
            <div id="divulgar-upload-label" style="color: var(--wa-text-muted); font-size: 0.85rem;">📂 Clique para selecionar qualquer mídia (Imagem, Vídeo, Áudio, PDF)</div>
            <input id="divulgar-media-input" type="file" accept="*" style="display:none">
          </div>

          <div id="divulgar-media-bar" style="display:none; background: rgba(168,85,247,0.12); border: 1px solid rgba(168,85,247,0.3); border-radius: 8px; padding: 8px 12px; margin-bottom: 0.75rem; display: flex; align-items: center; justify-content: space-between;">
            <span id="divulgar-media-text" style="font-size:0.82rem; color:#c084fc;">✅ Mídia anexada</span>
            <button id="btn-remove-divulgar-media" style="background: rgba(234,0,56,0.2); border: 1px solid rgba(234,0,56,0.4); color: #f87171; padding: 3px 10px; border-radius: 6px; cursor: pointer; font-size: 0.8rem;">🗑️ Remover Mídia</button>
          </div>

          <div class="btn-group" style="margin-top: 5px;">
            <button id="btn-start-broadcast" class="btn-purple" style="flex: 1;">📢 Disparar Divulgação Agora</button>
            <button id="btn-stop-broadcast" class="btn-danger" style="display: none;">🛑 Cancelar Disparo</button>
          </div>

          <!-- Barra de Progresso ao Vivo -->
          <div id="divulgar-prog-box" style="display: none; margin-top: 10px;">
            <div style="display:flex; justify-content:space-between; font-size:0.8rem; color:var(--wa-text-muted);">
              <span id="divulgar-prog-text">Progresso do disparo...</span>
              <span id="divulgar-prog-pct">0%</span>
            </div>
            <div class="progress-bar-wrap">
              <div id="divulgar-prog-fill" class="progress-bar-fill"></div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- AUTO-RESPONDER INTELIGENTE -->
    <div class="panel-card" style="margin-bottom: 1.5rem;">
      <div class="panel-header-title">
        <span>🤖 Auto-Responder Inteligente (PV / GP / ALL)</span>
      </div>
      <div style="display: grid; grid-template-columns: 1fr 2fr; gap: 1.5rem; align-items: start;">
        <div>
          <label style="font-size: 0.82rem; color: var(--wa-text-muted); font-weight: 700; display: block; margin-bottom: 6px;">Modo de Atuação:</label>
          <select id="autoresp-mode" style="width: 100%; margin-bottom: 1rem;">
            <option value="off">⏹️ Desativado</option>
            <option value="pv">👤 Apenas Mensagens Privadas (PV)</option>
            <option value="gp">👥 Apenas Grupos (GP) + Menção Invisível</option>
            <option value="all">🌐 Todos (PV + Grupos)</option>
          </select>
          <p style="font-size: 0.8rem; color: var(--wa-text-muted); line-height: 1.5;">
            • <b>Menção Invisível:</b> Notifica todos no grupo.<br>
            • <b>Variação Anti-Ban:</b> Emojis dinâmicos.<br>
            • <b>Cooldown:</b> Máx 1 resposta por conversa/hora.<br>
            • <b>Delay Seguro:</b> 5s - 8s entre respostas.
          </p>
        </div>
        <div>
          <label style="font-size: 0.82rem; color: var(--wa-text-muted); font-weight: 700; display: block; margin-bottom: 6px;">Mensagem / Legenda de Resposta:</label>
          <textarea id="autoresp-msg" style="height: 70px; margin-bottom: 0.75rem;" placeholder="Digite a mensagem ou legenda do arquivo..."></textarea>

          <label style="font-size: 0.82rem; color: var(--wa-text-muted); font-weight: 700; display: block; margin-bottom: 6px;">📎 Anexo Mídia / Arquivo (Vídeo, Imagem, PDF, Áudio, Documento, etc.):</label>
          <div id="media-upload-area" style="border: 2px dashed var(--wa-border); border-radius: 10px; padding: 14px; text-align: center; cursor: pointer; margin-bottom: 0.75rem; transition: border-color 0.2s;" onclick="document.getElementById('autoresp-media-input').click()">
            <div id="media-upload-label" style="color: var(--wa-text-muted); font-size: 0.85rem;">📂 Clique para selecionar qualquer arquivo (máx 100MB)</div>
            <input id="autoresp-media-input" type="file" accept="*" style="display:none">
          </div>
          <div id="media-status-bar" style="display:none; background: rgba(37,211,102,0.12); border: 1px solid rgba(37,211,102,0.3); border-radius: 8px; padding: 8px 12px; margin-bottom: 0.75rem; display: flex; align-items: center; justify-content: space-between;">
            <span id="media-status-text" style="font-size:0.82rem; color:#25d366;">✅ Arquivo anexado</span>
            <button id="btn-remove-media" style="background: rgba(234,0,56,0.2); border: 1px solid rgba(234,0,56,0.4); color: #f87171; padding: 3px 10px; border-radius: 6px; cursor: pointer; font-size: 0.8rem;">🗑️ Remover Anexo</button>
          </div>

          <button id="btn-save-autoresp" style="width: 100%;">💾 Salvar Configurações do Auto-Responder</button>
        </div>
      </div>
    </div>

    <!-- TABELA DE LINKS -->
    <div class="table-card">
      <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem; flex-wrap: wrap; gap: 0.5rem;">
        <div class="panel-header-title" style="margin-bottom: 0; border: none; padding: 0;">
          <span>📋 Grupos Cadastrados no Banco / Fila</span>
        </div>
        <input id="search-table" type="text" placeholder="Filtrar grupos por nome ou URL..." style="background: var(--wa-card-inner); border: 1px solid var(--wa-border); color: #fff; padding: 8px 14px; border-radius: 8px; font-size: 0.85rem; outline: none; min-width: 250px;">
      </div>

      <table>
        <thead>
          <tr>
            <th>#</th>
            <th>Nome do Grupo</th>
            <th>URL / Link</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody id="table-body">
          <tr><td colspan="4" style="text-align: center; color: var(--wa-text-muted);">Carregando grupos...</td></tr>
        </tbody>
      </table>
    </div>
  </div>

  <div class="toast" id="toast"></div>

  <script>
    function showToast(msg) {
      const t = document.getElementById('toast');
      t.innerText = msg;
      t.style.display = 'block';
      setTimeout(() => { t.style.display = 'none'; }, 3000);
    }

    // Proteção contra sobrescrita durante digitação do usuário
    const userEditingSet = new Set();
    ['autoresp-msg', 'autoresp-mode', 'links-input', 'divulgar-msg'].forEach(id => {
      const el = document.getElementById(id);
      if (el) {
        el.addEventListener('focus', () => userEditingSet.add(id));
        el.addEventListener('blur', () => userEditingSet.delete(id));
        el.addEventListener('input', () => userEditingSet.add(id));
      }
    });

    async function updateStatus() {
      try {
        const res = await fetch('/api/status');
        const d = await res.json();
        if (d.success) {
          // Badges
          const connBadge = document.getElementById('conn-badge');
          connBadge.className = 'badge ' + (d.isConnected ? 'badge-success' : 'badge-warning');
          connBadge.innerHTML = d.isConnected ? '🟢 Conectado' : '🟡 Desconectado';

          const procBadge = document.getElementById('proc-badge');
          procBadge.className = 'badge ' + (d.isProcessing ? 'badge-active' : 'badge-info');
          procBadge.innerHTML = d.isProcessing ? '⚡ Processando Fila...' : '⏸️ Fila em Espera';

          const rdbBadge = document.getElementById('rdb-badge');
          rdbBadge.className = 'badge ' + (d.rdbModeEnabled ? 'badge-info' : 'badge-warning');
          rdbBadge.innerHTML = d.rdbModeEnabled ? '⚡ RDB Real-Time Ativo' : '⏹️ RDB Inativo';

          const bcastBadge = document.getElementById('bcast-badge');
          bcastBadge.className = 'badge ' + (d.isBroadcasting ? 'badge-purple' : 'badge-info');
          bcastBadge.innerHTML = d.isBroadcasting ? '📢 Divulgando...' : '📢 Divulgação Pronta';

          // Stats
          document.getElementById('st-pending').innerText = d.stats.pending;
          document.getElementById('st-success').innerText = d.stats.success;
          document.getElementById('st-failed').innerText = d.stats.failed;
          document.getElementById('st-total').innerText = d.stats.total;

          // Auto-Responder UI
          const modeSel = document.getElementById('autoresp-mode');
          if (modeSel && document.activeElement !== modeSel && !userEditingSet.has('autoresp-mode')) {
            modeSel.value = d.autoRespMode || 'off';
          }
          const msgArea = document.getElementById('autoresp-msg');
          if (msgArea && document.activeElement !== msgArea && !userEditingSet.has('autoresp-msg')) {
            msgArea.value = d.autoRespMsg || '';
          }

          // Status do anexo de Auto-Responder
          const msBar = document.getElementById('media-status-bar');
          const msText = document.getElementById('media-status-text');
          const media = d.autoRespMedia || {};
          if (msBar && msText) {
            if (media.hasMedia) {
              msBar.style.display = 'flex';
              msText.textContent = '✅ Anexo: ' + (media.fileName || 'arquivo') + ' (' + (media.mimeType || 'mídia') + ')';
            } else {
              msBar.style.display = 'none';
            }
          }

          // Status do anexo de Divulgação
          const divBar = document.getElementById('divulgar-media-bar');
          const divText = document.getElementById('divulgar-media-text');
          const divMedia = d.divulgarMedia || {};
          if (divBar && divText) {
            if (divMedia.hasMedia) {
              divBar.style.display = 'flex';
              divText.textContent = '✅ Mídia: ' + (divMedia.fileName || 'arquivo') + ' (' + (divMedia.mimeType || 'mídia') + ')';
            } else {
              divBar.style.display = 'none';
            }
          }

          // Status do Progresso da Divulgação
          const progBox = document.getElementById('divulgar-prog-box');
          const btnStartBcast = document.getElementById('btn-start-broadcast');
          const btnStopBcast = document.getElementById('btn-stop-broadcast');
          if (d.isBroadcasting && d.broadcastProgress?.active) {
            progBox.style.display = 'block';
            btnStartBcast.style.display = 'none';
            btnStopBcast.style.display = 'inline-flex';
            const bp = d.broadcastProgress;
            const pct = bp.total > 0 ? Math.round((bp.current / bp.total) * 100) : 0;
            document.getElementById('divulgar-prog-pct').innerText = pct + '%';
            document.getElementById('divulgar-prog-fill').style.width = pct + '%';
            document.getElementById('divulgar-prog-text').innerText = 'Enviando ' + bp.current + '/' + bp.total + ' (' + bp.success + ' sucessos, ' + bp.failed + ' falhas)...';
          } else {
            progBox.style.display = 'none';
            btnStartBcast.style.display = 'inline-flex';
            btnStopBcast.style.display = 'none';
          }

          // QR Code Box
          const qrBox = document.getElementById('qr-box');
          if (d.isConnected) {
            qrBox.innerHTML = '<div style="color: #25d366; font-size: 1.25rem; font-weight: 700;">✅ WhatsApp Conectado & Ativo</div><p style="color: var(--wa-text-muted); font-size: 0.85rem; margin-top: 10px;">O bot está respondendo e pronto para processar grupos.</p>';
          } else if (d.latestQR) {
            qrBox.innerHTML = '<img src="' + d.latestQR + '" alt="QR Code WhatsApp" /><p style="color: var(--wa-text-muted); font-size: 0.85rem; margin-top: 10px;">Abra o WhatsApp > Aparelhos Conectados > Conectar um Aparelho</p>';
          } else {
            qrBox.innerHTML = '<p style="color: var(--wa-text-muted);">⏳ Carregando código QR...</p>';
          }
        }
      } catch (e) {}
    }

    async function loadTable() {
      try {
        const res = await fetch('/api/links');
        const d = await res.json();
        if (d.success && Array.isArray(d.data)) {
          const tbody = document.getElementById('table-body');
          const filter = document.getElementById('search-table').value.toLowerCase();
          const items = d.data.filter(i => (i.url || '').toLowerCase().includes(filter) || (i.group_name || '').toLowerCase().includes(filter));
          
          if (items.length === 0) {
            tbody.innerHTML = '<tr><td colspan="4" style="text-align: center; color: var(--wa-text-muted);">Nenhum grupo encontrado</td></tr>';
            return;
          }

          tbody.innerHTML = items.map((item, idx) => {
            let tagClass = 'tag-pending';
            if (item.status === 'success') tagClass = 'tag-success';
            if (item.status === 'failed' || item.status === 'rate_limited') tagClass = 'tag-failed';

            return '<tr>' +
              '<td>' + (idx + 1) + '</td>' +
              '<td style="font-weight: 600;">' + (item.group_name || '—') + '</td>' +
              '<td><a href="' + item.url + '" target="_blank" style="color: var(--wa-green); text-decoration: none;">' + item.url + '</a></td>' +
              '<td><span class="status-tag ' + tagClass + '">' + (item.status || 'pending').toUpperCase() + '</span></td>' +
            '</tr>';
          }).join('');
        }
      } catch (e) {}
    }

    // Event Listeners: Links
    document.getElementById('btn-add').addEventListener('click', async () => {
      const input = document.getElementById('links-input');
      const text = input.value.trim();
      if (!text) { showToast('Cole pelo menos um link!'); return; }

      try {
        const res = await fetch('/api/links', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ text })
        });
        const d = await res.json();
        if (d.success) {
          showToast('✨ ' + d.addedCount + ' novos links adicionados!');
          input.value = '';
          userEditingSet.delete('links-input');
          updateStatus();
          loadTable();
        }
      } catch (e) {
        showToast('Erro ao enviar links!');
      }
    });

    document.getElementById('btn-process').addEventListener('click', async () => {
      try {
        const res = await fetch('/api/trigger', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ action: 'process' })
        });
        const d = await res.json();
        if (d.success) {
          showToast('⚡ Processamento da fila iniciado!');
          updateStatus();
        }
      } catch (e) {}
    });

    document.getElementById('btn-stop').addEventListener('click', async () => {
      try {
        const res = await fetch('/api/trigger', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ action: 'stop' })
        });
        const d = await res.json();
        if (d.success) {
          showToast('🛑 Processamento interrompido!');
          updateStatus();
        }
      } catch (e) {}
    });

    document.getElementById('btn-toggle-rdb').addEventListener('click', async () => {
      try {
        const res = await fetch('/api/trigger', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ action: 'toggleRdb' })
        });
        const d = await res.json();
        if (d.success) {
          showToast('Modo RDB: ' + (d.rdbModeEnabled ? 'ATIVADO' : 'DESATIVADO'));
          updateStatus();
        }
      } catch (e) {}
    });

    document.getElementById('btn-reset-session').addEventListener('click', async () => {
      if (!confirm('Deseja desconectar a sessão atual do WhatsApp e gerar um NOVO QR Code?')) return;
      try {
        const res = await fetch('/api/trigger', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ action: 'resetSession' })
        });
        const d = await res.json();
        if (d.success) {
          showToast('📲 Sessão resetada! Gerando novo QR Code...');
          updateStatus();
        }
      } catch (e) {}
    });

    document.getElementById('btn-clear').addEventListener('click', async () => {
      if (!confirm('Tem certeza que deseja apagar todos os grupos salvos?')) return;
      try {
        const res = await fetch('/api/links', { method: 'DELETE' });
        const d = await res.json();
        if (d.success) {
          showToast('🗑️ Banco e fila limpos com sucesso!');
          updateStatus();
          loadTable();
        }
      } catch (e) {}
    });

    // ── Disparo / Divulgação em Massa (Web Dashboard) ──
    const btnStartBcast = document.getElementById('btn-start-broadcast');
    if (btnStartBcast) {
      btnStartBcast.addEventListener('click', async () => {
        const msg = document.getElementById('divulgar-msg').value.trim();
        const hasMedia = document.getElementById('divulgar-media-bar').style.display !== 'none';

        if (!msg && !hasMedia) {
          showToast('⚠️ Escreva uma mensagem ou anexe uma mídia para divulgar!');
          return;
        }

        if (!confirm('Deseja iniciar o disparo de divulgação para TODOS os grupos com chat aberto?')) return;

        try {
          const res = await fetch('/api/trigger', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: 'broadcast', msg })
          });
          const d = await res.json();
          if (d.success) {
            showToast('🚀 Disparo de divulgação iniciado!');
            updateStatus();
          } else {
            showToast('Erro: ' + (d.error || 'falha ao iniciar disparo'));
          }
        } catch (e) {
          showToast('Erro ao iniciar divulgação');
        }
      });
    }

    const btnStopBcast = document.getElementById('btn-stop-broadcast');
    if (btnStopBcast) {
      btnStopBcast.addEventListener('click', async () => {
        try {
          const res = await fetch('/api/trigger', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: 'stopBroadcast' })
          });
          const d = await res.json();
          if (d.success) {
            showToast('🛑 Divulgação cancelada!');
            updateStatus();
          }
        } catch (e) { showToast('Erro ao cancelar divulgação'); }
      });
    }

    const divulgarMediaInput = document.getElementById('divulgar-media-input');
    const divulgarMediaArea = document.getElementById('divulgar-upload-area');
    const divulgarMediaLabel = document.getElementById('divulgar-upload-label');
    if (divulgarMediaInput) {
      divulgarMediaInput.addEventListener('change', async () => {
        const file = divulgarMediaInput.files[0];
        if (!file) return;
        if (file.size > 100 * 1024 * 1024) { showToast('⚠️ Arquivo deve ter no máximo 100MB!'); return; }
        divulgarMediaLabel.textContent = '⏳ Enviando ' + file.name + '...';
        divulgarMediaArea.style.borderColor = '#ffbc11';
        const formData = new FormData();
        formData.append('media', file, file.name);
        try {
          const res = await fetch('/api/divulgar/media', { method: 'POST', body: formData });
          const d = await res.json();
          if (d.success) {
            divulgarMediaLabel.textContent = '✅ ' + file.name + ' anexado! Clique para trocar.';
            divulgarMediaArea.style.borderColor = '#a855f7';
            document.getElementById('divulgar-media-bar').style.display = 'flex';
            document.getElementById('divulgar-media-text').textContent = '✅ Mídia: ' + file.name;
            showToast('📎 Mídia ' + file.name + ' anexada para divulgação!');
          } else {
            divulgarMediaLabel.textContent = '❌ Erro ao carregar. Tente novamente.';
            divulgarMediaArea.style.borderColor = '#ea0038';
            showToast('Erro: ' + (d.error || 'falha no upload'));
          }
        } catch (e) {
          divulgarMediaLabel.textContent = '❌ Erro de rede. Tente novamente.';
          divulgarMediaArea.style.borderColor = '#ea0038';
          showToast('Erro ao enviar arquivo!');
        }
      });
    }

    const btnRemoveDivulgarMedia = document.getElementById('btn-remove-divulgar-media');
    if (btnRemoveDivulgarMedia) {
      btnRemoveDivulgarMedia.addEventListener('click', async (e) => {
        e.stopPropagation();
        try {
          const res = await fetch('/api/divulgar/media', { method: 'DELETE' });
          const d = await res.json();
          if (d.success) {
            document.getElementById('divulgar-media-bar').style.display = 'none';
            divulgarMediaArea.style.borderColor = 'var(--wa-border)';
            divulgarMediaLabel.textContent = '📂 Clique para selecionar qualquer mídia (Imagem, Vídeo, Áudio, PDF)';
            if (divulgarMediaInput) divulgarMediaInput.value = '';
            showToast('🗑️ Mídia de divulgação removida!');
          }
        } catch (e) { showToast('Erro ao remover mídia'); }
      });
    }

    // ── Auto-Responder Media / File Upload Handling ──
    const mediaInput = document.getElementById('autoresp-media-input');
    const mediaArea = document.getElementById('media-upload-area');
    const mediaLabel = document.getElementById('media-upload-label');
    if (mediaInput) {
      mediaInput.addEventListener('change', async () => {
        const file = mediaInput.files[0];
        if (!file) return;
        if (file.size > 100 * 1024 * 1024) { showToast('⚠️ Arquivo deve ter no máximo 100MB!'); return; }
        mediaLabel.textContent = '⏳ Enviando ' + file.name + '...';
        mediaArea.style.borderColor = '#ffbc11';
        const formData = new FormData();
        formData.append('media', file, file.name);
        try {
          const res = await fetch('/api/autoresp/media', { method: 'POST', body: formData });
          const d = await res.json();
          if (d.success) {
            mediaLabel.textContent = '✅ ' + file.name + ' enviado! Clique para trocar.';
            mediaArea.style.borderColor = '#25d366';
            document.getElementById('media-status-bar').style.display = 'flex';
            document.getElementById('media-status-text').textContent = '✅ Anexo: ' + file.name;
            showToast('📎 Arquivo ' + file.name + ' anexado com sucesso!');
          } else {
            mediaLabel.textContent = '❌ Erro ao carregar. Tente novamente.';
            mediaArea.style.borderColor = '#ea0038';
            showToast('Erro: ' + (d.error || 'falha no upload'));
          }
        } catch (e) {
          mediaLabel.textContent = '❌ Erro de rede. Tente novamente.';
          mediaArea.style.borderColor = '#ea0038';
          showToast('Erro ao enviar arquivo!');
        }
      });
    }

    const btnRemoveMedia = document.getElementById('btn-remove-media');
    if (btnRemoveMedia) {
      btnRemoveMedia.addEventListener('click', async (e) => {
        e.stopPropagation();
        try {
          const res = await fetch('/api/autoresp/media', { method: 'DELETE' });
          const d = await res.json();
          if (d.success) {
            document.getElementById('media-status-bar').style.display = 'none';
            mediaArea.style.borderColor = 'var(--wa-border)';
            mediaLabel.textContent = '📂 Clique para selecionar qualquer arquivo (máx 100MB)';
            if (mediaInput) mediaInput.value = '';
            showToast('🗑️ Anexo removido!');
          }
        } catch (e) { showToast('Erro ao remover anexo'); }
      });
    }

    const btnSaveAutoResp = document.getElementById('btn-save-autoresp');
    if (btnSaveAutoResp) {
      btnSaveAutoResp.addEventListener('click', async () => {
        const mode = document.getElementById('autoresp-mode').value;
        const msg = document.getElementById('autoresp-msg').value;
        try {
          const res = await fetch('/api/trigger', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: 'setAutoResp', mode, msg })
          });
          const d = await res.json();
          if (d.success) {
            userEditingSet.delete('autoresp-msg');
            userEditingSet.delete('autoresp-mode');
            showToast('🤖 Auto-Responder atualizado com sucesso!');
          }
        } catch (e) {
          showToast('Erro ao salvar Auto-Responder!');
        }
      });
    }

    document.getElementById('search-table').addEventListener('input', loadTable);

    // Initial load & Intervals
    updateStatus();
    loadTable();
    setInterval(updateStatus, 3000);
    setInterval(loadTable, 10000);
  </script>
</body>
</html>
    `);
  } catch (serverErr) {
    console.error('❌ Erro no processamento de requisição HTTP:', serverErr);
    try {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: false, error: 'Erro interno no servidor: ' + serverErr.message }));
    } catch (e) {}
  }
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
      latestQR = null;
      const statusCode = (lastDisconnect?.error)?.output?.statusCode;
      const isLoggedOut = statusCode === DisconnectReason.loggedOut;

      console.log(
        '⚠️ Conexão fechada. Motivo:',
        lastDisconnect?.error?.message || 'Desconhecido',
        `[StatusCode: ${statusCode}]`
      );

      if (isLoggedOut || statusCode === 401 || statusCode === 403) {
        console.log('🗑️ Sessão encerrada/desconectada pelo WhatsApp. Limpando credenciais antigas para gerar novo QR Code...');
        try {
          if (fs.existsSync(authPath)) {
            fs.rmSync(authPath, { recursive: true, force: true });
          }
        } catch (e) {
          console.error('Erro ao limpar pasta auth_info:', e.message);
        }
        setTimeout(startBot, 2000);
      } else {
        console.log('🔄 Tentando reconectar bot em 4 segundos...');
        setTimeout(startBot, 4000);
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

        // =========================================================
        // 🤖 AUTO-RESPONDER INTELIGENTE (PV / GP / ALL)
        // Responde mensagens, marca mensagem enviada, usa menção invisível em grupos
        // Cooldown de 1 hora por JID para evitar spam e ban do WhatsApp
        // =========================================================
        if (autoRespMode !== 'off' && !trimmedText.startsWith('!')) {
          const isGroup = fromJid.endsWith('@g.us');
          let responder = (autoRespMode === 'all') ||
                          (autoRespMode === 'gp' && isGroup) ||
                          (autoRespMode === 'pv' && !isGroup);

          if (responder) {
            const ultimoEnvio = autoRespCooldowns.get(fromJid) || 0;
            if (Date.now() - ultimoEnvio >= AUTO_RESP_COOLDOWN_MS) {
              autoRespCooldowns.set(fromJid, Date.now());
              autoRespQueue.push({ sock, fromJid, isGroup, msgObj: msg });
              setImmediate(processAutoRespQueue);
            }
          }
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

🤖 *Auto-Responder Inteligente:*
• \`!autoresp off\` : Desativar auto-resposta.
• \`!autoresp pv <msg>\` : Responder automaticamente PVs.
• \`!autoresp gp <msg>\` : Responder automaticamente em Grupos.
• \`!autoresp all <msg>\` : Responder PV + Grupos.
_Status atual:_ *${autoRespMode.toUpperCase()}*

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
            queueManager.clearAll();

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
        // 3.2 COMANDO: !divulgar / !broadcast <mensagem | mídia+legenda | resposta citando mídia>
        // Dispara para TODOS os grupos com chat aberto + menção invisível
        // Suporta: Texto, Imagem, Vídeo, Áudio, Documento/PDF ou citar qualquer mídia
        // ---------------------------------------------------------
        if (lowerText.startsWith('!divulgar') || lowerText.startsWith('!broadcast')) {
          let mediaMsg = null;
          let mediaType = null; // 'image' | 'video' | 'audio' | 'document'
          let targetMsgForDl = msg;
          let mimeType = '';
          let fileName = 'arquivo';

          // 1. Detectar mídia direta anexada na mensagem
          if (msg.message?.imageMessage) {
            mediaMsg = msg.message.imageMessage;
            mediaType = 'image';
            mimeType = mediaMsg.mimetype || 'image/jpeg';
          } else if (msg.message?.videoMessage) {
            mediaMsg = msg.message.videoMessage;
            mediaType = 'video';
            mimeType = mediaMsg.mimetype || 'video/mp4';
          } else if (msg.message?.audioMessage) {
            mediaMsg = msg.message.audioMessage;
            mediaType = 'audio';
            mimeType = mediaMsg.mimetype || 'audio/mp4';
          } else if (msg.message?.documentMessage) {
            mediaMsg = msg.message.documentMessage;
            mediaType = 'document';
            mimeType = mediaMsg.mimetype || 'application/pdf';
            fileName = mediaMsg.fileName || 'documento.pdf';
          } else if (msg.message?.documentWithCaptionMessage?.message?.documentMessage) {
            mediaMsg = msg.message.documentWithCaptionMessage.message.documentMessage;
            mediaType = 'document';
            mimeType = mediaMsg.mimetype || 'application/pdf';
            fileName = mediaMsg.fileName || 'documento.pdf';
          } else if (msg.message?.viewOnceMessage?.message?.imageMessage) {
            mediaMsg = msg.message.viewOnceMessage.message.imageMessage;
            mediaType = 'image';
            mimeType = mediaMsg.mimetype || 'image/jpeg';
          } else if (msg.message?.viewOnceMessage?.message?.videoMessage) {
            mediaMsg = msg.message.viewOnceMessage.message.videoMessage;
            mediaType = 'video';
            mimeType = mediaMsg.mimetype || 'video/mp4';
          } else if (msg.message?.viewOnceMessageV2?.message?.imageMessage) {
            mediaMsg = msg.message.viewOnceMessageV2.message.imageMessage;
            mediaType = 'image';
            mimeType = mediaMsg.mimetype || 'image/jpeg';
          } else if (msg.message?.viewOnceMessageV2?.message?.videoMessage) {
            mediaMsg = msg.message.viewOnceMessageV2.message.videoMessage;
            mediaType = 'video';
            mimeType = mediaMsg.mimetype || 'video/mp4';
          }

          // 2. Detectar mídia em mensagem citada (Reply)
          const quoted = msg.message?.extendedTextMessage?.contextInfo?.quotedMessage;
          if (!mediaType && quoted) {
            if (quoted.imageMessage) {
              mediaMsg = quoted.imageMessage;
              mediaType = 'image';
              mimeType = mediaMsg.mimetype || 'image/jpeg';
              targetMsgForDl = { message: { imageMessage: quoted.imageMessage } };
            } else if (quoted.videoMessage) {
              mediaMsg = quoted.videoMessage;
              mediaType = 'video';
              mimeType = mediaMsg.mimetype || 'video/mp4';
              targetMsgForDl = { message: { videoMessage: quoted.videoMessage } };
            } else if (quoted.audioMessage) {
              mediaMsg = quoted.audioMessage;
              mediaType = 'audio';
              mimeType = mediaMsg.mimetype || 'audio/mp4';
              targetMsgForDl = { message: { audioMessage: quoted.audioMessage } };
            } else if (quoted.documentMessage) {
              mediaMsg = quoted.documentMessage;
              mediaType = 'document';
              mimeType = mediaMsg.mimetype || 'application/pdf';
              fileName = mediaMsg.fileName || 'documento.pdf';
              targetMsgForDl = { message: { documentMessage: quoted.documentMessage } };
            } else if (quoted.documentWithCaptionMessage?.message?.documentMessage) {
              mediaMsg = quoted.documentWithCaptionMessage.message.documentMessage;
              mediaType = 'document';
              mimeType = mediaMsg.mimetype || 'application/pdf';
              fileName = mediaMsg.fileName || 'documento.pdf';
              targetMsgForDl = { message: { documentMessage: quoted.documentWithCaptionMessage.message.documentMessage } };
            } else if (quoted.viewOnceMessage?.message?.imageMessage) {
              mediaMsg = quoted.viewOnceMessage.message.imageMessage;
              mediaType = 'image';
              mimeType = mediaMsg.mimetype || 'image/jpeg';
              targetMsgForDl = { message: { imageMessage: quoted.viewOnceMessage.message.imageMessage } };
            } else if (quoted.viewOnceMessage?.message?.videoMessage) {
              mediaMsg = quoted.viewOnceMessage.message.videoMessage;
              mediaType = 'video';
              mimeType = mediaMsg.mimetype || 'video/mp4';
              targetMsgForDl = { message: { videoMessage: quoted.viewOnceMessage.message.videoMessage } };
            }
          }

          // 3. Extrair texto / legenda
          const msgTexto = trimmedText.replace(/^!(divulgar|broadcast)\s*/i, '').trim();

          // Sem conteúdo (nem mídia nem texto)
          if (!msgTexto && !mediaType) {
            await sock.sendMessage(fromJid, {
              text: `⚠️ *Uso do Comando de Divulgação:*\n\n` +
                    `📝 *Texto:* \`!divulgar Minha mensagem aqui\`\n` +
                    `🖼️ *Imagem:* Envie imagem com legenda \`!divulgar Minha legenda\`\n` +
                    `📹 *Vídeo:* Envie vídeo com legenda \`!divulgar Minha legenda\`\n` +
                    `🎵 *Áudio:* Envie ou responda a um áudio com \`!divulgar\`\n` +
                    `📄 *Documento/PDF:* Envie documento com legenda \`!divulgar Minha legenda\`\n` +
                    `↩️ *Resposta:* Responda a qualquer mensagem/mídia com \`!divulgar <legenda>\`\n\n` +
                    `🚀 Dispara para **todos os grupos com chat aberto**, com **menção invisível**!`
            });
            continue;
          }

          // 4. Baixar mídia se detectada
          let mediaBuffer = null;
          if (mediaType && targetMsgForDl) {
            try {
              mediaBuffer = await downloadMediaMessage(
                targetMsgForDl,
                'buffer',
                {},
                { logger: pino({ level: 'silent' }), reconnect: sock.type }
              );
              console.log(`📎 [Divulgar] Mídia (${mediaType}) baixada com sucesso: ${mediaBuffer.length} bytes`);
            } catch (dlErr) {
              console.error(`❌ Erro ao baixar mídia para divulgação:`, dlErr.message);
              await sock.sendMessage(fromJid, {
                text: `❌ Falha ao baixar o anexo de mídia. Tente reenviar ou use a Dashboard Web.`
              });
              continue;
            }
          }

          // 5. Executa a divulgação com menção invisível
          await executeBroadcast({
            sock,
            text: msgTexto,
            mediaBuffer,
            mediaType,
            mimeType,
            fileName,
            fromJid
          });
          continue;
        }


        // ---------------------------------------------------------
        // 3.3 COMANDO: !autoresp <modo> [mensagem] (Auto-Responder Inteligente)
        // Modos: off | pv | gp | all
        // Exemplo: !autoresp gp Olá, veja nosso catálogo!
        // ---------------------------------------------------------
        if (lowerText.startsWith('!autoresp')) {
          const parts = trimmedText.slice('!autoresp'.length).trim().split(/\s+/);
          const modo = (parts[0] || '').toLowerCase();
          const mensagem = parts.slice(1).join(' ').trim();

          if (!['off', 'pv', 'gp', 'all'].includes(modo)) {
            await sock.sendMessage(fromJid, {
              text: `🤖 *Auto-Responder - Uso:*\n\n• \`!autoresp off\` — Desativar\n• \`!autoresp pv Sua mensagem\` — Responder PVs\n• \`!autoresp gp Sua mensagem\` — Responder Grupos\n• \`!autoresp all Sua mensagem\` — Responder tudo\n\n_Status atual:_ *${autoRespMode.toUpperCase()}*\n_Mensagem:_ ${autoRespMsg}`
            });
            continue;
          }

          autoRespMode = modo;
          if (mensagem) autoRespMsg = mensagem;

          const modoEmoji = { off: '⏹️', pv: '💬', gp: '👥', all: '🌐' }[modo];
          const modoNome = { off: 'DESATIVADO', pv: 'Somente PV', gp: 'Somente Grupos', all: 'PV + Grupos' }[modo];

          await sock.sendMessage(fromJid, {
            text: `🤖 *Auto-Responder Atualizado!*\n\n${modoEmoji} Modo: *${modoNome}*\n📝 Mensagem: _${autoRespMsg}_\n⏱️ Cooldown: 1 hora por conversa\n\n_Variação de emojis e delay anti-ban ativados!_`
          });
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
  abortProcessing = false;
  console.log(`\n⚡ Processando ${pendingLinks.length} links pendentes (${isUsingLocalFallback ? 'Fila Local' : 'PostgreSQL'})...`);

  const batchResults = [];
  let limitReached = false;
  let scheduledNextRun = null;

  for (let i = 0; i < pendingLinks.length; i++) {
    if (abortProcessing) {
      console.log('🛑 [Processamento] Cancelado pelo usuário.');
      isProcessing = false;
      break;
    }

    const item = pendingLinks[i];

    if (i > 0) {
      const delayMs = getRandomDelay(config.minDelaySeconds, config.maxDelaySeconds);
      console.log(`⏱️ Delay (${(delayMs / 1000).toFixed(1)}s)...`);
      await sleep(delayMs);
    }

    if (abortProcessing) {
      console.log('🛑 [Processamento] Cancelado durante o delay pelo usuário.');
      isProcessing = false;
      break;
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

// Tratamento global de erros para manter a aplicação online 24/7 no Fly.io / Render
process.on('uncaughtException', (err) => {
  console.error('⚠️ [Process] Exceção não capturada interceptada (prevenindo crash):', err);
});

process.on('unhandledRejection', (reason) => {
  console.error('⚠️ [Process] Rejeição de Promise não tratada interceptada:', reason);
});

// Iniciar o bot
startBot().catch((err) => {
  console.error('Erro fatal na inicialização do bot:', err);
});
