import fs from 'fs';
import path from 'path';

const DATA_DIR = process.env.DATA_DIR ? path.resolve(process.env.DATA_DIR) : process.cwd();
const QUEUE_FILE = path.resolve(DATA_DIR, 'queue.json');

const defaultState = {
  pending: [],
  history: [],
  nextScheduledRun: null,
  activeBatchCount: 0
};

export class QueueManager {
  constructor() {
    this.ensureDataDir();
    this.data = this.loadQueue();
  }

  ensureDataDir() {
    try {
      if (!fs.existsSync(DATA_DIR)) {
        fs.mkdirSync(DATA_DIR, { recursive: true });
      }
    } catch (err) {
      console.error('[QueueManager] Erro ao criar DATA_DIR:', err.message);
    }
  }

  loadQueue() {
    try {
      if (fs.existsSync(QUEUE_FILE)) {
        const raw = fs.readFileSync(QUEUE_FILE, 'utf-8');
        return JSON.parse(raw);
      }
    } catch (err) {
      console.error('[QueueManager] Erro ao carregar queue.json:', err.message);
    }
    return { ...defaultState };
  }

  saveQueue() {
    try {
      this.ensureDataDir();
      fs.writeFileSync(QUEUE_FILE, JSON.stringify(this.data, null, 2), 'utf-8');
    } catch (err) {
      console.error('[QueueManager] Erro ao salvar queue.json:', err.message);
    }
  }

  /**
   * Adiciona novos convites à fila pendente (evitando duplicados)
   * @param {Array<{code: string, url: string}>} items 
   * @param {string} targetJid - JID de quem solicitou a entrada
   * @returns {{addedCount: number, totalPending: number}}
   */
  addToQueue(items, targetJid) {
    let addedCount = 0;
    const existingCodes = new Set([
      ...this.data.pending.map((i) => i.code),
      ...this.data.history.map((h) => h.code)
    ]);

    for (const item of items) {
      if (!existingCodes.has(item.code)) {
        existingCodes.add(item.code);
        this.data.pending.push({
          code: item.code,
          url: item.url,
          addedAt: new Date().toISOString(),
          targetJid: targetJid
        });
        addedCount++;
      }
    }

    this.saveQueue();
    return {
      addedCount,
      totalPending: this.data.pending.length
    };
  }

  getPendingItems() {
    return this.data.pending;
  }

  /**
   * Marca um item como processado e move para o histórico
   */
  markProcessed(code, status, reason = '', groupName = '') {
    const index = this.data.pending.findIndex((item) => item.code === code);
    let targetJid = null;

    if (index !== -1) {
      const item = this.data.pending.splice(index, 1)[0];
      targetJid = item.targetJid;
      this.data.history.push({
        code: item.code,
        url: item.url,
        status: status, // 'success' | 'failed'
        reason: reason,
        groupName: groupName,
        processedAt: new Date().toISOString(),
        targetJid: targetJid
      });
    }

    this.saveQueue();
    return targetJid;
  }

  /**
   * Define o momento em que o próximo lote poderá ser executado
   * @param {number} hours - Quantidade de horas para aguardar
   */
  scheduleNextBatch(hours = 6) {
    const nextRun = new Date(Date.now() + hours * 60 * 60 * 1000).toISOString();
    this.data.nextScheduledRun = nextRun;
    this.saveQueue();
    return nextRun;
  }

  clearSchedule() {
    this.data.nextScheduledRun = null;
    this.saveQueue();
  }

  isScheduledWaitActive() {
    if (!this.data.nextScheduledRun) return false;
    const nextRunTime = new Date(this.data.nextScheduledRun).getTime();
    if (Date.now() < nextRunTime) {
      return true;
    }
    // Já passou a hora agendada, limpa a espera
    this.clearSchedule();
    return false;
  }

  getNextScheduledRun() {
    return this.data.nextScheduledRun;
  }

  getHistorySummary() {
    return this.data.history;
  }
}
