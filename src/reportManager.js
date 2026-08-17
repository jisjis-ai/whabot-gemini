import { formatDate } from './utils.js';

/**
 * Gera um relatório formatado em Markdown para o WhatsApp
 * @param {Object} options
 * @param {Array} options.results - Resultados do lote atual [{code, url, status, reason, groupName}]
 * @param {number} options.totalPending - Total de grupos ainda na fila
 * @param {string|null} options.nextRunTime - Data/hora ISO do próximo lote agendado
 * @param {boolean} options.limitReached - Se o lote parou por limite ou bloqueio
 * @returns {string}
 */
export function generateReport({ results = [], totalPending = 0, nextRunTime = null, limitReached = false }) {
  const successes = results.filter((r) => r.status === 'success');
  const failures = results.filter((r) => r.status === 'failed');

  let report = `📊 *RELATÓRIO DE ENTRADA EM GRUPOS*\n`;
  report += `📅 *Data:* ${formatDate(new Date())}\n\n`;

  report += `📈 *Resumo do Lote:*\n`;
  report += `• 📥 Links processados: *${results.length}*\n`;
  report += `• ✅ Entradas com sucesso: *${successes.length}*\n`;
  report += `• ❌ Falhas / Impedimentos: *${failures.length}*\n`;
  report += `• ⏳ Restantes na fila: *${totalPending}*\n\n`;

  if (results.length > 0) {
    report += `📝 *Detalhamento:*\n`;
    results.forEach((item, idx) => {
      const num = idx + 1;
      if (item.status === 'success') {
        const title = item.groupName ? `*${item.groupName}*` : 'Grupo sem nome';
        report += `${num}. ✅ ${title}\n   🔗 ${item.url}\n`;
      } else {
        report += `${num}. ❌ Motivo: _${item.reason || 'Desconhecido'}_\n   🔗 ${item.url}\n`;
      }
    });
    report += `\n`;
  }

  if (limitReached && nextRunTime) {
    report += `🛑 *Limite de Segurança Anti-Ban Atingido*\n`;
    report += `⏰ O restante da fila (*${totalPending}* grupos) será reprocessado automaticamente em:\n`;
    report += `👉 *${formatDate(nextRunTime)}*\n\n`;
  } else if (totalPending > 0) {
    report += `ℹ️ *Ainda há ${totalPending} grupos aguardando processamento.*\n\n`;
  } else {
    report += `🎉 *Todos os grupos solicitados foram processados!*\n\n`;
  }

  report += `_Bot operando em modo silencioso (Apenas entradas e relatórios)._`;

  return report;
}
