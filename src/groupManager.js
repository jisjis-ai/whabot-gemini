/**
 * Tenta entrar em um grupo do WhatsApp pelo código de convite
 * @param {import('@whiskeysockets/baileys').WASocket} sock 
 * @param {string} code - Código de convite extraído do link
 * @returns {Promise<{success: boolean, groupJid?: string, groupName?: string, reason?: string, isRateLimited?: boolean}>}
 */
export async function joinGroup(sock, code) {
  let groupName = '';

  try {
    const responseJid = await sock.groupAcceptInvite(code);
    let jid = responseJid || '';

    // Se tiver o JID do grupo recém-entrado, busca o nome do grupo via metadata
    if (jid) {
      try {
        const metadata = await sock.groupMetadata(jid);
        if (metadata && metadata.subject) {
          groupName = metadata.subject;
        }
      } catch (e) {}
    }

    return {
      success: true,
      groupJid: jid,
      groupName: groupName || 'Grupo de WhatsApp',
      reason: ''
    };
  } catch (error) {
    const errorStr = String(error?.message || error || '').toLowerCase();
    const statusCode = error?.output?.statusCode || error?.status || error?.data;

    let reason = 'Erro desconhecido ao entrar';
    let isRateLimited = false;

    if (statusCode === 401 || statusCode === 404 || errorStr.includes('not-authorized') || errorStr.includes('invalid')) {
      reason = 'Convite inválido, expirado ou revogado';
    } else if (statusCode === 409 || errorStr.includes('conflict') || errorStr.includes('already')) {
      return {
        success: true, // Considera sucesso pois o objetivo de estar no grupo foi atingido
        groupName: 'Grupo (Já participante)',
        reason: 'Já era participante do grupo'
      };
    } else if (statusCode === 403 || errorStr.includes('forbidden')) {
      reason = 'Entrada restrita por administradores';
    } else if (statusCode === 429 || statusCode === 463 || errorStr.includes('rate') || errorStr.includes('overload') || errorStr.includes('too many')) {
      reason = 'Limite de solicitações do WhatsApp atingido (Rate Limit)';
      isRateLimited = true;
    } else if (errorStr.includes('full') || errorStr.includes('cap')) {
      reason = 'Grupo lotado (capacidade máxima atingida)';
    } else {
      reason = `Erro (${statusCode || 'desconhecido'}): ${error?.message || errorStr}`;
    }

    return {
      success: false,
      groupName: groupName,
      reason: reason,
      isRateLimited: isRateLimited
    };
  }
}
