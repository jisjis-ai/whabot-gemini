/**
 * Tenta entrar em um grupo do WhatsApp pelo código de convite
 * @param {import('@whiskeysockets/baileys').WASocket} sock 
 * @param {string} code - Código de convite extraído do link
 * @returns {Promise<{success: boolean, groupJid?: string, groupName?: string, reason?: string, isRateLimited?: boolean}>}
 */
export async function joinGroup(sock, code) {
  let groupName = '';

  // Tenta primeiramente obter as informações do convite para pegar o nome do grupo
  try {
    const inviteInfo = await sock.groupGetInviteInfo(code);
    if (inviteInfo && inviteInfo.subject) {
      groupName = inviteInfo.subject;
    }
  } catch (err) {
    // Se falhar ao pegar info, não impede a tentativa de aceitar o convite
  }

  try {
    const responseJid = await sock.groupAcceptInvite(code);
    return {
      success: true,
      groupJid: responseJid || '',
      groupName: groupName || 'Grupo de WhatsApp',
      reason: ''
    };
  } catch (error) {
    const errorStr = String(error?.message || error || '').toLowerCase();
    const statusCode = error?.output?.statusCode || error?.status || error?.data;

    let reason = 'Erro desconhecido ao entrar';
    let isRateLimited = false;

    if (statusCode === 401 || statusCode === 404 || errorStr.includes('not-authorized') || errorStr.includes('invalid')) {
      reason = 'Convite inválido ou revogado';
    } else if (statusCode === 409 || errorStr.includes('conflict') || errorStr.includes('already')) {
      reason = 'Você já é participante deste grupo';
      return {
        success: true, // Considera sucesso pois o objetivo de estar no grupo foi atingido
        groupName: groupName || 'Grupo (Já participante)',
        reason: 'Já era participante do grupo'
      };
    } else if (statusCode === 403 || errorStr.includes('forbidden')) {
      reason = 'Entrada proibida ou restrita pelos administradores';
    } else if (statusCode === 429 || statusCode === 463 || errorStr.includes('rate') || errorStr.includes('overload') || errorStr.includes('too many')) {
      reason = 'Limite de solicitações do WhatsApp atingido (Rate Limit)';
      isRateLimited = true;
    } else if (errorStr.includes('full') || errorStr.includes('cap')) {
      reason = 'O grupo atingiu a capacidade máxima de membros';
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
