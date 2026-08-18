import pg from 'pg';

const { Pool } = pg;

const connectionString =
  process.env.DATABASE_URL ||
  'postgresql://rdb_11ww_user:XZwi9aNOlPCiwyV3HuYizJ7RBsnDFALe@dpg-da1umt7lk1mc73ad5nlg-a.oregon-postgres.render.com/rdb_11ww';

export const pool = new Pool({
  connectionString,
  ssl: {
    rejectUnauthorized: false
  },
  max: 10, // Máximo de conexões no pool
  idleTimeoutMillis: 30000, // Fecha conexões ociosas após 30 segundos
  connectionTimeoutMillis: 10000 // Timeout de 10 segundos ao tentar conectar
});

// Trata erros de conexões ociosas caindo no banco do Render para não derrubar a aplicação
pool.on('error', (err) => {
  console.error('⚠️ [PostgreSQL Pool Warning] Conexão ociosa descartada pelo banco:', err.message);
});

/**
 * Função utilitária para executar consultas com auto-retry caso a conexão caia
 */
async function executeWithRetry(fn, retries = 3) {
  let attempt = 0;
  while (attempt < retries) {
    attempt++;
    try {
      return await fn();
    } catch (err) {
      const isConnError =
        err.message.includes('terminated unexpectedly') ||
        err.message.includes('closed') ||
        err.message.includes('timeout') ||
        err.code === '57P01';

      if (isConnError && attempt < retries) {
        console.warn(`⚠️ Conexão PostgreSQL oscilou (tentativa ${attempt}/${retries}). Reconectando em 1s...`);
        await new Promise((r) => setTimeout(r, 1000));
      } else {
        throw err;
      }
    }
  }
}

/**
 * Inicializa a tabela de links no PostgreSQL se não existir
 */
export async function initDb() {
  const query = `
    CREATE TABLE IF NOT EXISTS group_links (
      id SERIAL PRIMARY KEY,
      code VARCHAR(64) UNIQUE NOT NULL,
      url TEXT NOT NULL,
      status VARCHAR(32) NOT NULL DEFAULT 'pending',
      group_name TEXT DEFAULT '',
      reason TEXT DEFAULT '',
      created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
  `;
  try {
    await executeWithRetry(async () => {
      const client = await pool.connect();
      try {
        await client.query(query);
      } finally {
        client.release();
      }
    });
    console.log('🐘 PostgreSQL conectado e tabela group_links pronta!');
  } catch (err) {
    console.error('❌ Erro ao conectar/inicializar banco PostgreSQL:', err.message);
  }
}

/**
 * Adiciona novos convites ao PostgreSQL (ignorando códigos já existentes)
 * @param {Array<{code: string, url: string}>} items 
 * @returns {Promise<{addedCount: number, totalPending: number}>}
 */
export async function addLinks(items) {
  if (!items || items.length === 0) {
    const totalPending = await getPendingCount();
    return { addedCount: 0, totalPending };
  }

  let addedCount = 0;

  try {
    await executeWithRetry(async () => {
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        for (const item of items) {
          const res = await client.query(
            `INSERT INTO group_links (code, url, status) 
             VALUES ($1, $2, 'pending') 
             ON CONFLICT (code) DO NOTHING;`,
            [item.code, item.url]
          );
          if (res.rowCount > 0) {
            addedCount++;
          }
        }
        await client.query('COMMIT');
      } catch (err) {
        await client.query('ROLLBACK');
        throw err;
      } finally {
        client.release();
      }
    });
  } catch (err) {
    console.error('Erro ao adicionar links no PostgreSQL:', err.message);
  }

  const totalPending = await getPendingCount();
  return { addedCount, totalPending };
}

/**
 * Retorna todos os links com status 'pending' no banco de dados
 * @returns {Promise<Array<{id: number, code: string, url: string}>>}
 */
export async function getPendingLinks() {
  try {
    return await executeWithRetry(async () => {
      const res = await pool.query(
        `SELECT id, code, url FROM group_links WHERE status = 'pending' ORDER BY id ASC;`
      );
      return res.rows;
    });
  } catch (err) {
    console.error('Erro ao buscar links pendentes no PostgreSQL:', err.message);
    return [];
  }
}

/**
 * Retorna o total de links com status 'pending'
 * @returns {Promise<number>}
 */
export async function getPendingCount() {
  try {
    return await executeWithRetry(async () => {
      const res = await pool.query(`SELECT COUNT(*)::int as count FROM group_links WHERE status = 'pending';`);
      return res.rows[0]?.count || 0;
    });
  } catch (err) {
    return 0;
  }
}

/**
 * Atualiza o status e informações de um link após a tentativa de entrada
 * @param {string} code 
 * @param {string} status - 'success' | 'failed' | 'rate_limited'
 * @param {string} groupName 
 * @param {string} reason 
 */
export async function updateLinkStatus(code, status, groupName = '', reason = '') {
  try {
    await executeWithRetry(async () => {
      await pool.query(
        `UPDATE group_links 
         SET status = $1, group_name = $2, reason = $3, updated_at = NOW() 
         WHERE code = $4;`,
        [status, groupName, reason, code]
      );
    });
  } catch (err) {
    console.error(`Erro ao atualizar status do código ${code}:`, err.message);
  }
}

/**
 * Retorna o resumo estatístico de todos os links no banco
 */
export async function getStats() {
  try {
    return await executeWithRetry(async () => {
      const res = await pool.query(`
        SELECT 
          COUNT(*)::int as total,
          COUNT(*) FILTER (WHERE status = 'pending')::int as pending,
          COUNT(*) FILTER (WHERE status = 'success')::int as success,
          COUNT(*) FILTER (WHERE status = 'failed')::int as failed,
          COUNT(*) FILTER (WHERE status = 'rate_limited')::int as rate_limited
        FROM group_links;
      `);
      return res.rows[0];
    });
  } catch (err) {
    return { total: 0, pending: 0, success: 0, failed: 0, rate_limited: 0 };
  }
}

/**
 * Remove todos os links do banco de dados
 * @returns {Promise<number>} Quantidade de registros deletados
 */
export async function deleteAllLinks() {
  try {
    return await executeWithRetry(async () => {
      const res = await pool.query(`DELETE FROM group_links;`);
      console.log(`🗑️ ${res.rowCount} links excluídos do PostgreSQL.`);
      return res.rowCount;
    });
  } catch (err) {
    console.error('Erro ao deletar links do PostgreSQL:', err.message);
    return 0;
  }
}

/**
 * Retorna todos os links cadastrados (para GET /api/links)
 */
export async function getAllLinks(statusFilter = null) {
  try {
    return await executeWithRetry(async () => {
      if (statusFilter) {
        const res = await pool.query(
          `SELECT id, code, url, status, group_name, reason, created_at, updated_at 
           FROM group_links WHERE status = $1 ORDER BY id DESC;`,
          [statusFilter]
        );
        return res.rows;
      }
      const res = await pool.query(
        `SELECT id, code, url, status, group_name, reason, created_at, updated_at 
         FROM group_links ORDER BY id DESC LIMIT 500;`
      );
      return res.rows;
    });
  } catch (err) {
    console.error('Erro ao listar links:', err.message);
    return [];
  }
}
