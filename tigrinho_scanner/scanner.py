import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Any
import psycopg2
from psycopg2.extensions import connection

from tigrinho_scanner.database import DatabaseManager
from tigrinho_scanner.rpc_client import RPCClient
from tigrinho_scanner.address import derive_address

MAX_WORKERS = 20  # Aumentado para PostgreSQL
RETRY_LIMIT = 3
BLOCK_BATCH_SIZE = 20

class BitcoinScanner:
    def __init__(self, network: str, rpc_client: RPCClient, db_manager: DatabaseManager):
        self.network = network
        self.rpc = rpc_client
        self.db = db_manager
        self.current_chain_height = 0

        # Configurações de rede para endereços
        if self.network == 'testnet4':
            self.bech32_hrp = 'tb'
            self.p2pkh_prefix = b'\x6f'
            self.p2sh_prefix = b'\xc4'
        else:
            self.bech32_hrp = 'bc'
            self.p2pkh_prefix = b'\x00'
            self.p2sh_prefix = b'\x05'

    def _process_transaction(self, tx: dict, conn: connection):
        """Processa uma transação usando uma conexão existente"""
        with conn.cursor() as cur:
            # Processar outputs
            for vout in tx['vout']:
                script_hex = vout['scriptPubKey']['hex']
                address = derive_address(script_hex, self.network)
                if not address:
                    continue

                value = int(vout['value'] * 100_000_000)
                
                # Inserir UTXO
                cur.execute("""
                    INSERT INTO utxos (txid, vout, address, value)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (txid, vout) DO NOTHING
                """, (tx['txid'], vout['n'], address, value))
                
                # Atualizar saldo
                cur.execute("""
                    INSERT INTO balances (address, balance)
                    VALUES (%s, %s)
                    ON CONFLICT (address) DO UPDATE SET
                    balance = balances.balance + EXCLUDED.balance
                """, (address, value))

            # Processar inputs
            for vin in tx.get('vin', []):
                if 'txid' not in vin:
                    continue

                txid = vin['txid']
                vout = vin['vout']
                
                # Buscar UTXO
                cur.execute("""
                    SELECT address, value FROM utxos
                    WHERE txid = %s AND vout = %s
                    FOR UPDATE
                """, (txid, vout))
                
                if (row := cur.fetchone()):
                    address, value = row
                    
                    # Remover UTXO
                    cur.execute("""
                        DELETE FROM utxos
                        WHERE txid = %s AND vout = %s
                    """, (txid, vout))
                    
                    # Atualizar saldo
                    cur.execute("""
                        UPDATE balances
                        SET balance = balance - %s
                        WHERE address = %s
                    """, (value, address))

    def process_block(self, block_height: int) -> bool:
        """Processa um bloco de forma atômica"""
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cur:
                # Iniciar transação
                cur.execute("BEGIN")

                # Verificar se já foi processado
                cur.execute("""
                    SELECT 1 FROM processed_blocks
                    WHERE height = %s AND status = 'processed'
                    FOR UPDATE
                """, (block_height,))
                
                if cur.fetchone():
                    return True

                # Obter dados via RPC
                block_hash = self.rpc.call('getblockhash', [block_height])
                if not block_hash:
                    raise ValueError("Bloco não encontrado")

                block = self.rpc.call('getblock', [block_hash, 2])
                if not block or 'tx' not in block:
                    raise ValueError("Dados do bloco inválidos")

                # Processar transações
                for tx in block['tx']:
                    self._process_transaction(tx, conn)

                # Atualizar último bloco processado
                cur.execute("SELECT value FROM metadata WHERE key = 'last_block'")
                current_last = int(cur.fetchone()[0])
                
                if block_height == current_last + 1:
                    new_last = block_height
                    while True:
                        next_block = new_last + 1
                        cur.execute("""
                            SELECT 1 FROM processed_blocks
                            WHERE height = %s AND status = 'processed'
                        """, (next_block,))
                        if not cur.fetchone():
                            break
                        new_last = next_block
                    
                    cur.execute("""
                        UPDATE metadata
                        SET value = %s
                        WHERE key = 'last_block'
                    """, (str(new_last),))

                # Atualizar status do bloco
                cur.execute("""
                    INSERT INTO processed_blocks (height, status, retries)
                    VALUES (%s, 'processed', 0)
                    ON CONFLICT (height) DO UPDATE SET
                        status = EXCLUDED.status,
                        retries = EXCLUDED.retries
                """, (block_height,))

                conn.commit()
                return True

        except Exception as e:
            conn.rollback()
            logging.error(f"Erro no bloco {block_height}: {str(e)}")
            
            # Registrar falha em nova conexão
            try:
                cleanup_conn = self.db.get_connection()
                with cleanup_conn.cursor() as cleanup_cur:
                    cleanup_cur.execute("""
                        INSERT INTO processed_blocks (height, status, retries)
                        VALUES (%s, 'failed', 
                            COALESCE((SELECT retries FROM processed_blocks WHERE height = %s), 0) + 1)
                        ON CONFLICT (height) DO UPDATE SET
                            status = EXCLUDED.status,
                            retries = EXCLUDED.retries
                    """, (block_height, block_height))
                    cleanup_conn.commit()
                self.db.return_connection(cleanup_conn)
            except Exception as cleanup_error:
                logging.error(f"Erro no cleanup: {cleanup_error}")
            
            return False
        finally:
            self.db.return_connection(conn)

    def scan_blockchain(self, end_height: int):
        """Executa a varredura completa da blockchain"""
        self.current_chain_height = end_height
        logging.info(f"Iniciando varredura até o bloco {end_height}")

        total_processed = 0
        last_log_time = time.time()

        while True:
            next_blocks = self.get_next_blocks()
            
            if not next_blocks:
                if self.get_last_processed_block() >= end_height:
                    logging.info("Varredura concluída!")
                    break
                time.sleep(5)
                continue

            logging.info(f"Processando lote: {min(next_blocks)}-{max(next_blocks)}")

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(self.process_block, height): height
                    for height in next_blocks
                }

                for future in as_completed(futures):
                    height = futures[future]
                    try:
                        if future.result():
                            total_processed += 1
                    except Exception as e:
                        logging.error(f"Erro fatal no bloco {height}: {str(e)}")

            if time.time() - last_log_time > 30:
                self.print_progress()
                last_log_time = time.time()

        self.print_progress()

    def get_next_blocks(self) -> List[int]:
        """Obtém o próximo lote de blocos para processamento"""
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("BEGIN")
                
                cur.execute("""
                    WITH candidates AS (
                        SELECT height FROM processed_blocks
                        WHERE status = 'failed' AND retries < %s
                        UNION ALL
                        SELECT COALESCE(MAX(height), -1) + 1 FROM processed_blocks
                        WHERE height NOT IN (
                            SELECT height FROM processed_blocks
                            WHERE status = 'failed' AND retries < %s
                        )
                        UNION ALL
                        SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM processed_blocks)
                    )
                    SELECT height FROM candidates
                    WHERE height <= %s
                    ORDER BY height
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                """, (RETRY_LIMIT, RETRY_LIMIT, self.current_chain_height, BLOCK_BATCH_SIZE))
                
                next_blocks = [row[0] for row in cur.fetchall() if row[0] is not None]

                # Marcar blocos como pending
                for height in next_blocks:
                    cur.execute("""
                        INSERT INTO processed_blocks (height, status, retries)
                        VALUES (%s, 'pending', 
                            COALESCE((SELECT retries FROM processed_blocks WHERE height = %s), 0) + 1)
                        ON CONFLICT (height) DO UPDATE SET
                            status = EXCLUDED.status,
                            retries = EXCLUDED.retries
                    """, (height, height))

                conn.commit()
                return next_blocks

        except Exception as e:
            logging.error(f"Erro ao buscar blocos: {str(e)}")
            return []
        finally:
            self.db.return_connection(conn)

    def get_last_processed_block(self) -> int:
        """Retorna a altura do último bloco confirmado"""
        try:
            result = self.db.fetch_one("SELECT value FROM metadata WHERE key = 'last_block'")
            return int(result['value']) if result else -1
        except Exception as e:
            logging.error(f"Erro ao obter último bloco: {e}")
            return -1

    def print_progress(self):
        """Exibe o progresso atual da varredura"""
        try:
            stats = self.db.fetch_one("""
                SELECT 
                    SUM(CASE WHEN status = 'processed' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END)
                FROM processed_blocks
            """)
            
            if stats:
                processed = stats[0] or 0
                failed = stats[1] or 0
                pending = stats[2] or 0
                
                logging.info(
                    f"Progresso: ✅ {processed} | ❌ {failed} | ⏳ {pending} | "
                    f"Último confirmado: {self.get_last_processed_block()}"
                )
        except Exception as e:
            logging.error(f"Erro ao exibir progresso: {e}")

    def get_balances(self, min_balance: float = 0.0) -> Dict[str, float]:
        """Retorna saldos consolidados"""
        try:
            rows = self.db.fetch_all(
                "SELECT address, balance FROM balances WHERE balance >= %s",
                (int(min_balance * 100_000_000),)
            )
            return {row['address']: row['balance']/100_000_000 for row in rows}
        except Exception as e:
            logging.error(f"Erro ao obter saldos: {e}")
            return {}