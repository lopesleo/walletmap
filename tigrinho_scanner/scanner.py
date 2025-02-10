import logging
import time
from typing import Dict, List
import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection
from tigrinho_scanner.database import DatabaseManager
from tigrinho_scanner.address import derive_address

RETRY_LIMIT = 3
BLOCK_BATCH_SIZE = 100

class BitcoinScanner:
    def __init__(self, network: str, rpc_client, db_manager: DatabaseManager):
        self.network = network
        self.rpc = rpc_client
        self.db = db_manager
        self.current_chain_height = 0

        self._setup_network_params()
        self._verify_database()
        self._create_indexes()

    def _setup_network_params(self):
        """Configura parâmetros de rede para endereços"""
        self.bech32_hrp = 'bc' if self.network == 'mainnet' else 'tb'
        self.p2pkh_prefix = b'\x00' if self.network == 'mainnet' else b'\x6f'
        self.p2sh_prefix = b'\x05' if self.network == 'mainnet' else b'\xc4'

    def _verify_database(self):
        """Verifica a integridade do banco de dados"""
        if not self.db.fetch_one("SELECT 1"):
            raise ConnectionError("Falha na conexão com o banco de dados")

    def _create_indexes(self):
        """Garante a existência de índices essenciais"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS processed_blocks_height_idx ON processed_blocks (height)",
            "CREATE INDEX IF NOT EXISTS balances_address_idx ON balances (address)"
        ]
        for index in indexes:
            self.db.execute(index)

    def _process_transaction(self, tx: dict, conn: connection):
        """Processa uma transação com correção de ordem de operações"""
        try:
            with conn.cursor() as cur:
                # Processar outputs primeiro
                for vout in tx['vout']:
                    script_hex = vout['scriptPubKey']['hex']
                    address = derive_address(script_hex, self.network)
                    if not address:
                        continue

                    value = int(vout['value'] * 100_000_000)
                    
                    # 1. Atualizar saldo PRIMEIRO para garantir a existência do endereço
                    cur.execute("""
                        INSERT INTO balances (address, balance)
                        VALUES (%s, %s)
                        ON CONFLICT (address) DO UPDATE SET
                        balance = balances.balance + EXCLUDED.balance
                    """, (address, value))
                    
                    # 2. Inserir UTXO DEPOIS que o saldo está garantido
                    cur.execute("""
                        INSERT INTO utxos (txid, vout, address, value)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (txid, vout) DO NOTHING
                    """, (tx['txid'], vout['n'], address, value))

                # Processar inputs (mantido igual)
                for vin in tx.get('vin', []):
                    if 'txid' not in vin:
                        continue

                    txid = vin['txid']
                    vout = vin['vout']
                    
                    # Buscar UTXO
                    cur.execute("""
                        SELECT address, value FROM utxos
                        WHERE txid = %s AND vout = %s
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
        except Exception as e:
            logging.error(f"Erro ao processar transação {tx['txid']}: {str(e)}")
            conn.rollback()
            raise

    def process_block(self, block_height: int) -> bool:
        """Processa um bloco de forma atômica"""
        conn = self.db.get_connection()
        try:
            conn.autocommit = False
            
            with conn.cursor() as cur:
                # Verificar se já foi processado
                cur.execute("""
                    SELECT 1 FROM processed_blocks
                    WHERE height = %s AND status = 'processed'
                """, (block_height,))
                
                if cur.fetchone():
                    return True

                # Obter dados do bloco
                block_hash = self.rpc.call('getblockhash', [block_height])
                block = self.rpc.call('getblock', [block_hash, 2])

                if not block or 'tx' not in block:
                    raise ValueError(f"Bloco {block_height} inválido")

                # Processar transações
                for tx in block['tx']:
                    self._process_transaction(tx, conn)

                # Atualizar status
                cur.execute("""
                    INSERT INTO processed_blocks (height, status)
                    VALUES (%s, 'processed')
                    ON CONFLICT (height) DO UPDATE SET
                        status = EXCLUDED.status,
                        retries = 0
                """, (block_height,))

                conn.commit()
                return True

        except Exception as e:
            conn.rollback()
            logging.error(f"Erro no bloco {block_height}: {str(e)}")
            self._mark_block_failed(block_height)
            return False
        finally:
            self.db.return_connection(conn)

    def _mark_block_failed(self, height: int):
        """Marca um bloco como falhou"""
        self.db.execute("""
            INSERT INTO processed_blocks (height, status, retries)
            VALUES (%s, 'failed', 
                COALESCE((SELECT retries FROM processed_blocks WHERE height = %s), 0) + 1)
            ON CONFLICT (height) DO UPDATE SET
                status = EXCLUDED.status,
                retries = EXCLUDED.retries
        """, (height, height))

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

    def scan_blockchain(self, end_height: int):
        """Executa a varredura de forma sequencial"""
        self.current_chain_height = end_height
        logging.info(f"Iniciando varredura até o bloco {end_height}")
        self.print_progress()
        last_progress_time = time.time()

        while True:
            # Obter próximos blocos
            blocks = self._get_next_blocks()
            if not blocks:
                if self.get_last_processed_block() >= end_height:
                    break
                time.sleep(1)
                continue

            # Processar cada bloco em sequência
            for height in blocks:
                success = False
                retries = 0
                
                while not success and retries < RETRY_LIMIT:
                    success = self.process_block(height)
                    if not success:
                        retries += 1
                        time.sleep(2 ** retries)  # Backoff exponencial

                if not success:
                    logging.error(f"Falha  no bloco {height} após {RETRY_LIMIT} tentativas. Será processado novamente na próxima varredura")

                if time.time() - last_progress_time >= 60:  # Exibe progresso a cada 60 segundos
                    self.print_progress()
                    last_progress_time = time.time()

        self.print_progress()
        logging.info("Varredura concluída!")
        
    def _get_next_blocks(self) -> List[int]:
            """Obtém o próximo lote de blocos para processamento"""
            conn = self.db.get_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute("BEGIN")
                    
                    cur.execute("""
                        WITH candidates AS (
                            SELECT height FROM processed_blocks
                            WHERE status = 'failed' 
                            UNION ALL
                            SELECT COALESCE(MAX(height), -1) + 1 FROM processed_blocks
                            WHERE height NOT IN (
                                SELECT height FROM processed_blocks
                                WHERE status = 'failed' 
                            )
                            UNION ALL
                            SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM processed_blocks)
                        )
                        SELECT height FROM candidates
                        WHERE height <= %s
                        ORDER BY height
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                    """, (self.current_chain_height, BLOCK_BATCH_SIZE))
                    
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
        """Obtém a altura do último bloco processado"""
        result = self.db.fetch_one(
            "SELECT MAX(height) as last_block FROM processed_blocks WHERE status = 'processed'"
        )
        return result['last_block'] if result and result['last_block'] is not None else -1


    def print_progress(self):
        """Exibe estatísticas de progresso detalhadas"""
        try:
            stats = self.db.fetch_one("""
                SELECT 
                    COUNT(*) FILTER (WHERE status = 'processed') as processed,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed,
                    MAX(height) FILTER (WHERE status = 'processed') as tip
                FROM processed_blocks
            """)
            
            if stats:
                remaining = max(self.current_chain_height - stats['tip'], 0) if stats['tip'] else self.current_chain_height
                logging.info(
                    f"Progresso | Processed: {stats['processed']} | Failed: {stats['failed']} | "
                    f"Tip: {stats['tip'] or 0} | Remaining: {remaining}"
                )
        except Exception as e:
            logging.error(f"Erro ao exibir progresso: {str(e)}")

    def get_balances(self, min_balance: float = 0.0) -> Dict[str, float]:
        """Retorna saldos formatados em BTC"""
        try:
            rows = self.db.fetch_all(
                "SELECT address, balance FROM balances WHERE balance >= %s",
                (int(min_balance * 100_000_000),)
            )
            return {row['address']: row['balance'] / 100_000_000 for row in rows}
        except Exception as e:
            logging.error(f"Erro ao obter saldos: {e}")
            return {}