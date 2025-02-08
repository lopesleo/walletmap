import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List
import sqlite3

from bitcoin_scanner.database import DatabaseManager
from bitcoin_scanner.rpc_client import RPCClient
from bitcoin_scanner.address import derive_address

MAX_WORKERS = 10
RETRY_LIMIT = 3
BLOCK_BATCH_SIZE = 10

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

    def _process_transaction(self, tx: dict, conn: sqlite3.Connection):
        """Processa uma transação usando uma conexão existente"""
        # Processar outputs
        for vout in tx['vout']:
            script_hex = vout['scriptPubKey']['hex']
            address = derive_address(script_hex, self.network)
            if not address:
                continue

            value = int(vout['value'] * 100_000_000)
            conn.execute(
                'INSERT OR IGNORE INTO utxos VALUES (?, ?, ?, ?)',
                (tx['txid'], vout['n'], address, value)
            )
            conn.execute(
                '''INSERT INTO balances VALUES (?, ?)
                ON CONFLICT(address) DO UPDATE SET
                balance = balance + excluded.balance''',
                (address, value)
            )

        # Processar inputs
        for vin in tx.get('vin', []):
            if 'txid' not in vin:
                continue

            txid = vin['txid']
            vout = vin['vout']
            cursor = conn.execute(
                'SELECT address, value FROM utxos WHERE txid = ? AND vout = ?',
                (txid, vout)
            )
            row = cursor.fetchone()
            
            if row:
                address, value = row
                conn.execute(
                    'DELETE FROM utxos WHERE txid = ? AND vout = ?',
                    (txid, vout)
                )
                conn.execute(
                    'UPDATE balances SET balance = balance - ? WHERE address = ?',
                    (value, address)
                )

    def process_block(self, block_height: int) -> bool:
        """Processa um bloco de forma atômica"""
        try:
            with self.db.connection() as conn:
                conn.execute("BEGIN IMMEDIATE")

                # Verificar se já foi processado
                cursor = conn.execute(
                    'SELECT 1 FROM processed_blocks WHERE height = ? AND status = "processed"',
                    (block_height,)
                )
                if cursor.fetchone():
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
                cursor = conn.execute(
                    'SELECT value FROM metadata WHERE key = "last_block"'
                )
                current_last = int(cursor.fetchone()[0])
                
                if block_height == current_last + 1:
                    new_last = block_height
                    while True:
                        next_block = new_last + 1
                        cursor = conn.execute(
                            'SELECT 1 FROM processed_blocks WHERE height = ? AND status = "processed"',
                            (next_block,)
                        )
                        if not cursor.fetchone():
                            break
                        new_last = next_block
                    
                    conn.execute(
                        'UPDATE metadata SET value = ? WHERE key = "last_block"',
                        (str(new_last),)
                    )

                # Atualizar status do bloco
                conn.execute(
                    '''INSERT OR REPLACE INTO processed_blocks 
                    (height, status, retries) VALUES (?, "processed", 0)''',
                    (block_height,)
                )

                conn.commit()
                return True

        except Exception as e:
            logging.error(f"Erro no bloco {block_height}: {str(e)}")
            
            # Registrar falha em nova conexão
            try:
                with self.db.connection() as conn:
                    conn.execute(
                        '''INSERT OR REPLACE INTO processed_blocks 
                        (height, status, retries) VALUES (?, "failed", 
                        COALESCE((SELECT retries FROM processed_blocks WHERE height = ?), 0) + 1)''',
                        (block_height, block_height)
                    )
                    conn.commit()
            except Exception as cleanup_error:
                logging.error(f"Erro no cleanup: {cleanup_error}")
            
            return False

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
        try:
            with self.db.connection() as conn: 
                conn.execute("BEGIN IMMEDIATE")
                
                cursor = conn.execute('''
                    SELECT height FROM (
                        SELECT height FROM processed_blocks
                        WHERE status = 'failed' AND retries < ?
                        UNION ALL
                        SELECT COALESCE(MAX(height), -1) + 1 FROM processed_blocks
                        WHERE height NOT IN (
                            SELECT height FROM processed_blocks
                            WHERE status = 'failed' AND retries < ?
                        )
                        UNION ALL
                        SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM processed_blocks)
                    )
                    WHERE height <= ?
                    ORDER BY height
                    LIMIT ?
                ''', (RETRY_LIMIT, RETRY_LIMIT, self.current_chain_height, BLOCK_BATCH_SIZE))
                
                blocks = cursor.fetchall()
                next_blocks = [row[0] for row in blocks if row[0] is not None]

                for height in next_blocks:
                    conn.execute('''
                        INSERT OR REPLACE INTO processed_blocks
                        (height, status, retries)
                        VALUES (?, 'pending', 
                        COALESCE((SELECT retries FROM processed_blocks WHERE height = ?), 0) + 1)
                    ''', (height, height))

                conn.commit()
                return next_blocks

        except Exception as e:
            logging.error(f"Erro ao buscar blocos: {str(e)}")
            return []

    def get_last_processed_block(self) -> int:
        """Retorna a altura do último bloco confirmado"""
        try:
            with self.db.connection() as conn:
                cursor = conn.execute('SELECT value FROM metadata WHERE key = "last_block"')
                row = cursor.fetchone()
                return int(row[0]) if row else -1
        except Exception as e:
            logging.error(f"Erro ao obter último bloco: {e}")
            return -1

    def print_progress(self):
        """Exibe o progresso atual da varredura"""
        try:
            with self.db.connection() as conn:
                cursor = conn.execute('''
                    SELECT 
                        SUM(CASE WHEN status = 'processed' THEN 1 ELSE 0 END),
                        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END),
                        SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END)
                    FROM processed_blocks
                ''')
                stats = cursor.fetchone()

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
            with self.db.connection() as conn:
                cursor = conn.execute(
                    'SELECT address, balance FROM balances WHERE balance >= ?',
                    (int(min_balance * 100_000_000),)
                )
                rows = cursor.fetchall()
                return {row[0]: row[1]/100_000_000 for row in rows}
        except Exception as e:
            logging.error(f"Erro ao obter saldos: {e}")
            return {}