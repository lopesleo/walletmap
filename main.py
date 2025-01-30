import requests
from requests.auth import HTTPBasicAuth
import argparse
import base58
import bech32
import sqlite3
import logging
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional

URL = 'http://testchain.chon.group:'
DEFAULT_RPC_URLS = {
    'mainnet': f'{URL}8332/',
    'testnet4': f'{URL}48332/'  
}

MAX_WORKERS = 5  # Reduzido para melhor controle
RETRY_LIMIT = 3
BLOCK_BATCH_SIZE = 10  # Processa em lotes para melhor performance

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bitcoin_scanner.log', encoding='utf-8'),
        logging.StreamHandler()
    ])

class BitcoinScanner:
    def __init__(self, network: str = 'mainnet', rpc_user: str = 'tigrinho', 
                 rpc_password: str = 'cefetfriburgo', rpc_url: Optional[str] = None,
                 db_file: Optional[str] = None):
        self.network = network
        self.rpc_user = rpc_user
        self.rpc_password = rpc_password
        self.rpc_url = rpc_url or DEFAULT_RPC_URLS.get(network)
        self.db_file = db_file or f'bitcoin_balances_{network}.db'

        # Configurações de rede
        if self.network == 'testnet4':
            self.bech32_hrp = 'tb'
            self.p2pkh_prefix = b'\x6f'
            self.p2sh_prefix = b'\xc4'
        else:
            self.bech32_hrp = 'bc'
            self.p2pkh_prefix = b'\x00'
            self.p2sh_prefix = b'\x05'

        self.local = threading.local()
        self._init_db()
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(self.rpc_user, self.rpc_password)
        self.headers = {'content-type': 'application/json'}

    def _get_conn(self) -> sqlite3.Connection:
        if not hasattr(self.local, 'conn'):
            self.local.conn = sqlite3.connect(self.db_file, check_same_thread=False)
            self._init_db(self.local.conn)
        return self.local.conn

    def _init_db(self, conn: sqlite3.Connection = None):
        conn = conn or self._get_conn()
        with conn:
            # Tabelas principais
            conn.execute('''
                CREATE TABLE IF NOT EXISTS balances (
                    address TEXT PRIMARY KEY,
                    balance INTEGER NOT NULL DEFAULT 0
                )''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS utxos (
                    txid TEXT,
                    vout INTEGER,
                    address TEXT,
                    value INTEGER,
                    PRIMARY KEY (txid, vout)
                )''')
            
            # Metadados e controle
            conn.execute('''
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )''')
            conn.execute('''
                INSERT OR IGNORE INTO metadata (key, value)
                VALUES ('last_block', '-1')
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS processed_blocks (
                    height INTEGER PRIMARY KEY,
                    status TEXT CHECK(status IN ('pending', 'processed', 'failed')),
                    retries INTEGER DEFAULT 0,
                    last_attempt TIMESTAMP
                )
            ''')

    def get_next_blocks(self) -> list:
        """Obtém blocos pendentes de forma segura (versão corrigida)"""
        conn = self._get_conn()
        with conn:
            conn.execute("BEGIN IMMEDIATE")
            
            # Busca blocos de 3 formas diferentes:
            # 1. Blocos falhos com retentativas disponíveis
            # 2. Próximo bloco sequencial não processado
            # 3. Primeiro bloco da blockchain (height = 0)
            cursor = conn.execute('''
                SELECT height FROM (
                    -- Blocos falhos com retentativas disponíveis
                    SELECT height FROM processed_blocks 
                    WHERE status = 'failed' AND retries < ?
                    
                    UNION ALL
                    
                    -- Próximo bloco sequencial
                    SELECT COALESCE(MAX(height), -1) + 1 FROM processed_blocks
                    WHERE height NOT IN (
                        SELECT height FROM processed_blocks 
                        WHERE status = 'failed' AND retries < ?
                    )
                    
                    UNION ALL
                    
                    -- Força o primeiro bloco se nenhum existir
                    SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM processed_blocks)
                )
                WHERE height <= ?
                ORDER BY height
                LIMIT ?
            ''', (RETRY_LIMIT, RETRY_LIMIT, self.current_chain_height, BLOCK_BATCH_SIZE))
            
            next_blocks = [row[0] for row in cursor.fetchall() if row[0] is not None]
            
            # Marca como pending
            for height in next_blocks:
                conn.execute('''
                    INSERT OR REPLACE INTO processed_blocks 
                    (height, status, retries, last_attempt)
                    VALUES (?, 'pending', COALESCE(
                        (SELECT retries FROM processed_blocks WHERE height = ?), 0
                    ) + 1, CURRENT_TIMESTAMP)
                ''', (height, height))
            
            conn.commit()
            return next_blocks

    def process_block(self, block_height: int) -> bool:
        """Processa um bloco com transação atômica"""
        conn = self._get_conn()
        try:
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                
                # Verifica se já foi processado
                cursor = conn.execute('''
                    SELECT status FROM processed_blocks
                    WHERE height = ? AND status = 'processed'
                ''', (block_height,))
                if cursor.fetchone():
                    return True

                logging.info(f"Iniciando processamento do bloco {block_height}")
                
                # Obtém dados do bloco
                block_hash = self.rpc_call('getblockhash', [block_height])
                if not block_hash:
                    raise ValueError("Falha ao obter hash do bloco")
                
                block = self.rpc_call('getblock', [block_hash, 2])
                if not block:
                    raise ValueError("Falha ao obter dados do bloco")

                # Processa transações
                for tx in block['tx']:
                    self._process_transaction(conn, tx)

                # Atualiza último bloco processado
                current_last = self.get_last_processed_block()
                if block_height == current_last + 1:
                    new_last = block_height
                    # Avança até encontrar gap
                    while True:
                        next_block = new_last + 1
                        cursor = conn.execute('''
                            SELECT 1 FROM processed_blocks
                            WHERE height = ? AND status = 'processed'
                        ''', (next_block,))
                        if not cursor.fetchone():
                            break
                        new_last = next_block
                    conn.execute('''
                        UPDATE metadata SET value = ?
                        WHERE key = 'last_block'
                    ''', (str(new_last),))

                # Marca como processado
                conn.execute('''
                    UPDATE processed_blocks
                    SET status = 'processed'
                    WHERE height = ?
                ''', (block_height,))
                
                conn.commit()
                return True

        except Exception as e:
            conn.rollback()
            logging.error(f"Erro no bloco {block_height}: {str(e)}")
            with conn:
                conn.execute('''
                    UPDATE processed_blocks
                    SET status = 'failed'
                    WHERE height = ?
                ''', (block_height,))
            return False

    def _process_transaction(self, conn: sqlite3.Connection, tx: dict):
        """Processa uma transação dentro de uma transação existente"""
        # Process outputs
        for vout in tx['vout']:
            script_hex = vout['scriptPubKey']['hex']
            address = self.derive_address(script_hex)
            if not address:
                continue

            value = int(vout['value'] * 100_000_000)
            conn.execute('''
                INSERT OR IGNORE INTO utxos VALUES (?, ?, ?, ?)
            ''', (tx['txid'], vout['n'], address, value))
            
            conn.execute('''
                INSERT INTO balances VALUES (?, ?)
                ON CONFLICT(address) DO UPDATE SET
                balance = balance + excluded.balance
            ''', (address, value))

        # Process inputs
        for vin in tx.get('vin', []):
            if 'txid' not in vin:
                continue

            txid = vin['txid']
            vout = vin['vout']
            cursor = conn.execute('''
                SELECT address, value FROM utxos
                WHERE txid = ? AND vout = ?
            ''', (txid, vout))
            
            if (row := cursor.fetchone()):
                address, value = row
                conn.execute('DELETE FROM utxos WHERE txid = ? AND vout = ?', (txid, vout))
                conn.execute('''
                    UPDATE balances
                    SET balance = balance - ?
                    WHERE address = ?
                ''', (value, address))

    def scan_blockchain(self, end_height: int):
        """Varredura corrigida com controle preciso"""
        self.current_chain_height = end_height  # Armazena a altura atual
        
        logging.info(f"Iniciando varredura até o bloco {end_height}")
        
        total_processed = 0
        last_log_time = time.time()
        
        while True:
            next_blocks = self.get_next_blocks()
            
            if not next_blocks:
                if self.get_last_processed_block() >= end_height:
                    logging.info("Todos os blocos foram processados")
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
                        logging.error(f"Erro crítico no bloco {height}: {str(e)}")
            
            # Log de progresso a cada 30 segundos
            if time.time() - last_log_time > 30:
                self.print_progress()
                last_log_time = time.time()
        
        self.print_progress()
        logging.info(f"Varredura concluída. Total de blocos processados: {total_processed}")


    def get_last_processed_block(self) -> int:
        """Último bloco confirmado na chain"""
        conn = self._get_conn()
        cursor = conn.execute("SELECT value FROM metadata WHERE key = 'last_block'")
        row = cursor.fetchone()
        return int(row[0]) if row else -1

    def rpc_call(self, method: str, params: list = None, retries: int = 3) -> Optional[dict]:
        """Chamada RPC com tratamento robusto de erros"""
        payload = {
            "method": method,
            "params": params or [],
            "jsonrpc": "2.0",
            "id": 0,
        }

        for attempt in range(retries + 1):
            try:
                response = self.session.post(
                    self.rpc_url,
                    json=payload,
                    headers=self.headers,
                    timeout=30
                )
                response.raise_for_status()
                data = response.json()
                
                if error := data.get('error'):
                    logging.warning(f"Erro RPC ({method}): {error}")
                    time.sleep(2 ** attempt)
                    continue
                
                return data['result']

            except Exception as e:
                if attempt < retries:
                    logging.warning(f"Tentativa {attempt + 1}/{retries} falhou: {e}")
                    time.sleep(2 ** attempt)
                else:
                    logging.error(f"Falha final na chamada {method}: {e}")
                    return None

    def print_progress(self):
        """Exibe progresso detalhado"""
        conn = self._get_conn()
        stats = conn.execute('''
            SELECT 
                COALESCE(SUM(CASE WHEN status = 'processed' THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0)
            FROM processed_blocks
        ''').fetchone()
        
        logging.info(
            f"Progresso: PROCESSADOS {stats[0]} | FALHAS {stats[1]} | PENDENTES {stats[2]} | "
            f"ÚLTIMO BLOCO CONFIRMADO: {self.get_last_processed_block()}"
        )
    def get_balances(self, min_balance: float = 0.0) -> Dict[str, float]:
            """Retorna saldos consolidados"""
            conn = self._get_conn()
            cursor = conn.execute('''
                SELECT address, balance FROM balances
                WHERE balance >= ?
            ''', (int(min_balance * 100_000_000),))
            
            return {
                row[0]: row[1] / 100_000_000
                for row in cursor.fetchall()
            }
    def derive_address(self, script_hex: str) -> Optional[str]:
        """Deriva endereços com tratamento de erros"""
        try:
            if script_hex.startswith('76a914'):  # P2PKH
                pubkey_hash = bytes.fromhex(script_hex[6:-4])
                return base58.b58encode_check(self.p2pkh_prefix + pubkey_hash).decode()
            
            elif script_hex.startswith('a914'):  # P2SH
                script_hash = bytes.fromhex(script_hex[4:-2])
                return base58.b58encode_check(self.p2sh_prefix + script_hash).decode()
            
            elif script_hex.startswith('0014'):  # P2WPKH
                witness_program = bytes.fromhex(script_hex[4:])
                return bech32.encode(self.bech32_hrp, 0, witness_program)
            
            elif script_hex.startswith('0020'):  # P2WSH
                witness_program = bytes.fromhex(script_hex[4:])
                return bech32.encode(self.bech32_hrp, 0, witness_program)
            
            elif script_hex.startswith('5120'):  # P2TR
                witness_program = bytes.fromhex(script_hex[2:])
                return bech32.encode(self.bech32_hrp, 1, witness_program)
            
            return None
        
        except Exception as e:
            logging.error(f"Erro ao derivar endereço: {e}")
            return None

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bitcoin Blockchain Scanner')
    parser.add_argument('--network', choices=['mainnet', 'testnet4'], default='mainnet')
    parser.add_argument('--rpc-user', default='tigrinho')
    parser.add_argument('--rpc-password', default='cefetfriburgo')
    parser.add_argument('--rpc-url', default=None)
    parser.add_argument('--db-file', default=None)
    args = parser.parse_args()
    
    scanner = BitcoinScanner(
        network=args.network,
        rpc_user=args.rpc_user,
        rpc_password=args.rpc_password,
        rpc_url=args.rpc_url,
        db_file=args.db_file
    )
    try:
            blockchain_info = scanner.rpc_call('getblockchaininfo')
            if not blockchain_info:
                raise ConnectionError("Resposta RPC inválida")
                
            current_height = blockchain_info['blocks']
            logging.info(f"Dados da blockchain:")
            logging.info(f"- Rede: {args.network.upper()}")
            logging.info(f"- Altura atual: {current_height}")
            logging.info(f"- Hash do último bloco: {blockchain_info['bestblockhash']}")
            
    except Exception as e:
        logging.error(f"Falha na conexão RPC: {str(e)}")
        exit(1)

    try:
        scanner.scan_blockchain(current_height)
    except KeyboardInterrupt:
        logging.warning("\nVarredura interrompida pelo usuário")
    finally:
        final_height = scanner.get_last_processed_block()
        logging.info(f"Status final:")
        logging.info(f"- Último bloco processado: {final_height}")
        logging.info(f"- Blocos restantes: {current_height - final_height}")
        
        print("\nResumo de saldos:")
        if final_height > 0:
            balances = scanner.get_balances(min_balance=0.00000001)
            for address, balance in balances.items():
                print(f"{address}: {balance:.8f} BTC")
        else:
            print("Nenhum bloco processado")