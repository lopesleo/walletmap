import requests
from requests.auth import HTTPBasicAuth
import hashlib
import base58
import bech32
import sqlite3
import logging
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Tuple, Optional

# Configurações
RPC_USER = 'tigrinho'
RPC_PASSWORD = 'cefetfriburgo'
RPC_URL = 'http://testchain.chon.group:48332/'
DATABASE_FILE = 'bitcoin_balances.db'
MAX_WORKERS = 1

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bitcoin_scanner.log'),
        logging.StreamHandler()
    ]
)

class BitcoinScanner:
    def __init__(self):
        self.local = threading.local()  # Armazenamento por thread
        self._init_db()
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(RPC_USER, RPC_PASSWORD)
        self.headers = {'content-type': 'application/json'}
    def get_last_processed_block(self) -> int:
        """Retorna a altura do último bloco processado"""
        conn = self._get_conn()
        cursor = conn.execute("SELECT value FROM metadata WHERE key = 'last_block'")
        row = cursor.fetchone()
        return int(row[0]) if row else -1

    def update_last_block(self, height: int):
        """Atualiza o último bloco processado"""
        conn = self._get_conn()
        with conn:
            conn.execute('''
                UPDATE metadata 
                SET value = ?
                WHERE key = 'last_block'
            ''', (str(height),))
    def _get_conn(self) -> sqlite3.Connection:
        """Obtém conexão SQLite específica da thread"""
        if not hasattr(self.local, 'conn'):
            self.local.conn = sqlite3.connect(DATABASE_FILE, check_same_thread=False)
            self._init_db(self.local.conn)
        return self.local.conn

    def _init_db(self, conn: sqlite3.Connection = None):
        """Inicializa o banco de dados"""
        conn = conn or self._get_conn()
        with conn:
            # Tabelas existentes
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
            
            # Nova tabela de metadados
            conn.execute('''
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )''')
            
            # Inicializa o último bloco se não existir
            conn.execute('''
                INSERT OR IGNORE INTO metadata (key, value)
                VALUES ('last_block', '-1')
            ''')

    def rpc_call(self, method: str, params: list = None, retries: int = 3) -> Optional[dict]:
        """Chamada RPC com tratamento de erros"""
        payload = {
            "method": method,
            "params": params or [],
            "jsonrpc": "2.0",
            "id": 0,
        }

        for attempt in range(retries):
            try:
                response = self.session.post(RPC_URL, json=payload, headers=self.headers, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if error := data.get('error'):
                    logging.error(f"Erro RPC ({method}): {error}")
                    return None
                
                return data['result']

            except Exception as e:
                logging.warning(f"Tentativa {attempt + 1}/{retries} falhou: {e}")
                time.sleep(2 ** attempt)
        
        logging.error(f"Falha após {retries} tentativas: {method}")
        return None

    @staticmethod
    def derive_address(script_hex: str) -> Optional[str]:
        """Deriva endereços de scripts Bitcoin"""
        try:
            if script_hex.startswith('76a914'):
                pubkey_hash = bytes.fromhex(script_hex[6:-4])
                return base58.b58encode_check(b'\x00' + pubkey_hash).decode()
            
            elif script_hex.startswith('a914'):
                script_hash = bytes.fromhex(script_hex[4:-2])
                return base58.b58encode_check(b'\x05' + script_hash).decode()
            
            elif script_hex.startswith('0014'):
                witness_program = bytes.fromhex(script_hex[4:])
                return bech32.encode('bc', 0, witness_program)
            
            elif script_hex.startswith('0020'):
                witness_program = bytes.fromhex(script_hex[4:])
                return bech32.encode('bc', 0, witness_program)
            
            elif script_hex.startswith('5120'):
                witness_program = bytes.fromhex(script_hex[2:])
                return bech32.encode('bc', 1, witness_program)
            
            return None
        
        except Exception as e:
            logging.error(f"Erro ao derivar endereço: {e}")
            return None

    def process_transaction(self, tx: dict):
        """Processa transações de forma thread-safe"""
        conn = self._get_conn()
        with conn:
            # Processar outputs
            for vout in tx['vout']:
                script_hex = vout['scriptPubKey']['hex']
                address = self.derive_address(script_hex)
                
                if not address:
                    continue
                
                value = int(vout['value'] * 100_000_000)
                txid = tx['txid']
                vout_index = vout['n']
                
                conn.execute('''
                    INSERT OR IGNORE INTO utxos VALUES (?, ?, ?, ?)
                ''', (txid, vout_index, address, value))
                
                conn.execute('''
                    INSERT INTO balances VALUES (?, ?)
                    ON CONFLICT(address) DO UPDATE SET
                    balance = balance + excluded.balance
                ''', (address, value))

            # Processar inputs
            for vin in tx.get('vin', []):
                if 'txid' not in vin:
                    continue
                
                spent_txid = vin['txid']
                spent_vout = vin['vout']
                
                cursor = conn.execute('''
                    SELECT address, value FROM utxos
                    WHERE txid = ? AND vout = ?
                ''', (spent_txid, spent_vout))
                
                if (row := cursor.fetchone()):
                    address, value = row
                    
                    conn.execute('''
                        DELETE FROM utxos
                        WHERE txid = ? AND vout = ?
                    ''', (spent_txid, spent_vout))
                    
                    conn.execute('''
                        UPDATE balances
                        SET balance = balance - ?
                        WHERE address = ?
                    ''', (value, address))

    def process_block(self, block_height: int):
        """Processa um bloco de forma isolada por thread"""
        try:
            # Verifica se já foi processado
            if block_height <= self.get_last_processed_block():
                logging.debug(f"Bloco {block_height} já processado. Pulando.")
                return True
                
            block_hash = self.rpc_call('getblockhash', [block_height])
            if not block_hash:
                return False
            
            block = self.rpc_call('getblock', [block_hash, 2])
            if not block:
                return False
            
            logging.info(f"Processando bloco {block_height} ({len(block['tx'])} transações)")
            
            for tx in block['tx']:
                self.process_transaction(tx)
            
            # Atualiza apenas se processado com sucesso
            self.update_last_block(block_height)
            return True
    
        except Exception as e:
            logging.error(f"Erro no bloco {block_height}: {e}")
            return False

    def scan_blockchain(self, end_height: int):
        """Varre desde o último bloco processado até a altura especificada"""
        start_height = self.get_last_processed_block() + 1
        
        if start_height > end_height:
            logging.info("Nenhum bloco novo para processar")
            return
        
        logging.info(f"Iniciando varredura dos blocos {start_height}-{end_height}")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(self.process_block, height): height
                for height in range(start_height, end_height + 1)
            }
            
            for future in as_completed(futures):
                height = futures[future]
                try:
                    if not future.result():
                        logging.warning(f"Bloco {height} processado com falha")
                except Exception as e:
                    logging.error(f"Erro crítico no bloco {height}: {e}")

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


if __name__ == '__main__':
    scanner = BitcoinScanner()
    
    # Obtém altura atual da blockchain
    current_height = scanner.rpc_call('getblockcount')
    if current_height is None:
        logging.error("Falha ao obter altura da blockchain")
        exit(1)
    
    # Varre apenas os novos blocos
    scanner.scan_blockchain(current_height)
    
    # Exibir status
    last_processed = scanner.get_last_processed_block()
    logging.info(f"Último bloco processado: {last_processed}/{current_height}")
    
    # Exibir saldos
    print("\nTop Carteiras:")
    balances = scanner.get_balances(min_balance=0.00000001)  # Mostra todos os saldos
    for address, balance in balances.items():
        print(f"{address}: {balance:.8f} BTC")