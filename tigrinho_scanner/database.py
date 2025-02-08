import sqlite3
import threading
from typing import Optional

class DatabaseManager:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.local = threading.local()

   
    def _get_connection(self) -> sqlite3.Connection:
        if not hasattr(self.local, 'conn'):
            self.local.conn = sqlite3.connect(self.db_file, check_same_thread=False)
            self._initialize_database(self.local.conn)
        return self.local.conn

    def _initialize_database(self, conn: sqlite3.Connection):
        with conn:
            # Criação de tabelas
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
            conn.execute('''
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )''')
            conn.execute('''
                INSERT OR IGNORE INTO metadata (key, value)
                VALUES ('last_block', '0')
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS processed_blocks (
                    height INTEGER PRIMARY KEY,
                    status TEXT CHECK(status IN ('pending', 'processed', 'failed')),
                    retries INTEGER DEFAULT 0,
                    last_attempt TIMESTAMP
                )
            ''')

    def execute(self, query: str, params: tuple = ()):
        with self._get_connection() as conn:
            conn.execute(query, params)

    def fetch_one(self, query: str, params: tuple = ()):
        with self._get_connection() as conn:
            cursor = conn.execute(query, params)
            return cursor.fetchone()

    def fetch_all(self, query: str, params: tuple = ()):
        with self._get_connection() as conn:
            cursor = conn.execute(query, params)
            return cursor.fetchall()

    def get_last_processed_block(self) -> int:
        row = self.fetch_one("SELECT value FROM metadata WHERE key = 'last_block'")
        return int(row[0]) if row else -1
   
    def connection(self) -> sqlite3.Connection:
        """Retorna uma conexão com contexto"""
        conn = sqlite3.connect(self.db_file, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn