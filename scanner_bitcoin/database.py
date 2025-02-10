import psycopg2
from psycopg2 import pool
from psycopg2.extensions import connection
from typing import Optional, List, Dict
import logging
from config import (
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB
)

class DatabaseManager:
    def __init__(self):
        self.connection_params = {
            'dbname': POSTGRES_DB,
            'user': POSTGRES_USER,
            'password': POSTGRES_PASSWORD,
            'host': POSTGRES_HOST,
            'port': POSTGRES_PORT
        }
        
        self.connection_pool = self._create_connection_pool()
        self._initialize_database()

    def _create_connection_pool(self) -> psycopg2.pool.ThreadedConnectionPool:
        """Cria e retorna o pool de conexões"""
        return psycopg2.pool.ThreadedConnectionPool(
            minconn=5,
            maxconn=20,
            **self.connection_params,
            options='-c statement_timeout=30000'  # 30 segundos
        )

    def _initialize_database(self):
        """Inicializa a estrutura do banco de dados"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Criação das tabelas
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS balances (
                        address VARCHAR(128) PRIMARY KEY,
                        balance BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0)
                    )
                """)
                
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS utxos (
                        txid VARCHAR(64) NOT NULL,
                        vout INTEGER NOT NULL CHECK (vout >= 0),
                        address VARCHAR(128) NOT NULL,
                        value BIGINT NOT NULL CHECK (value >= 0),
                        PRIMARY KEY (txid, vout),
                        FOREIGN KEY (address) REFERENCES balances(address) ON DELETE CASCADE
                    )
                """)
                
                # cur.execute("""
                #     CREATE TABLE IF NOT EXISTS metadata (
                #         key VARCHAR(128) PRIMARY KEY,
                #         value TEXT NOT NULL
                #     )
                # """)
                
                # cur.execute("""
                #     INSERT INTO metadata (key, value)
                #     VALUES ('last_block', '0')
                #     ON CONFLICT (key) DO NOTHING
                # """)
                
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS processed_blocks (
                        height INTEGER PRIMARY KEY CHECK (height >= 0),
                        status VARCHAR(20) NOT NULL DEFAULT 'pending' 
                            CHECK(status IN ('pending', 'processed', 'failed')),
                        retries INTEGER NOT NULL DEFAULT 0 CHECK (retries >= 0),
                        last_attempt TIMESTAMP
                    )
                """)
                
                # Índices
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS utxos_address_idx 
                    ON utxos (address)
                """)
                
                conn.commit()

    def get_connection(self) -> connection:
        """Obtém uma conexão do pool"""
        try:
            conn = self.connection_pool.getconn()
            conn.autocommit = False
            return conn
        except psycopg2.Error as e:
            logging.error(f"Erro ao obter conexão: {e}")
            raise

    def return_connection(self, conn: connection):
        """Devolve uma conexão ao pool"""
        if not conn.closed:
            self.connection_pool.putconn(conn)

    def fetch_one(self, query: str, params: tuple = None) -> Optional[Dict]:
        """Executa uma query e retorna um único resultado"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params or ())
                result = cur.fetchone()
                if result:
                    return {desc[0]: value for desc, value in zip(cur.description, result)}
                return None
        except psycopg2.Error as e:
            logging.error(f"Erro no fetch_one: {e}")
            return None
        finally:
            self.return_connection(conn)

    def fetch_all(self, query: str, params: tuple = None) -> List[Dict]:
        """Executa uma query e retorna todos os resultados"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params or ())
                results = cur.fetchall()
                return [
                    {desc[0]: value for desc, value in zip(cur.description, row)}
                    for row in results
                ]
        except psycopg2.Error as e:
            logging.error(f"Erro no fetch_all: {e}")
            return []
        finally:
            self.return_connection(conn)

    def execute(self, query: str, params: tuple = None):
        """Executa uma query sem retorno"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params or ())
                conn.commit()
        except psycopg2.Error as e:
            conn.rollback()
            logging.error(f"Erro na execução: {e}")
            raise
        finally:
            self.return_connection(conn)

    def get_last_processed_block(self) -> int:
            """Obtém a altura do último bloco processado"""
            result = self.fetch_one(
                "SELECT MAX(height) as last_block FROM processed_blocks WHERE status = 'processed'"
            )
            return result['last_block'] if result and result['last_block'] is not None else -1
        
    def close_pool(self):
        """Fecha todas as conexões do pool"""
        self.connection_pool.closeall()