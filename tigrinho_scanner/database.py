import psycopg2
from psycopg2 import pool, sql
from psycopg2.extensions import connection
from typing import Optional, Any, List, Dict
import logging

class DatabaseManager:
    def __init__(self):
        self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=5,
            maxconn=20,
            user="scanner",
            password="cefetfriburgo",
            host="localhost",
            port="5432",
            database="bitcoin",
            options='-c client_encoding=UTF8'
        )
        
        self._initialize_database()

    def _initialize_database(self):
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Criação das tabelas
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS balances (
                        address VARCHAR(128) PRIMARY KEY,
                        balance BIGINT NOT NULL DEFAULT 0
                    )
                """)
                
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS utxos (
                        txid VARCHAR(64),
                        vout INTEGER,
                        address VARCHAR(128),
                        value BIGINT,
                        PRIMARY KEY (txid, vout)
                    )
                """)
                
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS metadata (
                        key VARCHAR(128) PRIMARY KEY,
                        value TEXT
                    )
                """)
                
                # Inserção inicial com tratamento de conflito
                cur.execute("""
                    INSERT INTO metadata (key, value)
                    VALUES ('last_block', '0')
                    ON CONFLICT (key) DO NOTHING
                """)
                
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS processed_blocks (
                        height INTEGER PRIMARY KEY,
                        status VARCHAR(20) CHECK(status IN ('pending', 'processed', 'failed')),
                        retries INTEGER DEFAULT 0,
                        last_attempt TIMESTAMP
                    )
                """)
                conn.commit()

    def get_connection(self) -> connection:
        """Obtém uma conexão do pool"""
        conn = self.connection_pool.getconn()
        conn.autocommit = False
        conn.set_client_encoding('UTF8')

        return conn

    def return_connection(self, conn: connection):
        """Devolve uma conexão ao pool"""
        self.connection_pool.putconn(conn)

    def execute(self, query: str, params: Optional[tuple] = None) -> None:
        """Executa uma query sem retorno de resultados"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                conn.commit()
        except Exception as e:
            conn.rollback()
            logging.error(f"Erro na execução da query: {e}")
            raise
        finally:
            self.return_connection(conn)

    def fetch_one(self, query: str, params: Optional[tuple] = None) -> Optional[Dict]:
        """Executa uma query e retorna um único resultado"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                if result:
                    return {desc[0]: value for desc, value in zip(cur.description, result)}
                return None
        except Exception as e:
            logging.error(f"Erro na consulta: {e}")
            return None
        finally:
            self.return_connection(conn)

    def fetch_all(self, query: str, params: Optional[tuple] = None) -> List[Dict]:
        """Executa uma query e retorna todos os resultados"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                results = cur.fetchall()
                return [
                    {desc[0]: value for desc, value in zip(cur.description, row)}
                    for row in results
                ]
        except Exception as e:
            logging.error(f"Erro na consulta: {e}")
            return []
        finally:
            self.return_connection(conn)

    def get_last_processed_block(self) -> int:
        """Retorna o último bloco processado"""
        result = self.fetch_one("SELECT value FROM metadata WHERE key = 'last_block'")
        return int(result['value']) if result else -1

    def close_pool(self):
        """Fecha todas as conexões do pool"""
        self.connection_pool.closeall()

