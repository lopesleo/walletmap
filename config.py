import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env.development")  


MAINNET_RPC_PORT = os.getenv("MAINNET_RPC_PORT", "8332")
TESTNET4_RPC_PORT = os.getenv("TESTNET4_RPC_PORT", "48332")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")