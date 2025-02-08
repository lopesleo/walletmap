"""
Bitcoin Blockchain Scanner
==========================

Um pacote para análise e rastreamento de transações na blockchain Bitcoin.
"""

__version__ = "0.1.0"
__author__ = "Leonardo Lopes <leonardo.lopes@aluno.cefet-rj.br>"

# Importações relativas explícitas
from .scanner import BitcoinScanner
from .rpc_client import RPCClient
from .database import DatabaseManager

__all__ = [
    'BitcoinScanner',
    'RPCClient',
    'DatabaseManager'
]