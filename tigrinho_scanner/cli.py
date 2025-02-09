import argparse
import logging
from tigrinho_scanner.scanner import BitcoinScanner
from tigrinho_scanner.rpc_client import RPCClient
from tigrinho_scanner.database import DatabaseManager

def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('bitcoin_scanner.log', encoding='utf-8'),
            logging.StreamHandler()
        ])

def main():
    configure_logging()
    
    parser = argparse.ArgumentParser(description='Bitcoin Blockchain Scanner')
    parser.add_argument('--network', choices=['mainnet', 'testnet4'], default='testnet4')
    parser.add_argument('--rpc-user', default='tigrinho')
    parser.add_argument('--rpc-password', default='cefetfriburgo')
    parser.add_argument('--rpc-url')
    parser.add_argument('--db-file')
    args = parser.parse_args()

    # Inicializar componentes
    rpc_client = RPCClient(
        network=args.network,
        rpc_user=args.rpc_user,
        rpc_password=args.rpc_password,
        rpc_url=args.rpc_url
    )
    
    db_manager = DatabaseManager()
    
    scanner = BitcoinScanner(
        network=args.network,
        rpc_client=rpc_client,
        db_manager=db_manager
    )

    try:
        # Obter altura atual e iniciar varredura
        blockchain_info = rpc_client.call('getblockchaininfo')
        current_height = blockchain_info['blocks']
        scanner.scan_blockchain(current_height)
        
    except KeyboardInterrupt:
        logging.warning("\nVarredura interrompida pelo usuário")
    finally:
        # Exibir resumo final
        last_block = db_manager.get_last_processed_block()
        logging.info(f"Último bloco processado: {last_block}")
        logging.info(f"Blocos restantes: {current_height - last_block}")

if __name__ == '__main__':
    main()