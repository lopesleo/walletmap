from bitcoinutils.script import Script
from bitcoinutils.setup import setup
from bitcoinutils.constants import TYPE_ABSOLUTE_TIMELOCK
from bitcoinutils.proxy import NodeProxy
import logging
import os

# Configuração aprimorada para diferentes redes
NETWORK = 'testnet'  # Mude para 'mainnet' posteriormente
setup(NETWORK)

# Configurações RPC dinâmicas
RPC_CONFIG = {
    'testnet': {
        'url': os.getenv('RPC_URL', 'http://testchain.chon.group:48332/'),
        'user': os.getenv('RPC_USER', 'tigrinho'),
        'password': os.getenv('RPC_PASSWORD', 'cefetfriburgo')
    },
    'mainnet': {
        'url': os.getenv('RPC_URL_MAIN', 'http://localhost:8332/'),
        'user': os.getenv('RPC_USER_MAIN', 'your_mainnet_user'),
        'password': os.getenv('RPC_PASSWORD_MAIN', 'your_mainnet_password')
    }
}

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BlockchainAnalyzer:
    def __init__(self, network):
        self.network = network
        self.config = RPC_CONFIG[network]
        self.proxy = NodeProxy(self.config['user'], self.config['password'], 
                             self.config['url']).get_proxy()
        self.utxo_map = {}
        self.address_balances = {}
        self.processed_blocks = set()

    def derive_address(self, script_hex):
        """Decodifica scripts complexos usando bitcoinutils"""
        try:
            script = Script.from_hex(script_hex)
            
            # Tenta obter endereço diretamente
            if script.is_p2pkh():
                return script.to_address().to_string()
            elif script.is_p2sh():
                return script.to_address().to_string()
            elif script.is_p2wpkh():
                return script.to_address().to_string()
            elif script.is_p2wsh():
                return script.to_address().to_string()
            elif script.is_p2tr():
                return script.to_address().to_string()
            
            # Fallback para scripts não padrão
            return f'Non-standard script: {script_hex}'
            
        except Exception as e:
            logging.error(f"Erro ao decodificar script {script_hex}: {e}")
            return None

    def process_block(self, block_height):
        """Processa um único bloco"""
        try:
            if block_height in self.processed_blocks:
                return

            block_hash = self.proxy.getblockhash(block_height)
            block = self.proxy.getblock(block_hash, 2)
            
            logging.info(f"Processando bloco {block_height} com {len(block['tx'])} transações")

            for tx in block['tx']:
                self.process_transaction(tx)

            self.processed_blocks.add(block_height)
            
        except Exception as e:
            logging.error(f"Erro no bloco {block_height}: {e}")

    def process_transaction(self, tx):
        """Processa UTXOs e entradas de uma transação"""
        # Processar saídas
        for vout_index, vout in enumerate(tx['vout']):
            script_hex = vout['scriptPubKey']['hex']
            value = vout['value']
            
            address = self.derive_address(script_hex)
            if not address:
                continue
                
            utxo_key = (tx['txid'], vout_index)
            self.utxo_map[utxo_key] = {
                'address': address,
                'value': value,
                'block_height': tx.get('height', None)
            }
            
            self.address_balances[address] = self.address_balances.get(address, 0) + value

        # Processar entradas (UTXOs gastos)
        for vin in tx.get('vin', []):
            if 'txid' not in vin or 'vout' not in vin:
                continue  # Ignorar coinbase
            
            spent_key = (vin['txid'], vin['vout'])
            if spent_key in self.utxo_map:
                spent_utxo = self.utxo_map.pop(spent_key)
                self.address_balances[spent_utxo['address']] -= spent_utxo['value']

    def get_balances(self, min_balance=0):
        """Retorna saldos filtrados"""
        return {addr: bal for addr, bal in self.address_balances.items() if bal >= min_balance}

    def scan_blocks(self, start_height, end_height):
        """Varre um intervalo de blocos"""
        for height in range(start_height, end_height + 1):
            self.process_block(height)

        logging.info("\n=== Saldos finais ===")
        for address, balance in self.get_balances().items():
            logging.info(f"{address}: {balance:.8f} BTC")

# Uso exemplo para testnet
if __name__ == "__main__":
    analyzer = BlockchainAnalyzer('testnet')
    
    try:
        # Obter altura atual da blockchain
        current_height = analyzer.proxy.getblockcount()
        logging.info(f"Altura atual da blockchain: {current_height}")
        
        # Varre os últimos 100 blocos (ajuste conforme necessidade)
        analyzer.scan_blocks(max(0, current_height - 100), current_height)
        
    except Exception as e:
        logging.error(f"Erro crítico: {e}")