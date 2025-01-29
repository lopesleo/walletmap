import requests
from requests.auth import HTTPBasicAuth
from bitcoinutils.setup import setup
import logging
import os

# Configurar rede (testnet)
setup('testnet')

# Configurações RPC (usando variáveis de ambiente)
rpc_user = os.getenv('RPC_USER', 'tigrinho')  # Valor padrão para testes
rpc_password = os.getenv('RPC_PASSWORD', 'cefetfriburgo')  # Valor padrão para testes
rpc_url = os.getenv('RPC_URL', 'http://testchain.chon.group:48332/')  # Valor padrão para testes

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def rpc_call(method, params=None):
    headers = {'content-type': 'application/json'}
    payload = {
        "method": method,
        "params": params or [],
        "jsonrpc": "2.0",
        "id": 0,
    }
    try:
        response = requests.post(rpc_url, json=payload, headers=headers, 
                               auth=HTTPBasicAuth(rpc_user, rpc_password))
        response.raise_for_status()
        data = response.json()
        if data.get('error'):
            raise Exception(f"RPC Error: {data['error']}")
        return data['result']
    except Exception as e:
        logging.error(f"Erro RPC ({method}): {e}")
        return None

def derive_address(script_hex):
    try:
        # Decodificar o script usando o RPC
        logging.info(script_hex)
        decoded = rpc_call("decodescript", [script_hex])
        
        logging.info(f"Script decodificado: {decoded}")
        
        if decoded and "addresses" in decoded:
            return decoded["addresses"][0]
        
        # Caso o script seja do tipo P2SH ou P2WSH
        if script_hex.startswith("a9") or script_hex.startswith("00"):  # P2SH ou P2WSH
            return handle_p2sh(script_hex)
        
        # Caso do SegWit P2WPKH
        elif script_hex.startswith("76a914") or script_hex.startswith("0014"):  # P2WPKH
            return handle_p2wpkh(script_hex)
        
        # Caso do P2PK (Pay-to-PubKey)
        elif script_hex.startswith("21") or script_hex.startswith("41"):  # P2PK
            return handle_p2pk(script_hex)
        
        # Caso do P2WSH (Pay-to-Witness-Script-Hash)
        elif script_hex.startswith("0020"):  # P2WSH
            return handle_p2wsh(script_hex)
        
        logging.warning(f"Script não suportado: {script_hex}")
        return None
    except Exception as e:
        logging.error(f"Erro ao derivar endereço: {e}")
        return None

def handle_p2sh(script_hex):
    # Decodifica o P2SH (Pay-to-Script-Hash)
    try:
        logging.info(f"Tratando P2SH: {script_hex}")
        # Remover a parte do script
        redeem_script = script_hex[4:]  # Retira 'a9' e '14' (tamanho do hash)
        return "P2SH Address: " + redeem_script
    except Exception as e:
        logging.error(f"Erro ao processar P2SH: {e}")
        return None

def handle_p2wpkh(script_hex):
    # Decodifica o P2WPKH (Pay-to-Witness-PubKey-Hash)
    try:
        logging.info(f"Tratando P2WPKH: {script_hex}")
        # O script SegWit P2WPKH é formado por '0014' seguido pelo hash da chave pública
        return "P2WPKH Address: " + script_hex[4:]
    except Exception as e:
        logging.error(f"Erro ao processar P2WPKH: {e}")
        return None

def handle_p2pk(script_hex):
    # Decodifica o P2PK (Pay-to-PubKey)
    try:
        logging.info(f"Tratando P2PK: {script_hex}")
        # O script P2PK é formado por uma chave pública seguida de OP_CHECKSIG
        pubkey = script_hex[:-2]  # Remove o OP_CHECKSIG (0xac)
        return "P2PK Address: " + pubkey
    except Exception as e:
        logging.error(f"Erro ao processar P2PK: {e}")
        return None

def handle_p2wsh(script_hex):
    # Decodifica o P2WSH (Pay-to-Witness-Script-Hash)
    try:
        logging.info(f"Tratando P2WSH: {script_hex}")
        # O script P2WSH é formado por '0020' seguido pelo hash do script
        script_hash = script_hex[4:]  # Remove o prefixo '0020'
        return "P2WSH Address: " + script_hash
    except Exception as e:
        logging.error(f"Erro ao processar P2WSH: {e}")
        return None

def process_block(block, utxo_map, address_balances):
    for tx in block["tx"]:
        txid = tx["txid"]
        
        # Processar saídas (UTXOs)
        for vout_index, vout in enumerate(tx["vout"]):
            script_hex = vout["scriptPubKey"]["hex"]
            value = vout["value"]
            
            # Tentar decodificar o endereço
            address = derive_address(script_hex)
            
            if address:
                utxo_key = (txid, vout_index)
                utxo_map[utxo_key] = {"address": address, "value": value}
                address_balances[address] = address_balances.get(address, 0) + value
            else:
                logging.warning(f"Script não suportado: {script_hex}")
        
        # Processar entradas (UTXOs gastos)
        for vin in tx.get("vin", []):
            if "txid" not in vin or "vout" not in vin:
                continue  # Ignorar coinbase
            spent_utxo_key = (vin["txid"], vin["vout"])
            if spent_utxo_key in utxo_map:
                spent_utxo = utxo_map.pop(spent_utxo_key)
                address_balances[spent_utxo["address"]] -= spent_utxo["value"]
    
    return utxo_map, address_balances

def process_blocks(start_height, end_height):
    utxo_map = {}
    address_balances = {}
    
    logging.info(f"=== Iniciando varredura de blocos de {start_height} a {end_height} ===")
    
    for height in range(start_height, end_height + 1):
        block_hash = rpc_call("getblockhash", [height])
        if block_hash:
            block = rpc_call("getblock", [block_hash, 2])
            if block:
                logging.info(f"Processando bloco {height} ({len(block['tx'])} transações)")
                utxo_map, address_balances = process_block(block, utxo_map, address_balances)
            else:
                logging.warning(f"Bloco {height} não pôde ser carregado")
        else:
            logging.warning(f"Hash do bloco {height} não pôde ser obtido")
    
    logging.info("\n=== Resultados ===")
    for address, balance in address_balances.items():
        if balance > 0:
            logging.info(f"{address}: {balance:.8f} BTC")

# Execução principal
try:
    process_blocks(0, 10)
except Exception as e:
    logging.error(f"Erro crítico: {e}")


