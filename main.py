import requests
from requests.auth import HTTPBasicAuth

# Configurações RPC
rpc_user = 'tigrinho'
rpc_password = 'cefetfriburgo'
rpc_url = 'http://testchain.chon.group:18332/'

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
        print(f"Erro RPC ({method}): {e}")
        return None

def derive_address(script_hex):
    # Usa o RPC para decodificar o script
    decoded = rpc_call("decodescript", [script_hex])
    logging.info(f"Decodificado: {decoded}")
    if decoded and decoded.get("addresses"):
        return decoded["addresses"][0]  # Retorna o primeiro endereço associado
    return None

def process_block(block, utxo_map, address_balances):
    for tx in block["tx"]:
        txid = tx["txid"]
        
        # Processar saídas (UTXOs)
        for vout_index, vout in enumerate(tx["vout"]):
            script_hex = vout["scriptPubKey"]["hex"]
            value = vout["value"]
            logging.info(f"UTXO: {txid}:{vout_index} - {value:.8f} BTC - {script_hex}")
            logging.info(value)
            # Tentar decodificar o endereço
            address = derive_address(script_hex)
            
            if address:
                utxo_key = (txid, vout_index)
                utxo_map[utxo_key] = {"address": address, "value": value}
                address_balances[address] = address_balances.get(address, 0) + value
            else:
                print(f"Script não suportado: {script_hex}")
        
        # Processar entradas (UTXOs gastos)
        for vin in tx.get("vin", []):
            if "txid" not in vin or "vout" not in vin:
                continue  # Ignorar coinbase
            spent_utxo_key = (vin["txid"], vin["vout"])
            if spent_utxo_key in utxo_map:
                spent_utxo = utxo_map.pop(spent_utxo_key)
                address_balances[spent_utxo["address"]] -= spent_utxo["value"]
    
    return utxo_map, address_balances

# Execução principal
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
    process_blocks(0, 100)
except Exception as e:
    logging.error(f"Erro crítico: {e}")