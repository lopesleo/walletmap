import requests
from requests.auth import HTTPBasicAuth
import logging
import time
from typing import Optional, Dict

class RPCClient:
    def __init__(self, network: str, rpc_user: str, rpc_password: str, rpc_url: str = None):
        self.rpc_url = rpc_url or self._get_default_url(network)
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(rpc_user, rpc_password)
        self.headers = {'content-type': 'application/json'}

    def _get_default_url(self, network: str) -> str:
        base_url = 'http://testchain.chon.group:'# http://testchain.chon.group:48332/
        return {
            'mainnet': f'{base_url}8332/',
            'testnet4': f'{base_url}48332/'
        }[network]

    def call(self, method: str, params: list = None, retries: int = 3) -> Optional[Dict]:
        payload = {
            "method": method,
            "params": params or [],
            "jsonrpc": "2.0",
            "id": 0,
        }

        for attempt in range(retries + 1):
            try:
                response = self.session.post(
                    self.rpc_url,
                    json=payload,
                    headers=self.headers,
                    timeout=30
                )
                response.raise_for_status()
                data = response.json()
                
                if error := data.get('error'):
                    logging.warning(f"Erro RPC ({method}): {error}")
                    time.sleep(2 ** attempt)
                    continue
                
                return data['result']

            except Exception as e:
                if attempt < retries:
                    logging.warning(f"Tentativa {attempt + 1}/{retries} falhou: {e}")
                    time.sleep(2 ** attempt)
                else:
                    logging.error(f"Falha final na chamada {method}: {e}")
                    return None