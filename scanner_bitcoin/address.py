import base58
import bech32
import logging
from typing import Optional

def derive_address(script_hex: str, network: str) -> Optional[str]:
    try:
        if network == 'testnet4':
            bech32_hrp = 'tb'
            p2pkh_prefix = b'\x6f'
            p2sh_prefix = b'\xc4'
        else:
            bech32_hrp = 'bc'
            p2pkh_prefix = b'\x00'
            p2sh_prefix = b'\x05'

        if script_hex.startswith('76a914'):  # P2PKH
            pubkey_hash = bytes.fromhex(script_hex[6:-4])
            return base58.b58encode_check(p2pkh_prefix + pubkey_hash).decode()
        
        elif script_hex.startswith('a914'):  # P2SH
            script_hash = bytes.fromhex(script_hex[4:-2])
            return base58.b58encode_check(p2sh_prefix + script_hash).decode()
        
        elif script_hex.startswith('0014'):  # P2WPKH
            witness_program = bytes.fromhex(script_hex[4:])
            return bech32.encode(bech32_hrp, 0, witness_program)
        
        elif script_hex.startswith('0020'):  # P2WSH
            witness_program = bytes.fromhex(script_hex[4:])
            return bech32.encode(bech32_hrp, 0, witness_program)
        
        elif script_hex.startswith('5120'):  # P2TR
            witness_program = bytes.fromhex(script_hex[2:])
            return bech32.encode(bech32_hrp, 1, witness_program)
        
        return None
    
    except Exception as e:
        logging.error(f"Erro ao derivar endereço: {e}")
        return None