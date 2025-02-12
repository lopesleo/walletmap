import hashlib
import logging
from enum import Enum
from typing import Optional, ByteString, Tuple

# Implementação Bech32/Bech32m -------------------------------------------------
class Encoding(Enum):
    BECH32 = 1
    BECH32M = 2

CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
BECH32M_CONST = 0x2BC830A3

def bech32_polymod(values: ByteString) -> int:
    generator = [0x3B6A57B2, 0x26508E6D, 0x1EA119FA, 0x3D4233DD, 0x2A1462B3]
    chk = 1
    for value in values:
        top = chk >> 25
        chk = (chk & 0x1FFFFFF) << 5 ^ value
        for i in range(5):
            chk ^= generator[i] if ((top >> i) & 1) else 0
    return chk

def bech32_hrp_expand(hrp: str) -> bytes:
    return bytes([ord(x) >> 5 for x in hrp] + [0] + [ord(x) & 31 for x in hrp])

def bech32_verify_checksum(hrp: str, data: bytes) -> Encoding:
    const = bech32_polymod(bech32_hrp_expand(hrp) + data)
    if const == 1:
        return Encoding.BECH32
    if const == BECH32M_CONST:
        return Encoding.BECH32M
    raise ValueError("Invalid checksum")

def bech32_create_checksum(hrp: str, data: bytes, spec: Encoding) -> bytes:
    values = bech32_hrp_expand(hrp) + data
    const = BECH32M_CONST if spec == Encoding.BECH32M else 1
    polymod = bech32_polymod(values + bytes(6)) ^ const
    return bytes((polymod >> 5 * (5 - i)) & 31 for i in range(6))

def bech32_encode(hrp: str, data: bytes, spec: Encoding) -> str:
    combined = data + bech32_create_checksum(hrp, data, spec)
    return hrp + "1" + "".join([CHARSET[d] for d in combined])

def convertbits(data: ByteString, frombits: int, tobits: int, pad: bool = True) -> bytearray:
    acc = 0
    bits = 0
    ret = bytearray()
    maxv = (1 << tobits) - 1
    max_acc = (1 << (frombits + tobits - 1)) - 1
    for value in data:
        if value < 0 or (value >> frombits):
            raise ValueError("Invalid value")
        acc = ((acc << frombits) | value) & max_acc
        bits += frombits
        while bits >= tobits:
            bits -= tobits
            ret.append((acc >> bits) & maxv)
    if pad and bits:
        ret.append((acc << (tobits - bits)) & maxv)
    elif bits >= frombits or ((acc << (tobits - bits)) & maxv):
        raise ValueError("Invalid padding")
    return ret

def segwit_encode(hrp: str, witver: int, witprog: bytes) -> str:
    spec = Encoding.BECH32 if witver == 0 else Encoding.BECH32M
    converted = convertbits(witprog, 8, 5)
    return bech32_encode(hrp, bytes([witver]) + converted, spec)

# Implementação Base58 ---------------------------------------------------------
def base58_encode_check(prefix: bytes, payload: bytes) -> str:
    data = prefix + payload
    checksum = hashlib.sha256(hashlib.sha256(data).digest()).digest()[:4]
    b58_chars = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
    n = int.from_bytes(data + checksum, 'big')
    res = []
    while n > 0:
        n, rem = divmod(n, 58)
        res.append(b58_chars[rem])
    return ''.join(reversed(res)).lstrip('1')  # Remove leading '1's

# Função principal -------------------------------------------------------------
def derive_address(script_hex: str, network: str) -> Optional[str]:
    try:
        # Configurações de rede
        hrp = 'bc' if network == 'mainnet' else 'tb'
        p2pkh_ver = b'\x00' if network == 'mainnet' else b'\x6f'
        p2sh_ver = b'\x05' if network == 'mainnet' else b'\xc4'

        script_bytes = bytes.fromhex(script_hex)

        # P2PKH (Legacy)
        if script_hex.startswith('76a914') and len(script_hex) == 50:
            pubkey_hash = script_bytes[3:23]  # OP_DUP OP_HASH160 <hash> OP_EQUALVERIFY OP_CHECKSIG
            return base58_encode_check(p2pkh_ver, pubkey_hash)

        # P2SH (Legacy)
        elif script_hex.startswith('a914') and len(script_hex) == 46:
            script_hash = script_bytes[2:22]  # OP_HASH160 <hash> OP_EQUAL
            return base58_encode_check(p2sh_ver, script_hash)

        # P2WPKH (SegWit v0)
        elif script_hex.startswith('0014') and len(script_hex) == 44:
            witness_program = script_bytes[2:]  # OP_0 <20-byte-hash>
            return segwit_encode(hrp, 0, witness_program)

        # P2WSH (SegWit v0)
        elif script_hex.startswith('0020') and len(script_hex) == 68:
            witness_program = script_bytes[2:]  # OP_0 <32-byte-hash>
            return segwit_encode(hrp, 0, witness_program)

        # P2TR (SegWit v1 - Taproot)
        elif script_hex.startswith('5120') and len(script_hex) == 68:
            witness_program = script_bytes[2:]  # OP_1 <32-byte-hash>
            return segwit_encode(hrp, 1, witness_program)

        return None

    except Exception as e:
        logging.error(f"Error deriving address: {e}", exc_info=True)
        return None
