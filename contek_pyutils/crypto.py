from base64 import b64decode, b64encode
from hashlib import sha256

from Crypto.Cipher import AES

# Padding for the input string --not
# related to encryption itself.
BLOCK_SIZE = 16  # Bytes

pad = lambda s: s + (BLOCK_SIZE - len(s) % BLOCK_SIZE) * chr(BLOCK_SIZE - len(s) % BLOCK_SIZE)
unpad = lambda s: s[: -ord(s[len(s) - 1 :])]


def aes_encrypt(raw, key):
    key = sha256(key.encode("utf8")).digest()
    raw = pad(raw)
    cipher = AES.new(key, AES.MODE_ECB)
    return b64encode(cipher.encrypt(raw.encode("utf8"))).decode("utf8")


def aes_decrypt(enc, key):
    key = sha256(key.encode("utf8")).digest()
    enc = b64decode(enc)
    cipher = AES.new(key, AES.MODE_ECB)
    return unpad(cipher.decrypt(enc)).decode("utf8")


def decrypt_secret(enc):
    key = input("Enter the key for the secret: ")
    secret = aes_decrypt(enc, key)

    if not secret:
        raise Exception("Wrong key!")
    else:
        return secret
