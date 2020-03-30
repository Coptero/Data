import sys
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from Crypto.Util.Padding import unpad
import base64
import hashlib
from Crypto import Random

BLOCK_SIZE = 16  # AES 16 bytes


class CryptUtils:

    def __init__(self):
        self.Algoritm = "AES/CBC/PKCS5Padding"
        self.key = base64.b64encode(bytes("a2UlQScoptero73E5iHPwg==", "utf-8"))
        self.IvSpec = Random.new().read(AES.block_size)

    def encrypt(self, text):
        cipher = AES.new(self.key, AES.MODE_CBC, self.IvSpec)
        encrypt = cipher.encrypt(pad(text.encode(), BLOCK_SIZE))
        return encrypt

    def decrypt(self, text):
        cipher = AES.new(self.key, AES.MODE_CBC, self.IvSpec)
        decrypt = unpad(cipher.decrypt(text), BLOCK_SIZE)
        return decrypt.decode()


message = "hola"
cifrado = CryptUtils()
encriptado = cifrado.encrypt(message)
descifrado = cifrado.decrypt(encriptado)

print("MENSAJE CIFRADO: ", encriptado)
print("MENSAJE DESCIFRADO:", descifrado)