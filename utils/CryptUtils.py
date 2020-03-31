from cryptography.fernet import Fernet

def decrypt(text):
    key = b'unXTIbnovnAnjRgTWlUTSQ1ln_AsEQntQjGjAW6diCA='
    f = Fernet(key)
    return  f.decrypt(text).decode("utf-8")