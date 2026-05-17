from __future__ import annotations

import base64
import os

from cryptography.fernet import Fernet, InvalidToken
from dotenv import load_dotenv

load_dotenv()


class EncryptionError(Exception):
    pass


class DecryptionError(Exception):
    pass


def _get_fernet_key() -> bytes:
    key = os.getenv("FERNET_KEY")

    if not key:
        raise EncryptionError(
            "FERNET_KEY is missing from environment variables"
        )

    try:
        base64.urlsafe_b64decode(key)
    except Exception as exc:
        raise EncryptionError(
            "FERNET_KEY is invalid"
        ) from exc

    return key.encode()


def _get_cipher() -> Fernet:
    return Fernet(_get_fernet_key())


def generate_fernet_key() -> str:
    return Fernet.generate_key().decode()


def encrypt_text(plain_text: str) -> str:
    if not plain_text:
        raise EncryptionError(
            "Cannot encrypt empty text"
        )

    cipher = _get_cipher()

    encrypted = cipher.encrypt(
        plain_text.encode("utf-8")
    )

    return encrypted.decode("utf-8")


def decrypt_text(encrypted_text: str) -> str:
    if not encrypted_text:
        raise DecryptionError(
            "Encrypted text is empty"
        )

    cipher = _get_cipher()

    try:
        decrypted = cipher.decrypt(
            encrypted_text.encode("utf-8")
        )

        return decrypted.decode("utf-8")

    except InvalidToken as exc:
        raise DecryptionError(
            "Invalid encryption token or key"
        ) from exc


def encrypt_bytes(data: bytes) -> bytes:
    if not data:
        raise EncryptionError(
            "Cannot encrypt empty bytes"
        )

    cipher = _get_cipher()

    return cipher.encrypt(data)


def decrypt_bytes(data: bytes) -> bytes:
    if not data:
        raise DecryptionError(
            "Encrypted bytes are empty"
        )

    cipher = _get_cipher()

    try:
        return cipher.decrypt(data)

    except InvalidToken as exc:
        raise DecryptionError(
            "Invalid encrypted bytes"
        ) from exc


def encrypt_dict(data: dict) -> str:
    import json

    json_string = json.dumps(
        data,
        separators=(",", ":")
    )

    return encrypt_text(json_string)


def decrypt_dict(encrypted_text: str) -> dict:
    import json

    decrypted = decrypt_text(encrypted_text)

    return json.loads(decrypted)
