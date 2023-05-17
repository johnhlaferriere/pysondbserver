

from os.path import exists
from os import remove
from symbol import try_stmt
import uuid
from threading import Lock
import secrets
from base64 import urlsafe_b64encode as b64e, urlsafe_b64decode as b64d

from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


try:
    import ujson as json
except ImportError:
    import json as json

import uuid
from typing import List
from typing import Optional
from typing import Union
from typing import Dict

from pysondb.errors import MissingConfigError
from pysondb.errors import InvalidUserError
from os import getcwd
import zlib
from base64 import urlsafe_b64encode as b64e, urlsafe_b64decode as b64d

backend = default_backend()
iterations = 100_000
class Config:

    def __init__(self, filename: str) -> None:
        self._filename: str = filename
        self._lock = Lock()


        with self._lock:
            self._pwd = getcwd()
            if exists(self._filename):
                with open(self._filename, encoding="utf-8", mode="r") as f:
                    self._config: dict = json.load(f)
            else:
                raise (
                    MissingConfigError(
                        f"the config file :{self._filename} does not exist."
                    )
                )

    def obscure(self, data: bytes) -> bytes:
        return b64e(zlib.compress(data, 9))

    def unobscure(self, obscured: bytes) -> bytes:
        return zlib.decompress(b64d(obscured))

    def _save(self) -> None:
        with self._lock:
            with open(self._filename, encoding="utf-8", mode="w") as f:
                json.dump(self._config, f, indent=4)

    def get_config(self) -> Dict:
        return self._config

    def get_pwd(self):
        return self._pwd

    def add_db(self, db: str,user:str) -> bool:
        self._config['databases'].append({'name': db, 'filename': db + '.json'})
        for u in self._config['users']:
            if u == user:
                u['access'].append(db)
        self._save()
        return True

    def del_db(self, dbname: str) -> bool:
        dbs = self._config["databases"]
        for db in dbs:
            if db["name"] == dbname:
                remove(db["filename"])
                dbs.remove(db)
                for u in self._config["users"]:
                    try:
                        u["access"].remove(dbname)
                    except ValueError:
                        pass
                self._save()
                return True
        return False

    def exists(self, dbname: str) -> bool:
        for db in self._config:
            if db["name"] == dbname:
                return True
        return False

    def auth_user(self, data: object) -> Dict:
        upass = json.loads(self.unobscure(bytes(data[1:], "utf-8")))
        u = upass["u"]
        p = upass["p"]
        passwd = str(self.obscure(bytes(u + p + u, "utf-8")), "utf-8")
        for user in self._config["users"]:
            if user["user"] == u and user["passwd"] == passwd:
                _auth = user.copy()
                _auth["passwd"] = p
                _auth["key"] = str(
                    self.obscure(bytes(str(uuid.uuid4()) + u, "utf-8")), "utf-8"
                )
                return _auth
        raise InvalidUserError(f"User '{u}' does not exist or has an invalid password")

    def _derive_key(self,password: bytes, salt: bytes, iterations: int = iterations) -> bytes:
        """Derive a secret key from a given password and salt"""
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(), length=32, salt=salt,
            iterations=iterations, backend=backend)
        return b64e(kdf.derive(password))

    def password_encrypt(self,message: bytes, password: str, iterations: int = iterations) -> bytes:
        salt = secrets.token_bytes(16)
        key = self._derive_key(password.encode(), salt, iterations)
        return b64e(
            b'%b%b%b' % (
                salt,
                iterations.to_bytes(4, 'big'),
                b64d(Fernet(key).encrypt(message)),
            )
        )

    def password_decrypt(self,token: bytes, password: str) -> bytes:
        decoded = b64d(token)
        salt, iter, token = decoded[:16], decoded[16:20], b64e(decoded[20:])
        iterations = int.from_bytes(iter, 'big')
        key = self._derive_key(password.encode(), salt, iterations)
        return Fernet(key).decrypt(token)
