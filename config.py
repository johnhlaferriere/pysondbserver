
from os.path import exists
from os import remove

try:
    import ujson as json
except ImportError:
    import json as json

from typing import List
from typing import Optional
from typing import Union
from typing import Dict

from errors import MissingConfigError



class Config():

    @classmethod
    def __init__(self,filename:str) -> None:
        self._filename : str = filename
        if (exists(self._filename)):
            with open(self._filename, encoding="utf-8", mode="r") as f:
                self._config :dict = json.load(f)
        else:
            raise(MissingConfigError(
                f'the config file :{self._filename} does not exist.')
                )

    @classmethod
    def _save(self) -> None:
        with open(self._filename, encoding='utf-8', mode='w') as f:
            json.dump(self._config, f,indent=4)

    @classmethod
    def get_config(self) -> Dict:
        return self._config

    def add_db(self,db:str) -> bool:
        self._config["databases"].append({"name":db,"filename":db+".json"})
        self._save()
        return True

    @classmethod
    def del_db(self,dbname:str) -> bool:
        dbs = self._config["databases"]
        for db in dbs:
            if db["name"] == dbname:
                remove(db["filename"])
                dbs.remove(db)
                self._save()
                return True
        return False

    @classmethod
    def exists(self,dbname:str) -> bool:
        for db in self._config:
            if db["name"] == dbname:
                return True
        return False



    