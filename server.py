#!/usr/bin/python           # This is server.py file


from typing import Dict
from typing import Type

from os.path import exists
from os import remove

try:
    import ujson as json
except ImportError:
    import json as json

import socketserver
import uuid

from pysondb.config import Config


from pysondb.errors import DatabaseNotFoundError
from pysondb.errors import DatabaseAlreadyExistsError
from pysondb.errors import SectionNotFoundError
from pysondb.errors import MalformedIdGeneratorError

from enum import Enum
from copy import deepcopy

# import pytest

from pysondb.db import PysonDB


RETVAL : Dict = {"error": "NoError", "data": ""}
config :Config = None


class ClientTCPHandler(socketserver.StreamRequestHandler):

    
    def __init__(self, request, client_address, server) -> None:
        self._commands = {
            "ADD": self.add,
            "ADD_MANY": self.add_many,
            "ADD_NEW_KEY": self.add_new_key,
            "ADD_SECTION": self.add_section,
            "CREATE_DB": self.create_db,
            "GET_ALL": self.get_all,
            "GET_ALL_BY_SECTION": self.get_all_by_section,
            "GET_BY_ID": self.get_by_id,
            "GET_BY_QUERY": self.get_by_query,
            "UPDATE_BY_ID": self.get_by_id,
            "UPDATE_BY_QUERY": self.update_by_query,
            "DELETE_BY_ID": self.delete_by_id,
            "DELETE_BY_QUERY": self.delete_by_query,
            "PURGE": self.purge,
            "PURGE_ALL": self.purge_all,
            "USE_DB": self.use_db,
            "USE_SECTION":self.use_section,
            "SET_ID_GENERATOR":self.set_id_generator,
        }

        self._db_list = {}
        for d in config.get_config()["databases"]:
            path = config.get_pwd()+"/"+config.get_config()["path"]+"/"+d["filename"]
            self._db_list[d["name"]] = {
                "filename": d["filename"],
                "handle": PysonDB(path, False),
            }
        self._db :Type[PysonDB] = None

        super().__init__(request, client_address, server)

    def _process_error(self, e):
        rval = {}
        rval["error"] = e.__class__.__name__
        rval["data"] = e.message
        return rval


    def use_db(self, data: Dict):
        retval = RETVAL.copy()
        dbname = data['dbname']
        section = data['section']
        try:
            if not dbname in self._db_list:
                raise DatabaseNotFoundError(f"database : {dbname} not found.")
            self._db = self._db_list[dbname]['handle']
            self._db.force_load()
            retval['data'] = {'dbname':dbname}
            if section != None:
                sec_retval = self.use_section({'section':section})
                if sec_retval['error'] == RETVAL['error']:
                    retval['data']['section'] = sec_retval['data']
                else:
                    retval = sec_retval
            return retval
        except Exception as e:
            return self._process_error(e)


    def use_section(self,data:Dict):
        retval = RETVAL.copy()
        section = data['section']
        try:
            if not section in self._db._load_file():
                raise SectionNotFoundError(
                    f"Section { section} not found."
                    )
            retval['data'] = section
            return retval
        except Exception as e:
            return self._process_error(e)


    def create_db(self, data: Dict):
        retval = RETVAL.copy()
        dbname = data["dbname"]
        filename = f"{dbname}.json"
        force = data["force"]
        path = config.get_pwd()+"/"+config.getconfig()["path"]+"/"+filename
        try:
            if not force:
                if dbname in self._db_list or exists(filename):
                    raise DatabaseAlreadyExistsError(
                        f"database {dbname} already exists"
                    )
            else:
                if exists(path):
                    remove(path)
            newdb = PysonDB(path)
            del newdb
            config.add_db(dbname)
            self._db_list[dbname] = {
                "filename": filename,
                "handle": PysonDB(path, False),
            }
            if data["use"]:
                self._db = self._db_list[dbname]["handle"]
                self._db.force_load()
            return retval
        except Exception as e:
            return self._process_error(e)


    def add(self, data: Dict) -> Dict:
        retval = RETVAL.copy()
        try:
            retval["data"] = self._db.add(data["section"], data["data"])
            self._db.commit()
            return retval
        except Exception as e:
            return self._process_error(e)


    def add_many(self, data: Dict) ->Dict:
        retval = RETVAL.copy()
        try:
            retval["data"] = self._db.add_many(
                data["section"], data["data"], data["json_response"]
            )
            self._db.commit()
            return retval
        except Exception as e:
            return self._process_error(e)


    def add_new_key(self, data: Dict)->Dict:
        retval = RETVAL.copy()
        try:
            retval["data"] = self._db.add_new_key(
                data["section"], data["key"], data["default"]
            )
            self._db.commit()
            return retval
        except Exception as e:
            return self._process_error(e)


    def add_section(self, data: Dict) ->Dict:
        retval = RETVAL.copy()
        try:
            retval["data"] = self._db.add_section(data["section"])
            self._db.commit()
            return retval
        except Exception as e:
            return self._process_error(e)


    def get_all(self, data: Dict) -> Dict:
        retval = RETVAL.copy()
        try:
            retval["data"] = self._db.get_all()
            return retval
        except Exception as e:
            return self._process_error(e)


    def get_all_by_section(self, data: Dict) -> Dict:
        retval = RETVAL.copy()
        try:
            retval["data"] = self._db.get_all_by_section(data["section"])
            return retval
        except Exception as e:
            return self._process_error(e)


    def get_by_id(self, data: Dict) -> Dict:
        retval = RETVAL.copy()
        try:
            retval["data"] = self._db.get_by_id(data["section"], data["id"])
        except Exception as e:
            return self._process_error(e)


    def get_by_query(self, data: Dict) -> Dict:
        retval = RETVAL.copy()
        try:
            retval['data'] = self._db.get_by_query(data["section"], data["query"])
            return retval
        except Exception as e:
            return self._process_error(e)


    def update_by_id(self, data: Dict) ->Dict:
        retval = RETVAL.copy()
        try:
            retval = self._db.update_by_id(data["section"], data["id"], data["data"])
            self._db.commit()
            return retval
        except Exception as e:
            return self._process_error(e)


    def update_by_query(self, data: Dict) ->Dict:
        retval = RETVAL.copy()
        try:
            retval['data'] = self._db.update_by_query(
                data["section"], data["query"], data["data"]
            )
            self._db.commit()
            return retval
        except Exception as e:
            return self._process_error(e)


    def delete_by_id(self, data: Dict) ->Dict:
        retval = RETVAL.copy()
        try:
            retval = self._db.delete_by_id(data["section"], data["id"])
            self._db.commit()
            return retval
        except Exception as e:
            return self._process_error(e)


    def delete_by_query(self, data: Dict) ->Dict:
        retval = RETVAL.copy()
        try:
            retval['data'] = self._db.delete_by_query(data["section"], data["query"])
            self._db.commit()
            return retval
        except Exception as e:
            return self._process_error(e)


    def purge(self, data: Dict) -> Dict:
        retval = RETVAL.copy()
        try:
            retval = self._db.purge(data["section"])
            self._db.commit()
            return retval
        except Exception as e:
            return self._process_error(e)


    def purge_all(self, data: Dict) -> Dict:
        retval = RETVAL.copy()
        try:
            retval = self._db.purge_all()
            self._db.commit()
            return retval
        except Exception as e:
            return self._process_error(e)


    def set_id_generator(self,data: Dict) ->Dict:
        retval = RETVAL.copy()
        fn = data["fn"]
        try:
            try:
                _fn = eval(fn)
            except:
                raise MalformedIdGeneratorError(
                    f"Function {fn} is malformed.")
            if not callable(_fn):
                raise TypeError(
                    f'"Function" must be a callable and not {type(fn)!r}')
            self._db.set_id_generator(_fn)
            return retval
        except Exception as e:
            return self._process_error(e)


    def handle(self) ->None:
        while True:
            try:
                self.data = self.rfile.readline().strip()
                if len(self.data) <= 0:
                    return
                print("{} wrote:".format(self.client_address[0]))
                print(self.data)
                d = json.loads(self.data)
                retval = json.dumps(self._commands.get(d["cmd"])(d["payload"]))
                self.wfile.write(len(retval).to_bytes(8, "big"))
                self.wfile.write(bytes(retval, encoding="utf8"))
            except:
                return


def main() -> int:
    print ("pysondb server starting")
    global config
    config = Config("config.json")
    print ("config loaded")
    c = config.get_config()
    print (f"execuition path : {config.get_pwd()}")
    HOST, PORT = c["host"], c["port"]
    server = socketserver.ThreadingTCPServer((HOST, PORT), ClientTCPHandler)
    print (f"server started on {HOST}:{PORT}")
    print("Available databases:")
    for f in c["databases"]:
        print (f"\t{f['name']}")
    print ("server accepting requests")
    server.serve_forever()
    return 0

if __name__ == "__main__":
    main()
