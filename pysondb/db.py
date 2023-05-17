
# import json


import uuid
from copy import deepcopy
from pathlib import Path
from threading import Lock
from typing import List
from typing import Optional
from typing import Union
from typing import Dict


try:
    import ujson as json
except ImportError:
    import json as json


from pysondb.db_types import DBSchemaType
from pysondb.db_types import IdGeneratorType
from pysondb.db_types import NewKeyValidTypes
from pysondb.db_types import SingleDataType
from pysondb.db_types import ReturnWithIdType
from pysondb.db_types import QueryType
from pysondb.errors import IdDoesNotExistError
from pysondb.errors import SchemaTypeError
from pysondb.errors import UnknownKeyError
from pysondb.errors import SectionNotFoundError
from pysondb.errors import SectionAlreadExistsError
from pysondb.errors import MalformedQueryError


class PysonDB:
    def __init__(
        self, filename: str, auto_update: bool = True, indent: int = 4
    ) -> None:
        self.filename = filename
        self.auto_update = auto_update
        self._au_memory: DBSchemaType = {"version": 2, "keys": {}}
        self.indent = indent
        self._id_generator = self._gen_id
        self.lock = Lock()

        self._gen_db_file()

    def _load_file(self) -> DBSchemaType:
        if self.auto_update:
            with open(self.filename, encoding="utf-8", mode="r") as f:
                return json.load(f)
        else:
            return deepcopy(self._au_memory)

    def _dump_file(self, data: DBSchemaType) -> None:
        if self.auto_update:
            with open(self.filename, encoding="utf-8", mode="w") as f:
                json.dump(data, f, indent=self.indent)
        else:
            self._au_memory = deepcopy(data)
        return None

    def _gen_db_file(self) -> None:
        if self.auto_update:
            if not Path(self.filename).is_file():
                self.lock.acquire()
                self._dump_file({"version": 2, "keys": {}})
                self.lock.release()

    def _gen_id(self) -> str:
        # generates a random 18 digit uuid
        return str(int(uuid.uuid4()))[:18]

    def force_load(self) -> None:
        """
        Used when the data from a file needs to be loaded when auto update is turned off.
        """
        if not self.auto_update:
            self.auto_update = True
            self._au_memory = self._load_file()
            self.auto_update = False

    def commit(self) -> None:
        if not self.auto_update:
            self.auto_update = True
            self._dump_file(self._au_memory)
            self.auto_update = False

    def set_id_generator(self, fn: IdGeneratorType) -> None:
        self._id_generator = fn

    def add(self, section: str, data: object, ignore: bool = False) -> Dict:
        if not isinstance(data, dict):
            raise TypeError(f"data must be of type dict and not {type(data)}")
        try:
            with self.lock:
                db_data = self._load_file()
                keys = db_data["keys"][section]
                if not isinstance(keys, list):
                    raise SchemaTypeError(
                        f"keys must of type 'list' and not {type(keys)}"
                    )
                if len(keys) == 0:
                    db_data["keys"][section] = sorted(list(data.keys()))
                else:
                    if not ignore and not sorted(keys) == sorted(data.keys()):
                        raise UnknownKeyError(
                            f"Unrecognized / missing key(s) {set(keys) ^ set(data.keys())}"
                            "(Either the key(s) does not exists in the DB or is missing in the given data)"
                        )
                _id = str(self._id_generator())
                if not isinstance(db_data[section], dict):
                    raise SchemaTypeError('data key in the db must be of type "dict"')

                db_data[section][_id] = data
                self._dump_file(db_data)
                return _id
        except KeyError:
            raise SectionNotFoundError(f"section: {section} must existing in database ")

    def add_many(
        self,
        section: str,
        data: object,
        json_response: bool = True,
        ignore: bool = False,
    ) -> Dict:  # Union[SingleDataType, None]:
        if not data:
            return None

        if not isinstance(data, list):
            raise TypeError(f'data must be of type "list" and not {type(data)}')

        if not all(isinstance(i, dict) for i in data):
            raise TypeError("all the new data in the data list must of type dict")
        try:
            with self.lock:
                # new_data: SingleDataType = {}
                new_ids = []
                db_data = self._load_file()
                # verify all the keys in all the dicts in the list are valid
                keys = db_data["keys"][section]
                if not keys:
                    db_data["keys"][section] = sorted(list(data[0].keys()))
                    keys = db_data["keys"][section]
                if not isinstance(keys, list):
                    raise SchemaTypeError(
                        f"keys must of type 'list' and not {type(keys)}"
                    )

                for d in data:
                    if not ignore and not sorted(keys) == sorted(d.keys()):
                        raise UnknownKeyError(
                            f"Unrecognized / missing key(s) {set(keys) ^ set(d.keys())}"
                            "(Either the key(s) does not exists in the DB or is missing in the given data)"
                        )

                if not isinstance(db_data[section], dict):
                    raise SchemaTypeError('data key in the db must be of type "dict"')

                for d in data:
                    _id = str(self._id_generator())
                    db_data[section][_id] = d
                    if json_response:
                        new_ids.append(_id)
                        # new_data[_id] = d
                self._dump_file(db_data)
                return new_ids if json_response else True
                # return  new_data if json_response else True
        except KeyError:
            raise SectionNotFoundError(f"section: {section} must existing in database ")

    def get_all(self) -> Dict:  # ReturnWithIdType:
        with self.lock:
            data = self._load_file()
            if isinstance(data, dict):
                data.pop("version")
                data.pop("keys")
                return data
        return ""

    def get_all_by_section(self, section: str) -> Dict:
        try:
            with self.lock:
                data = self._load_file()[section]
                if isinstance(data, dict):
                    return data
            return ""
        except KeyError:
            raise SectionNotFoundError(f"section: {section} must existing in database ")

    def get_by_id(self, section: str, id: str) -> Dict:  # SingleDataType:
        if not isinstance(id, str):
            raise TypeError(f'id must be of type "str" and not {type(id)}')
        try:
            with self.lock:
                data = self._load_file()[section]
                if isinstance(data, dict):
                    if id in data:
                        return data[id]
                    else:
                        raise IdDoesNotExistError(f"{id!r} does not exists in the DB")
                else:
                    raise SchemaTypeError('"data" key in the DB must be of type dict')
        except KeyError:
            raise SectionNotFoundError(f"section: {section} must existing in database ")

    def get_by_query(self, section: str, query: QueryType) -> Dict:  # ReturnWithIdType:
        try:
            _query = eval(query)
        except Exception:
            raise MalformedQueryError(f"Query {query} is malformed.")
        if not callable(_query):
            raise TypeError(f'"query" must be a callable and not {type(query)!r}')
        try:
            with self.lock:
                new_data: ReturnWithIdType = {}
                data = self._load_file()[section]
                if isinstance(data, dict):
                    for id, values in data.items():
                        if isinstance(values, dict):
                            if _query(values):
                                new_data[id] = values
                return new_data
        except KeyError:
            raise SectionNotFoundError(f"section: {section} must existing in database ")

    def update_by_id(
        self, section: str, id: str, new_data: object
    ) -> Dict:  # SingleDataType:
        if not isinstance(new_data, dict):
            raise TypeError(f"new_data must be of type dict and not {type(new_data)!r}")
        try:
            with self.lock:
                data = self._load_file()
                keys = data["keys"][section]

                if isinstance(keys, list):
                    if not all(i in keys for i in new_data):
                        raise UnknownKeyError(
                            f"Unrecognized key(s) {[i for i in new_data if i not in keys]}"
                        )

                if not isinstance(data[section], dict):
                    raise SchemaTypeError(
                        "the value for the data keys in the DB must be of type dict"
                    )

                if id not in data[section]:
                    raise IdDoesNotExistError(
                        f"The id {id!r} does noe exists in the DB"
                    )

                data[section][id] = {**data[section][id], **new_data}
                self._dump_file(data)
                return data[section][id]
        except KeyError:
            raise SectionNotFoundError(f"section: {section} must existing in database ")

    def update_by_query(
        self, section: str, query: QueryType, new_data: object
    ) -> Dict:  # List[str]:
        try:
            _query = eval(query)
        except Exception:
            raise MalformedQueryError(f"Query {query} is malformed.")
        if not callable(_query):
            raise TypeError(f'"query" must be a callable and not {type(query)!r}')

        if not isinstance(new_data, dict):
            raise TypeError(
                f'"new_data" must be of type dict and not f{type(new_data)!r}'
            )
        try:
            with self.lock:
                updated_keys = []
                db_data = self._load_file()
                keys = db_data["keys"][section]

                if isinstance(keys, list):
                    if not all(i in keys for i in new_data):
                        raise UnknownKeyError(
                            f"Unrecognized / missing key(s) {[i for i in new_data if i not in keys]}"
                        )

                if not isinstance(db_data[section], dict):
                    raise SchemaTypeError("The data key in the DB must be of type dict")

                for key, value in db_data[section].items():
                    if _query(value):
                        db_data[section][key] = {**db_data[section][key], **new_data}
                        updated_keys.append(key)

                self._dump_file(db_data)
                return updated_keys
        except KeyError:
            raise SectionNotFoundError(f"section: {section} must existing in database ")

    def delete_by_id(self, section: str, id: str) -> Dict:  # None:
        try:
            with self.lock:
                data = self._load_file()
                if not isinstance(data[section], dict):
                    raise SchemaTypeError('"data" key in the DB must be of type dict')
                if id not in data[section]:
                    raise IdDoesNotExistError(f"ID {id} does not exists in the DB")
                del data[section][id]
                self._dump_file(data)
                return {}
        except KeyError:
            raise SectionNotFoundError(f"section: {section} must existing in database ")

    def delete_by_query(self, section: str, query: QueryType) -> List[str]:
        try:
            _query = eval(query)
        except Exception:
            raise MalformedQueryError(f"Query {query} is malformed.")
        if not callable(_query):
            raise TypeError(f'"query" must be a callable and not {type(query)!r}')
        try:
            with self.lock:
                data = self._load_file()
                if not isinstance(data[section], dict):
                    raise SchemaTypeError('"data" key in the DB must be of type dict')
                ids_to_delete = []
                for id, value in data[section].items():
                    if _query(value):
                        ids_to_delete.append(id)
                for id in ids_to_delete:
                    del data[section][id]
                self._dump_file(data)
                return ids_to_delete
        except KeyError:
            raise SectionNotFoundError(f"section: {section} must existing in database ")

    def purge(self, section: str) -> Dict:
        try:
            with self.lock:
                data = self._load_file()
                if not isinstance(data[section], dict):
                    raise SchemaTypeError('"data" key in the DB must be of type dict')
                if not isinstance(data["keys"][section], list):
                    raise SchemaTypeError('"key" key in the DB must be of type dict')
                data[section] = {}
                data["keys"][section] = []
                self._dump_file(data)
                return {}
        except KeyError:
            raise SectionNotFoundError(f"section: {section} must existing in database ")

    def purge_all(self):
        pass

    def add_new_key(
        self, section: str, key: str, default: Optional[NewKeyValidTypes] = None
    ) -> Dict:
        if default is not None:
            if not isinstance(default, (list, str, int, bool, dict)):
                raise TypeError(
                    f"default field must be of any of (list, int, str, bool, dict) but for {type(default)}"
                )
        try:
            with self.lock:
                data = self._load_file()
                if isinstance(data["keys"][section], list):
                    data["keys"][section].append(key)
                    data["keys"][section].sort()

                if isinstance(data[section], dict):
                    for d in data[section].values():
                        d[key] = default
                self._dump_file(data)
                return {}
        except KeyError:
            raise SectionNotFoundError(f"section: {section} must exist in database ")

    def add_section(self, section: str) -> str:
        with self.lock:
            data = self._load_file()
            if section in data["keys"]:
                raise SectionAlreadExistsError(
                    f"section: {section} alreay exists in the database"
                )
            data["keys"][section] = []
            data[section] = {}
            self._dump_file(data)
            return section
