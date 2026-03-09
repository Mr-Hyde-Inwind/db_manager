from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List, Set, Dict
import sqlite3
from pathlib import Path
from enum import Enum

class DB_Type(Enum):
    SQLITE = 1

class DbManager(ABC):
    @staticmethod
    def _is_sqlite3(file_path: str|Path):
        with open(file_path, 'rb') as f:
            header = f.read(16)
            if header == b'SQLite format 3\x00':
                return True
            else:
                return False

    @classmethod
    def load(cls, db_path: str|Path) -> DbManager:
        if DbManager._is_sqlite3(db_path):
            return SqliteManager(db_path)
        else:
            raise ValueError("Unknown database type.")

    @abstractmethod
    def show_tables(self)->List[str]:
        pass

    @abstractmethod
    def show_table_structure(self, table_name: str)->str:
        pass

    @abstractmethod
    def start(self) -> DbManager:
        pass

    @abstractmethod
    def pick(self, *items: str) -> DbManager:
        pass

    @abstractmethod
    def from_table(self, target_table:str) -> DbManager:
        pass

    @abstractmethod
    def with_cond(self, cond: str) -> DbManager:
        pass

    @abstractmethod
    def do_get(self) -> List[Dict[str, object]]:
        pass

class SqliteManager(DbManager):
    def __init__(self, db_path: str|Path):
        self.db_path = db_path
        self.type = DB_Type.SQLITE
        self.connection = None
        self.cursor = None

        self.pick_item: Set[str] = set()
        self.conds: List[str] = []
        self.target_table: str = ""

    def execute(self, sql: str):
        if not self.connection:
            self.connection = sqlite3.connect(self.db_path)
        if not self.cursor:
            self.cursor = self.connection.cursor()

        self.cursor.execute(sql)


    def show_tables(self)->List[str]:
        SQL: str = "SELECT name FROM sqlite_master WHERE type='table';"
        self.execute(SQL)
        if not self.cursor:
            raise RuntimeError("SQL executed, but got None cursor.")
        return [str(item) for item, in self.cursor]

    def show_table_structure(self, table_name: str) -> str:
        SQL: str = f"PRAGMA table_info({table_name});"
        self.execute(SQL)
        if not self.cursor:
            raise RuntimeError("SQL executed, but got None cursor.")

        res: str = ""
        for row in self.cursor:
            if not res:
                res = "|".join(str(item) for item in row)
            else:
                res += f"\n{'|'.join(str(item) for item in row)}"

        return res

    def start(self) -> SqliteManager:
        self.pick_item = set()
        self.conds = []
        self.target_table = ""
        return self

    def pick(self, *items: str) -> SqliteManager:
        for item in items:
            self.pick_item.add(item)
        return self

    def from_table(self, target_table:str) -> SqliteManager:
        self.target_table = target_table
        return self

    def with_cond(self, cond: str) -> SqliteManager:
        self.conds.append(cond.strip())
        return self

    # return dict type
    def do_get(self) -> List[Dict[str, object]]:
        if not self.target_table:
            raise ValueError("No target table set.")
        if not self.pick_item:
            raise ValueError("No item trying to pick.")

        sql = f"SELECT {','.join(self.pick_item)} FROM {self.target_table}"
        conds_str = ""
        for cond in self.conds:
            if not conds_str:
                conds_str = f"WHERE {cond}"
            else:
                if cond.startswith("LIMIT") or cond.startswith("limit"):
                    conds_str += f"\n{cond}"
                else:
                    conds_str += f"\nAND {cond}"

        final_sql = f"{sql}\n{conds_str};"
        self.execute(final_sql)

        res: List[Dict[str, object]] = []
        if not self.cursor:
            raise RuntimeError("SQL executed, but got None cursor.")
        for item in self.cursor:
            res.append({k: v for k,v in zip(self.pick_item, item)})

        return res

