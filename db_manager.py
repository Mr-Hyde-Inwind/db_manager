from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List
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

class SqliteManager(DbManager):
    def __init__(self, db_path: str|Path):
        self.db_path = db_path
        self.type = DB_Type.SQLITE
        self.connection = None
        self.cursor = None

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

