import psycopg2
from psycopg2.errors import DuplicateTable
from psycopg2.extras import execute_values
from configparser import ConfigParser
import typing


class Database:
    def __init__(self, **db_params:str) -> None:
        self.conn = psycopg2.connect(**db_params)
        self.cur = self.conn.cursor()

    def __del__(self) -> None:
        self.conn.rollback()
        self.cur.close()
        self.conn.close()

    def create_table(self, sql:str) -> None:
        try:
            self.cur.execute(sql)
            self.conn.commit()
        except DuplicateTable:
            self.conn.rollback()
        except Exception:
            self.conn.rollback()

    def insert_values(self, sql:str, data:list[tuple]) -> None:
        try:
            execute_values(self.cur, sql, data)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
    
    def insert_by_select_from(self, sql:str) -> None:
        try:
            self.cur.execute(sql)
            self.conn.commit()
        except Exception:
            self.conn.rollback()

    def select_all(self, sql:str, 
                values:typing.Dict[str,typing.Any]=None
                ) -> tuple[list[str], list[tuple]]:
        self.cur.execute(sql, values)
        column_names:list[str] = [desc[0] for desc in self.cur.description]
        data:list[tuple] = self.cur.fetchall()
        return (column_names, data)


def db_config(filename:str, section:str) -> typing.Dict[str,str]:
    parser = ConfigParser()
    parser.read(filename)
    db:typing.Dict[str,str] = {}
    if parser.has_section(section):
        params:list[tuple[str,str]] = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')
    return db



