import psycopg2
from psycopg2.errors import DuplicateTable
from psycopg2.extras import execute_values
from configparser import ConfigParser
import typing


class Database:
    """
    A class used to connect to Postgres database

    This class requires that 'psycopg2' be installed.

    ...

    Attributes
    ----------
    db_params : str
        database parameters to establish a connection
    conn : connection
        database connection
    cur : cursor
        database cursor

    Methods
    -------
    create_table(sql)
        Creates database table
    insert_values(sql, data)
        Inserts data from python script to database table
    insert_by_select_from(sql)
        Inserts data from database table to database table
    select_all(sql, values=None)
        Selects data from database
    """

    def __init__(self, **db_params:str) -> None:
        """
        Parameters
        ----------
        db_params : str
            database parameters to establish a connection
        conn : connection
            database connection
        cur : cursor
            database cursor
        """

        self.conn = psycopg2.connect(**db_params)
        self.cur = self.conn.cursor()

    def __del__(self) -> None:
        """Rollbacks and cleans connection when references to the object have been deleted"""

        self.conn.rollback()
        self.cur.close()
        self.conn.close()

    def create_table(self, sql:str) -> None:
        """Executes SQL statement for creating or dropping table

        Parameters
        ----------
        sql: str
            SQL statement for creating or dropping table
        """

        try:
            self.cur.execute(sql)
            self.conn.commit()
        except DuplicateTable:
            self.conn.rollback()
        except Exception:
            self.conn.rollback()

    def insert_values(self, sql:str, data:list[tuple]) -> None:
        """Inserts data from python script to database table

        Parameters
        ----------
        sql: str
            SQL 'INSERT INTO ... VALUES ...' statement
        data: list[tuple]
            List of tuples of values to insert into table 
        """

        try:
            execute_values(self.cur, sql, data)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
    
    def insert_by_select_from(self, sql:str) -> None:
        """Inserts data from database table to database table
        
        Parameters
        ----------
        sql: str
            SQL 'INSERT INTO ... SELECT * FROM ...' statement
        """

        try:
            self.cur.execute(sql)
            self.conn.commit()
        except Exception:
            self.conn.rollback()

    def select_all(self, sql:str, 
                values:typing.Dict[str,typing.Any]=None
                ) -> tuple[list[str], list[tuple]]:
        """Selects data from database

        Parameters
        ----------
        sql: str
            SQL 'SELECT * FROM ...' statement with query placeholders (%s)
        values: Dict[str, Any], optional
            Values inserted into query placeholders
            
        Returns
        -------
        tuple[list[str], list[tuple]]
            tuple[list[column names], list[tuple[query records]]]
        """

        self.cur.execute(sql, values)
        column_names:list[str] = [desc[0] for desc in self.cur.description]
        data:list[tuple] = self.cur.fetchall()
        return (column_names, data)


def db_config(filename:str, section:str) -> typing.Dict[str,str]:
    """Loads database parameters from .ini file

    Parameters
    ----------
    filename: str
        The filename of .ini file (if .ini is located in main folder) or path (otherwise)
    section:str
        Section name of the .ini file
        
    Returns
    -------
    dict
        Dictionary with database parameters
    """

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



