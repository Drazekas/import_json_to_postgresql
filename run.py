from src.database import Database, db_config
from src.logging_tools import func_status
import ijson
import typing
from typing import Union

CHUNK_SIZE:int = 10000
DATA_PATH:str = 'data\cities.json'

@func_status
def create_db_structures(db:Database) -> None:
    create_statements: list[str] = [
        """CREATE TABLE states (
        id INTEGER NOT NULL,
        code VARCHAR(255),
        name VARCHAR(255),
        PRIMARY KEY (id)
        )""",
        """CREATE TABLE countries (
        id INTEGER NOT NULL,
        code VARCHAR(255),
        name VARCHAR(255),
        PRIMARY KEY (id)
        )""",
        """CREATE TABLE cities (
        id INTEGER NOT NULL,
        name VARCHAR(255),
        state_id INTEGER,
        country_id INTEGER,
        latitude VARCHAR(255),
        longitude VARCHAR(255),
        wikiDataId VARCHAR(255),
        PRIMARY KEY (id),
        FOREIGN KEY (state_id) REFERENCES states (id),
        FOREIGN KEY (country_id) REFERENCES countries (id)
        )""",
        """DROP TABLE IF EXISTS cities_tmp;
        """,
        """CREATE TABLE cities_tmp (
        id INTEGER NOT NULL,
        name VARCHAR(255),
        state_id INTEGER,
        country_id INTEGER,
        latitude VARCHAR(255),
        longitude VARCHAR(255),
        wikiDataId VARCHAR(255),
        PRIMARY KEY (id)
        )"""]
    for create_statement in create_statements:
        db.create_table(create_statement) 

@func_status
def insert_json_to_db(db:Database, json_path:str) -> None:
    insert_statements:typing.Dict[str, str] = {
        'cities': """INSERT INTO cities_tmp (id, name, state_id, country_id,
                                             latitude, longitude, wikiDataId) 
                    VALUES %s 
                    ON CONFLICT (id) DO NOTHING;
                    """,
        'states': """INSERT INTO states (id, code, name) 
                    VALUES %s 
                    ON CONFLICT (id) DO NOTHING;
                    """,
        'countries': """INSERT INTO countries (id, code, name) 
                    VALUES %s 
                    ON CONFLICT (id) DO NOTHING;
                    """}
    with open(json_path, 'rb') as f:
        import_data:typing.Dict[str, Union[set[tuple], list[tuple]]] = {
                        'states':set(), 'countries':set(), 'cities':list()
                        }
        for record in ijson.items(f, 'item'):
            import_data['states'].add((record['state_id'], 
                                    record['state_code'], 
                                    record['state_name']))
            import_data['countries'].add((record['country_id'], 
                                        record['country_code'], 
                                        record['country_name']))
            import_data['cities'].append((record['id'], 
                                        record['name'], 
                                        record['state_id'], 
                                        record['country_id'], 
                                        record['latitude'], 
                                        record['longitude'], 
                                        record['wikiDataId']))
            if len(import_data['cities'])==CHUNK_SIZE:
                db.insert_values(insert_statements['cities'], 
                                import_data['cities'])
                import_data['cities'] = list()
            if len(import_data['states'])==CHUNK_SIZE:
                db.insert_values(insert_statements['states'], 
                                import_data['states'])
                import_data['states'] = set()
            if len(import_data['countries'])==CHUNK_SIZE:
                db.insert_values(insert_statements['countries'], 
                                import_data['countries'])
                import_data['countries'] = set()
        if len(import_data['cities']):
            db.insert_values(insert_statements['cities'], 
                            import_data['cities'])
        if len(import_data['states']):
            db.insert_values(insert_statements['states'], 
                            import_data['states'])
        if len(import_data['countries']):
            db.insert_values(insert_statements['countries'], 
                            import_data['countries'])

@func_status
def move_data_from_tmp_table(db:Database) -> None:
    db.insert_by_select_from("""INSERT INTO cities 
                                    SELECT * FROM cities_tmp 
                                    ON CONFLICT (id) DO NOTHING;""")

@func_status
def import_data_to_db(db:Database, data_path:str) -> None:
    create_db_structures(db)
    insert_json_to_db(db, data_path)
    move_data_from_tmp_table(db)

@func_status
def select_number_of_cities(db:Database, country_name:str, 
                        state_name:str
                        ) -> tuple[list[str], list[tuple]]:
    statement:str = """SELECT COUNT(ct.id) AS number_of_cities
                    FROM cities ct 
                    JOIN states s on ct.state_id = s.id 
                    JOIN countries ctr on ct.country_id = ctr.id
                    WHERE ctr.name = %(country_name)s
                    AND s.name = %(state_name)s
                    ;"""
    values:typing.Dict[str,str] = {'country_name':country_name, 
                                'state_name': state_name}
    return db.select_all(statement, values)

@func_status
def import_to_csv(file_name:str, data:list[tuple]) -> None:
    import csv
    with open(f'{file_name}.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(data[0])
        writer.writerows(data[1])

@func_status
def number_of_cities(db:Database, *, country_name:str, 
                state_name:str) -> None:
    data:tuple[list[str], list[tuple]] = select_number_of_cities(db, 
                                                    country_name, state_name)
    import_to_csv(f'Number of cities for {state_name} in {country_name}', 
                data)
    print(f"""Number of cities for {state_name} in {country_name}: 
        {data[1][0][0]}""")

@func_status
def main() -> None:
    db_params:typing.Dict[str,str] = db_config(filename='database.ini', 
                                            section='postgresql')
    db = Database(**db_params) 
    import_data_to_db(db, DATA_PATH)
    number_of_cities(db, country_name='Poland', 
                state_name='Masovian Voivodeship')


if __name__ == '__main__':
    main()
