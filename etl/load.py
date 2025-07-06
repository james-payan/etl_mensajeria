import pandas as pd
from pandas import DataFrame
from sqlalchemy.engine import Engine
from sqlalchemy import text
import yaml
from sqlalchemy.dialects.postgresql import insert

def load(table: DataFrame, etl_conn: Engine, tname, replace: bool = False):
    """

    :param table: table to load into the database
    :param etl_conn: sqlalchemy engine to connect to the database
    :param tname: table name to load into the database
    :param replace:  when true it deletes existing table data(rows)
    :return: void it just load the table to the database
    """
    # statement = insert(f'{table})
    # with etl_conn.connect() as conn:
    #     conn.execute(statement)
    if replace :
        print(f'reemplazando datos de la tabla {tname}')
        with etl_conn.connect() as conn:
            print(f'Eliminando datos de la tabla {tname}')
            conn.execute(text(f'Delete from {tname}'))
            conn.commit()
        print(f'insertando los nuevos datos de la tabla {tname}')
        table.to_sql(f'{tname}', etl_conn, if_exists='append', index=False)
    else :
        print(f'insertando datos de la tabla {tname}')
        table.to_sql(f'{tname}', etl_conn, if_exists='append', index=False)
