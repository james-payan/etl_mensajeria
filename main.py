import pandas as pd
import datetime
from datetime import date
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
import yaml
from etl import extract, transform, load, utils_etl
import psycopg2

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)


# ---- Carga de Configuración ----
# Cargar configuración de base de datos desde archivo YAML
with open('config.yml', 'r') as f:
    config = yaml.safe_load(f)
    config_oltp = config['MENSAJERIA_OLTP']  # Configuración base de datos OLTP
    config_olap = config['MENSAJERIA_OLAP']  # Configuración base de datos OLAP

# ---- Configuración de Conexiones a Bases de Datos ----
# Construir URLs de conexión para ambas bases de datos
url_oltp = (f"{config_oltp['drivername']}://{config_oltp['user']}:{config_oltp['password']}@{config_oltp['host']}:"
          f"{config_oltp['port']}/{config_oltp['dbname']}")
url_olap = (f"{config_olap['drivername']}://{config_olap['user']}:{config_olap['password']}@{config_olap['host']}:"
           f"{config_olap['port']}/{config_olap['dbname']}")

# Crear motores de conexión SQLAlchemy
oltp_conn = create_engine(url_oltp)
olap_conn = create_engine(url_olap)

# ---- Inicialización del Esquema de Base de Datos ----
# Verificar si existen las tablas en la base de datos OLAP
print("verificando si existen tablas en la base de datos OLAP")
inspector = inspect(olap_conn)
tnames = inspector.get_table_names()

# Si no existen tablas, crearlas desde los scripts SQL
if not tnames:
    print("no existen tablas en la base de datos OLAP, creando tablas")
    # Crear conexión directa para ejecutar scripts SQL
    conn = psycopg2.connect(dbname=config_olap['dbname'], user=config_olap['user'], password=config_olap['password'],
                            host=config_olap['host'], port=config_olap['port'])
    cur = conn.cursor()
    
    # Cargar y ejecutar scripts de creación SQL desde archivo YAML
    with open('sqlscripts.yml', 'r') as f:
        tablas_olap = yaml.safe_load(f)
        for key, val in tablas_olap.items():
            print("creando tabla: ", key)
            cur.execute(val)
            conn.commit()
    cur.close()
    conn.close()
else:
    print("Ya existen tablas en la base de datos OLAP: ", tnames)

# ---- Procesamiento de Datos ----
# Verificar si hay nuevos datos en la base de datos OLTP
if utils_etl.new_data(oltp_conn, olap_conn):
    print("hay nuevos datos en la base de datos OLTP")
    # ---- Carga de Dimensiones ----
    # Extraer datos de dimensiones de la base de datos OLTP
    if config['LOAD_DIMENSIONS']:
        print("Extraer datos de la base de datos OLTP")
        tablas_mensajero = extract.extract(['clientes_mensajeroaquitoy', 'auth_user'], oltp_conn)
        tablas_clientes = extract.extract(['cliente'], oltp_conn)
        tablas_sede = extract.extract(['sede', 'ciudad'], oltp_conn)
        tablas_tiempo = extract.extract(['mensajeria_servicio'], oltp_conn)

        print("transformando datos")
        dim_mensajero = transform.transform_mensajero(tablas_mensajero)
        print("total mensajeros: ", len(dim_mensajero))
        dim_cliente = transform.transform_cliente(tablas_clientes)
        print("total clientes: ", len(dim_cliente))
        dim_sede = transform.transform_sede(tablas_sede)
        print("total sedes: ", len(dim_sede))
        dim_tiempo = transform.transform_tiempo(tablas_tiempo)
        print("total tiempo: ", len(dim_tiempo))

        print("cargando datos en la base de datos OLAP")
        load.load(dim_mensajero, etl_conn=olap_conn, tname='dim_mensajero', replace=True)
        load.load(dim_cliente, etl_conn=olap_conn, tname='dim_cliente', replace=True)
        load.load(dim_sede, etl_conn=olap_conn, tname='dim_sede', replace=True)
        load.load(dim_tiempo, etl_conn=olap_conn, tname='dim_tiempo', replace=True)

    # ---- Carga de Hechos ----
    print("Extraer datos de la base de datos OLTP para el hecho de servicios")
    tablas_servicios_oltp = extract.extract(['mensajeria_servicio', 'clientes_usuarioaquitoy', 'mensajeria_estadosservicio', 'mensajeria_novedadesservicio'], oltp_conn)
    print("Extraer datos de la base de datos OLAP para las dimensiones")
    tablas_dimensiones_olap = extract.extract(['dim_tiempo', 'dim_sede', 'dim_cliente', 'dim_mensajero'], olap_conn)

    print("transformando datos para el hecho de servicios")
    hecho_servicios = transform.transform_hecho_servicios(tablas_servicios_oltp + tablas_dimensiones_olap)
    print("total servicios: ", len(hecho_servicios))
    #print(hecho_servicios.head(10))

    print("cargando datos en la base de datos OLAP para el hecho de servicios")
    load.load(hecho_servicios, etl_conn=olap_conn, tname='hecho_servicios', replace=False)


    print("Carga Satisfactoria hecho de servicios")

else:
    print("no hay nuevos datos en la base de datos OLTP")
