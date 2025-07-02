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
        tablas_mensajero = extract.extract(['mensajeria_servicio', 'clientes_mensajeroaquitoy', 'auth_user'], oltp_conn)

        print("transformando datos")
        dim_mensajero = transform.transform_mensajero(tablas_mensajero)
        print("total mensajeros: ", len(dim_mensajero))

        print("cargando datos en la base de datos OLAP")
        load.load(dim_mensajero, olap_conn, 'dim_mensajero', True)


else:
    print("no hay nuevos datos en la base de datos OLTP")


#         dim_ips = extract.extract_ips(oltp_conn)
#         dim_persona = extract.extract_persona(oltp_conn)
#         dim_medico = extract.extract_medico(oltp_conn)
#         trans_servicio = extract.extract_trans_servicio(oltp_conn)
#         dim_demo = extract.extract_demografia(oltp_conn)
#         dim_diag = extract.extract_enfermedades(oltp_conn)
#         dim_drug = extract.extract_medicamentos(config['medicamentos'])
#         dim_servicio = extract.extract_servicios(oltp_conn)


#         # transform
#         dim_ips = transform.transform_ips(dim_ips)
#         dim_persona = transform.transform_persona(dim_persona)
#         dim_medico = transform.transform_medico(dim_medico)
#         trans_servicio = transform.transform_trans_servicio(trans_servicio)
#         dim_fecha = transform.transform_fecha()
#         dim_demo = transform.transform_demografia(dim_demo)
#         dim_diag = transform.transform_enfermedades(dim_diag)



#         load.load(dim_ips, olap_conn, 'dim_ips', True)
#         load.load(dim_fecha, olap_conn, 'dim_fecha', True)
#         load.load(dim_servicio, olap_conn, 'dim_servicio', True)
#         load.load(dim_persona, olap_conn, 'dim_persona', True)
#         load.load(dim_medico, olap_conn, 'dim_medico', True)
#         load.load(trans_servicio, olap_conn, 'trans_servicio', True)
#         load.load(dim_diag, olap_conn, 'dim_diag', True)
#         load.load(dim_demo, olap_conn, 'dim_demografia', True)
#         load.load(dim_drug,olap_conn,'dim_medicamentos',True)


#     #hecho Atencion
#     hecho_atencion = extract.extract_hecho_atencion(olap_conn)
#     hecho_atencion = transform.transform_hecho_atencion(hecho_atencion)
#     load.load_hecho_atencion(hecho_atencion, olap_conn)
#     print('Done atencion fact')
#     # Hecho Entrega medicamentos
#     hecho_entrega = extract.extract_hecho_entrega(oltp_conn,olap_conn)
#     hecho_entrega, masrecetados = transform.transform_hecho_entrega(hecho_entrega)
#     load.load_hecho_entrega(hecho_entrega, olap_conn)
#     print('Done entrega fact')
#     # medicamentos que mas se recetan juntos
#     masrecetados = masrecetados.astype('string')
#     load.load(masrecetados,olap_conn, 'mas_recetados', False)
#     # Hecho retrios
#     hecho_retiros = extract.extract_retiros(oltp_conn,olap_conn)
#     hecho_retiros = transform.transform_hecho_retiros(hecho_retiros,1)
#     load.load(hecho_retiros, olap_conn, 'hecho_retiros', False)
#     print('Done retiros fact')

#     print('success all facts loaded')
# else:
#     print('done not new data')