from sqlalchemy import Engine, text

def new_data(conn_oltp: Engine, conn_olap: Engine) -> bool:
    # Ultimo id en la tabla de hecho_servicios
    query_ultimo_id_olap = text('select max(id_servicio) from hecho_servicios;')

    # Ultimo id en la tabla de mensajeria_servicio del OLTP
    query_ultimo_id_oltp = text('select max(id) from mensajeria_servicio;')

    with conn_oltp.connect() as con:
        rs = con.execute(query_ultimo_id_oltp)
        ultimo_id_oltp = rs.fetchone()[0]
        if ultimo_id_oltp is None:
            return True
        print(f'Ultimo id en la tabla de mensajeria_servicio: {ultimo_id_oltp}')

    with conn_olap.connect() as con:
        rs = con.execute(query_ultimo_id_olap)
        ultimo_id_olap = rs.fetchone()[0]
        if ultimo_id_olap is None:
            print(f'No hay datos en la tabla de hecho_servicios')
            return True
        print(f'Ultimo id en la tabla de hecho_servicios: {ultimo_id_olap}')

    if ultimo_id_oltp > ultimo_id_olap:
        print(f'Hay nuevos datos en la tabla de mensajeria_servicio')
        return True
    else:
        print(f'No hay nuevos datos en la tabla de mensajeria_servicio')
        return False


def push_dimensions(co_sa, etl_conn):
    dim_ips = extract.extract_ips(co_sa)
    dim_persona = extract.extract_persona(co_sa)
    dim_medico = extract.extract_medico(co_sa)
    trans_servicio = extract.extract_trans_servicio(co_sa)
    dim_demo = extract.extract_demografia(co_sa)
    dim_diag = extract.extract_enfermedades(co_sa)
    dim_servicio = extract.extract_servicios(co_sa)

    # transform
    dim_ips = transform.transform_ips(dim_ips)
    dim_persona = transform.transform_persona(dim_persona)
    dim_medico = transform.transform_medico(dim_medico)
    trans_servicio = transform.transform_trans_servicio(trans_servicio)
    dim_fecha = transform.transform_fecha()

    dim_demo = transform.transform_demografia(dim_demo)
    dim_diag = transform.transform_enfermedades(dim_diag)

    load.load(dim_ips, etl_conn, 'dim_ips')
    load.load(dim_fecha, etl_conn, 'dim_fecha')
    load.load(dim_servicio, etl_conn, 'dim_servicio')
    load.load(dim_persona, etl_conn, 'dim_persona')
    load.load(dim_medico, etl_conn, 'dim_medico')
    load.load(trans_servicio, etl_conn, 'trans_servicio')
    load.load(dim_diag, etl_conn, 'dim_diag')
    load.load(dim_demo, etl_conn, 'dim_demografia')