#%%
import datetime
from datetime import timedelta, date, datetime
from typing import Tuple, Any

import holidays
import numpy as np
import pandas as pd
from mlxtend.frequent_patterns import apriori
from mlxtend.preprocessing import TransactionEncoder
from pandas import DataFrame

def clean_hecho_servicios(hecho_servicios: DataFrame) -> DataFrame:
    """
    Limpia el DataFrame hecho_servicios analizando y eliminando registros con valores nulos
    en las llaves de dimensiones y columnas relacionadas con tiempo.
    
    Args:
        hecho_servicios: DataFrame con datos de servicios
        
    Returns:
        DataFrame: DataFrame hecho_servicios limpio
    """
    # Almacenar conteo inicial
    initial_count = len(hecho_servicios)
    print(f"=== ANÁLISIS DE LIMPIEZA DE HECHO_SERVICIOS ===")
    print(f"Número inicial de servicios: {initial_count}")
    print()
    
    # Paso 1: Analizar valores nulos en llaves de dimensiones
    dimension_columns = ['key_dim_cliente', 'key_dim_mensajero', 'key_dim_tiempo', 'key_dim_sede']
    
    print("=== ANÁLISIS DE VALORES NULOS EN DIMENSIONES ===")
    for col in dimension_columns:
        null_count = hecho_servicios[col].isnull().sum()
        null_percentage = (null_count / initial_count) * 100
        print(f"{col}: {null_count} servicios nulos ({null_percentage:.2f}%)")
    
    print()
    
    # Paso 2: Eliminar servicios con llaves de dimensión faltantes
    print("=== PRIMERA LIMPIEZA: ELIMINAR SERVICIOS CON DIMENSIONES FALTANTES ===")
    before_dim_clean = len(hecho_servicios)
    
    # Eliminar filas donde cualquier llave de dimensión sea nula
    hecho_servicios_clean = hecho_servicios.dropna(subset=dimension_columns)
    
    after_dim_clean = len(hecho_servicios_clean)
    removed_dim = before_dim_clean - after_dim_clean
    remaining_percentage = (after_dim_clean / initial_count) * 100
    
    print(f"Servicios eliminados por dimensiones faltantes: {removed_dim}")
    print(f"Servicios restantes después de primera limpieza: {after_dim_clean}")
    print(f"Porcentaje de cobertura: {remaining_percentage:.2f}%")
    print()
    
    # Paso 3: Analizar valores nulos en columnas de tiempo
    time_columns = ['tiempo_total_espera', 'tiempo_espera_inicial', 'tiempo_espera_asignado', 
                   'tiempo_espera_recogido', 'tiempo_espera_en_destino']
    
    print("=== ANÁLISIS DE VALORES NULOS EN COLUMNAS DE TIEMPO ===")
    for col in time_columns:
        null_count = hecho_servicios_clean[col].isnull().sum()
        null_percentage = (null_count / after_dim_clean) * 100
        print(f"{col}: {null_count} servicios nulos ({null_percentage:.2f}%)")
    
    print()
    
    # Paso 4: Eliminar servicios con tiempo total de espera faltante
    print("=== SEGUNDA LIMPIEZA: ELIMINAR SERVICIOS CON TIEMPO_TOTAL_ESPERA FALTANTE ===")
    before_time_clean = len(hecho_servicios_clean)
    
    # Eliminar filas donde tiempo_total_espera es nulo
    hecho_servicios_final = hecho_servicios_clean.dropna(subset=time_columns)
    
    after_time_clean = len(hecho_servicios_final)
    removed_time = before_time_clean - after_time_clean
    final_percentage = (after_time_clean / initial_count) * 100
    
    print(f"Servicios eliminados por tiempos de espera faltantes: {removed_time}")
    print(f"Servicios restantes después de segunda limpieza: {after_time_clean}")
    print(f"Porcentaje final de cobertura: {final_percentage:.2f}%")
    print()
    
    # Resumen
    print("=== RESUMEN DE LIMPIEZA ===")
    print(f"Total de servicios eliminados: {initial_count - after_time_clean}")
    print(f"Servicios finales: {after_time_clean}")
    print(f"Cobertura final: {final_percentage:.2f}%")
    print("=" * 50)
    
    return hecho_servicios_final


def transform_hecho_servicios(tablas: list[DataFrame]) -> DataFrame:
    servicio, cliente_usuario, estado_servicio, novedad_servicio, dim_tiempo, dim_sede, dim_cliente, dim_mensajero = tablas

    # Asigna el mensajero usando una estrategia de respaldo - si mensajero3_id está vacío, usa mensajero2_id,
    # si mensajero2_id está vacío usa mensajero_id. Toma el primer valor no nulo encontrado.
    servicio['mensajero_id'] = servicio[
        ['mensajero3_id', 'mensajero2_id', 'mensajero_id']
    ].bfill(axis=1).iloc[:, 0]

    servicio['fecha_solicitud_str'] = servicio['fecha_solicitud'].dt.strftime('%Y-%m-%d')
    servicio['hora_solicitud_str'] = servicio['hora_solicitud'].apply(lambda x: x.strftime('%H:%M:%S') if pd.notnull(x) else None)

    servicio['fecha_hora_solicitud'] = pd.to_datetime(servicio['fecha_solicitud_str'] + ' ' + servicio['hora_solicitud_str'])

    servicio['hora_solicitud'] = servicio['hora_solicitud'].apply(lambda x: x.hour if pd.notnull(x) else None)

    servicio = servicio[['id', 'cliente_id', 'mensajero_id', 'fecha_solicitud', 'hora_solicitud', 'usuario_id', 'fecha_hora_solicitud']]

    hecho_servicios = servicio.merge(cliente_usuario, left_on='usuario_id', right_on='id', how='left')[
        ['id_x', 'cliente_id_x', 'mensajero_id', 'fecha_solicitud', 'hora_solicitud', 'sede_id', 'fecha_hora_solicitud']
    ].rename(columns={'id_x': 'id', 'cliente_id_x': 'cliente_id'})

    hecho_servicios = hecho_servicios.merge(dim_tiempo, left_on=['fecha_solicitud', 'hora_solicitud'], right_on=['fecha', 'hora_dia'], how='left')[
        ['id', 'cliente_id', 'mensajero_id', 'fecha_solicitud', 'hora_solicitud', 'sede_id', 'key_dim_tiempo', 'fecha_hora_solicitud']
    ]

    hecho_servicios = hecho_servicios.merge(dim_sede, left_on='sede_id', right_on='id_sede', how='left')[
        ['id', 'cliente_id', 'mensajero_id', 'fecha_solicitud', 'hora_solicitud', 'sede_id', 'key_dim_tiempo', 'key_dim_sede', 'fecha_hora_solicitud']
    ]

    hecho_servicios = hecho_servicios.merge(dim_cliente, left_on='cliente_id', right_on='id_cliente', how='left')[
        ['id', 'cliente_id', 'mensajero_id', 'fecha_solicitud', 'hora_solicitud', 'sede_id',
         'key_dim_tiempo', 'key_dim_sede', 'key_dim_cliente', 'fecha_hora_solicitud']
    ]

    hecho_servicios = hecho_servicios.merge(dim_mensajero, left_on='mensajero_id', right_on='id_mensajero', how='left')[
        ['id', 'key_dim_tiempo', 'key_dim_sede', 'key_dim_cliente', 'key_dim_mensajero', 'fecha_hora_solicitud']
    ]

    # Renombrar columna id a id_servicio para coincidir con el esquema
    hecho_servicios = hecho_servicios.rename(columns={'id': 'id_servicio'}).drop_duplicates()
    hecho_servicios.set_index('id_servicio', inplace=True)

    # Calcular tiempos de servicio
    for estado_id in [1,2,4,5]:
        estado_servicio_id = estado_servicio[estado_servicio['estado_id'] == estado_id].copy()
        estado_servicio_id['fecha'] = estado_servicio_id['fecha'].dt.strftime('%Y-%m-%d')
        estado_servicio_id['hora'] = estado_servicio_id['hora'].apply(lambda x: x.strftime('%H:%M:%S') if pd.notnull(x) else None)
        estado_servicio_id['fecha_hora'] = pd.to_datetime(estado_servicio_id['fecha'] + ' ' + estado_servicio_id['hora'])
        estado_servicio_id = estado_servicio_id.groupby('servicio_id')['fecha_hora'].max().to_frame()
        hecho_servicios[f'estado_{estado_id}_fecha_hora'] = hecho_servicios.index.map(
            estado_servicio_id['fecha_hora']
        )

    # Inicializar las columnas faltantes con None
    # Calcular las diferencias de tiempo y convertir a timedelta de pandas
    hecho_servicios['tiempo_total_espera'] = (hecho_servicios['estado_5_fecha_hora'] - hecho_servicios['fecha_hora_solicitud']).apply(lambda x: None if pd.isna(x) else str(x))
    hecho_servicios['tiempo_espera_inicial'] = (hecho_servicios['estado_1_fecha_hora'] - hecho_servicios['fecha_hora_solicitud']).apply(lambda x: None if pd.isna(x) else str(x))
    hecho_servicios['tiempo_espera_asignado'] = (hecho_servicios['estado_2_fecha_hora'] - hecho_servicios['estado_1_fecha_hora']).apply(lambda x: None if pd.isna(x) else str(x))
    hecho_servicios['tiempo_espera_recogido'] = (hecho_servicios['estado_4_fecha_hora'] - hecho_servicios['estado_2_fecha_hora']).apply(lambda x: None if pd.isna(x) else str(x))
    hecho_servicios['tiempo_espera_en_destino'] = (hecho_servicios['estado_5_fecha_hora'] - hecho_servicios['estado_4_fecha_hora']).apply(lambda x: None if pd.isna(x) else str(x))
    
    # Calcular los tipos de novedades
    for tipo_novedad in [1,2]:
        novedad_servicio_tipo = novedad_servicio[novedad_servicio['tipo_novedad_id'] == tipo_novedad]
        novedad_servicio_tipo = novedad_servicio_tipo.groupby('servicio_id').size().to_frame(f'cantidad_novedades_tipo_{tipo_novedad}')
        hecho_servicios[f'cantidad_novedades_tipo_{tipo_novedad}'] = hecho_servicios.index.map(
            novedad_servicio_tipo[f'cantidad_novedades_tipo_{tipo_novedad}']
        ).fillna(0)

    hecho_servicios.reset_index(inplace=True)

    return clean_hecho_servicios(hecho_servicios[[
        'id_servicio',
        'key_dim_cliente',
        'key_dim_mensajero', 
        'key_dim_tiempo',
        'key_dim_sede',
        'tiempo_total_espera',
        'tiempo_espera_inicial',
        'tiempo_espera_asignado',
        'tiempo_espera_recogido', 
        'tiempo_espera_en_destino',
        'cantidad_novedades_tipo_1',
        'cantidad_novedades_tipo_2'
    ]])


def transform_tiempo(tablas: list[DataFrame]) -> DataFrame:

    # Obtener el DataFrame de servicio que contiene las fechas
    servicio = tablas[0]
    
    # Obtener la primera fecha y el último año de los datos
    first_date = servicio['fecha_solicitud'].min()
    last_year = servicio['fecha_solicitud'].max().year
    
    # Crear fecha final (31 de diciembre del último año)
    end_date = datetime(last_year, 12, 31).date()
    
    # Generar un rango de fechas diario desde la primera fecha hasta la fecha final
    date_range = pd.date_range(start=first_date, end=end_date, freq='D')
    
    # Crear DataFrame base con todas las fechas
    dim_tiempo = pd.DataFrame(date_range, columns=['fecha'])
    
    # Expandir para incluir las 24 horas de cada día
    dim_tiempo = dim_tiempo.loc[dim_tiempo.index.repeat(24)].reset_index(drop=True)
    
    # Agregar columna de hora (0-23) para cada fecha
    dim_tiempo['hora_dia'] = list(range(24)) * len(date_range)
    
    # Agregar columnas de día y mes
    dim_tiempo['dia_semana'] = dim_tiempo['fecha'].dt.day_name()
    dim_tiempo['mes'] = dim_tiempo['fecha'].dt.month_name()

    return dim_tiempo

def transform_sede(tablas: list[DataFrame]) -> DataFrame:
    sede, ciudad = tablas

    ciudad = ciudad[['ciudad_id', 'nombre']].rename(columns={'nombre': 'ciudad'})

    dim_sede = sede[['sede_id', 'nombre', 'ciudad_id']].rename(columns={'sede_id': 'id_sede', 'nombre': 'nombre_sede'})
    
    dim_sede = dim_sede.merge(
        ciudad, 
        left_on='ciudad_id', right_on='ciudad_id', 
        how='left'
    )[['id_sede', 'nombre_sede', 'ciudad']]
    return dim_sede

def transform_cliente(tablas: list[DataFrame]) -> DataFrame:
    cliente = tablas[0]
    dim_cliente = cliente[['cliente_id', 'nombre']].rename(columns={'cliente_id': 'id_cliente', 'nombre': 'nombre_cliente'})
    return dim_cliente

def transform_mensajero(tablas: list[DataFrame]) -> DataFrame:
    mensajero, user = tablas

    # Unir las tablas para obtener información del usuario
    mensajero_user = mensajero[['id', 'user_id']].merge(user[['id', 'first_name', 'last_name', 'username']], 
                                    left_on='user_id', 
                                    right_on='id', 
                                    how='inner')\
                             .rename(columns={'id_x': 'mensajero_id'})

    # Seleccionar y renombrar las columnas relevantes
    dim_mensajero = mensajero_user[['mensajero_id', 'first_name', 'last_name', 'username']]
    # Crear columna nombre_mensajero concatenando first_name y last_name
    dim_mensajero['nombre_mensajero'] = dim_mensajero['first_name'] + ' ' + dim_mensajero['last_name'] + ' (' + dim_mensajero['username'] + ')'
    dim_mensajero = dim_mensajero[['mensajero_id', 'nombre_mensajero']].rename(columns={'mensajero_id': 'id_mensajero'})
    # Eliminar duplicados
    dim_mensajero = dim_mensajero.drop_duplicates()

    return dim_mensajero



