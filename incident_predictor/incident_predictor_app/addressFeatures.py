from collections import defaultdict
import pandas as pd
import numpy as np
import geopy.distance
from enums import MOSCOW_LAT, MOSCOW_LON


def get_unom_address_meta(sql_engine):
    address_df = pd.read_sql_query("""
        SELECT 
            unom,
            MIN(date_address_register) AS date_address_register,
            MIN(date_state_address_register) AS date_state_address_register,
            MIN(date_document_address_register) AS date_document_address_register,
            MIN(x) as x,
            MIN(y) as y
        FROM hak.address
        WHERE unom IN (
            SELECT
                unom
            FROM hak.characteristic_structure
        )
        GROUP BY unom
    """, con=sql_engine)

    unom_address_meta = defaultdict(dict)
    for row in address_df.itertuples():
        def add_if_not_null(unom, value, value_name):
            if (value is not None) and (not pd.isna(value)):
                unom_address_meta[unom][value_name] = pd.to_datetime(value)

        unom = row.unom
        add_if_not_null(unom, row.date_address_register, 'date_address_register')
        add_if_not_null(unom, row.date_state_address_register, 'date_state_address_register')
        add_if_not_null(unom, row.date_document_address_register, 'date_document_address_register')
        if (row.x is not None) and (row.y is not None):
            unom_address_meta[unom]['coords'] = (row.y, row.x)

    return dict(unom_address_meta)


def get_distance_to_moscow_center(unom, unom_address_meta):
    if unom in unom_address_meta:
        if 'coords' in unom_address_meta[unom]:
            return geopy.distance.geodesic((MOSCOW_LAT, MOSCOW_LON), unom_address_meta[unom]['coords']).km
    return None


def add_address_features(pool, unom_address_meta):
    def get_days_from_date(row, date_name):
        unom = row.UNOM
        if unom in unom_address_meta:
            if date_name in unom_address_meta[unom]:
                return (row.prediction_date - unom_address_meta[unom][date_name]).days
        return None

    def get_angle_to_moscow_center(unom):
        if unom in unom_address_meta:
            if 'coords' in unom_address_meta[unom]:
                lat, lon = unom_address_meta[unom]['coords']
                return 90 - np.arctan((lon - MOSCOW_LON) / (lat - MOSCOW_LAT))
        return None

    def get_lat_lon_dict(unom):
        if unom in unom_address_meta:
            if 'coords' in unom_address_meta[unom]:
                return {
                    'lat': unom_address_meta[unom]['coords'][0],
                    'lon': unom_address_meta[unom]['coords'][1]
                }
        return {}

    pool['days_from_date_address_register'] = pool.apply(lambda row: get_days_from_date(row, 'date_address_register'),
                                                         axis=1)
    pool['days_from_state_address_register'] = pool.apply(
        lambda row: get_days_from_date(row, 'date_state_address_register'), axis=1)
    pool['days_from_date_document_address_register'] = pool.apply(
        lambda row: get_days_from_date(row, 'date_document_address_register'), axis=1)
    pool['distance_to_moscow_center'] = pool.UNOM.apply(
        lambda unom: get_distance_to_moscow_center(unom, unom_address_meta))
    pool['lat_lon_dict'] = pool.UNOM.apply(get_lat_lon_dict)
    pool['angle_to_moscow_center'] = pool.UNOM.apply(get_angle_to_moscow_center)
    pool['lat'] = pool.lat_lon_dict.apply(lambda d: d['lat'] if 'lat' in d else None)
    pool['lon'] = pool.lat_lon_dict.apply(lambda d: d['lon'] if 'lon' in d else None)
