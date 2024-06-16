import pandas as pd
from collections import defaultdict


def get_unom_moek_meta(sql_engine):
    moek_df = pd.read_sql_query("""
        SELECT
            unom_building,
            heat_source,
            date_commissioning,
            number_tp,
            placement_type,
            heat_load_dhw_average,
            heat_load_dhw_actual,
            heat_load_building,
            heat_load_ventilation_building,
            is_dispatching
        FROM hak.schema_moek
    """, con=sql_engine)
    moek_df = moek_df[(moek_df.unom_building.notna()) & (moek_df.number_tp.notna())]
    moek_df.unom_building = moek_df.unom_building.astype(int)
    moek_df = moek_df.groupby('unom_building').first().reset_index()
    tp_connections_count = moek_df.number_tp.value_counts().to_dict()
    moek_df['tp_connections_count'] = moek_df.number_tp.apply(lambda x: tp_connections_count[x])
    moek_df.date_commissioning = moek_df.date_commissioning.apply(pd.to_datetime)

    unom_moek_meta = defaultdict(dict)
    for row in moek_df.itertuples():
        def add_if_not_null(unom, value, value_name):
            if (value is not None) and (not pd.isna(value)):
                unom_moek_meta[unom][value_name] = value

        unom = row.unom_building
        add_if_not_null(unom, row.heat_source, 'heat_source')
        add_if_not_null(unom, row.date_commissioning, 'date_commissioning')
        add_if_not_null(unom, row.placement_type, 'placement_type')
        add_if_not_null(unom, row.heat_load_dhw_average, 'heat_load_dhw_average')
        add_if_not_null(unom, row.heat_load_dhw_actual, 'heat_load_dhw_actual')
        add_if_not_null(unom, row.heat_load_building, 'heat_load_building')
        add_if_not_null(unom, row.heat_load_ventilation_building, 'heat_load_ventilation_building')
        add_if_not_null(unom, row.is_dispatching, 'is_dispatching')
        add_if_not_null(unom, row.tp_connections_count, 'tp_connections_count')

    unom_moek_meta = dict(unom_moek_meta)
    return unom_moek_meta


def add_moek_features(pool, unom_moek_meta):
    def get_if_exists_else(unom, name, else_val):
        if unom in unom_moek_meta:
            if name in unom_moek_meta[unom]:
                return unom_moek_meta[unom][name]
        return else_val

    def get_days_from_commissioning(row):
        unom = row.UNOM
        if unom in unom_moek_meta:
            if 'date_commissioning' in unom_moek_meta[unom]:
                return (row.prediction_date - unom_moek_meta[unom]['date_commissioning']).days
        return None

    pool['heat_source'] = pool.UNOM.apply(lambda unom: get_if_exists_else(unom, 'heat_source', 'unknown'))
    pool['days_from_commissioning'] = pool.apply(lambda row: get_days_from_commissioning(row), axis=1)
    pool['placement_type'] = pool.UNOM.apply(lambda unom: get_if_exists_else(unom, 'placement_type', 'unknown'))
    pool['heat_load_dhw_average'] = pool.UNOM.apply(
        lambda unom: get_if_exists_else(unom, 'heat_load_dhw_average', None))
    pool['heat_load_dhw_actual'] = pool.UNOM.apply(lambda unom: get_if_exists_else(unom, 'heat_load_dhw_actual', None))
    pool['heat_load_building'] = pool.UNOM.apply(lambda unom: get_if_exists_else(unom, 'heat_load_building', None))
    pool['heat_load_ventilation_building'] = pool.UNOM.apply(
        lambda unom: get_if_exists_else(unom, 'heat_load_ventilation_building', None))
    pool['is_dispatching'] = pool.UNOM.apply(lambda unom: get_if_exists_else(unom, 'is_dispatching', 'unknown'))
    pool['tp_connections_count'] = pool.UNOM.apply(lambda unom: get_if_exists_else(unom, 'tp_connections_count', None))
