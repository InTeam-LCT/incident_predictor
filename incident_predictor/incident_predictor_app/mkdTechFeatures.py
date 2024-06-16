from collections import defaultdict
import pandas as pd


def get_unom_to_mkd_tech_meta(sql_engine):
    mkd_tech_df = pd.read_sql_query('select * from hak.characteristic', con=sql_engine)

    unom_to_mkd_tech_meta = defaultdict(dict)
    for row in mkd_tech_df.itertuples():
        def add_if_not_null(unom, value, value_name):
            if (value is not None) and (not pd.isna(value)):
                unom_to_mkd_tech_meta[unom][value_name] = value

        unom = row.unom
        add_if_not_null(unom, row.area, 'area')
        add_if_not_null(unom, row.col_759, 'mkd_tech_n_floors')
        add_if_not_null(unom, row.col_760, 'n_entrances')
        add_if_not_null(unom, row.col_761, 'n_flats')
        add_if_not_null(unom, row.col_762, 'mkd_tech_square')
        unom_to_mkd_tech_meta[unom]['mkd_tech_living_square'] = row.col_763
        if isinstance(row.col_764, int) or isinstance(row.col_764, str) or isinstance(row.col_764, float):
            unom_to_mkd_tech_meta[unom]['mkd_tech_nonliving_square'] = float(row.col_764)
        add_if_not_null(unom, row.col_766, 'wear_and_tear')
        add_if_not_null(unom, row.col_769, 'mkd_tech_wall_materials')
        add_if_not_null(unom, row.col_770, 'mkd_tech_is_emergency')
        add_if_not_null(unom, row.col_771, 'n_elevators')
        add_if_not_null(unom, row.col_772, 'n_big_elevators')
        add_if_not_null(unom, row.col_775, 'roof_cleaning_order')
        add_if_not_null(unom, row.col_781, 'roof_material')
        add_if_not_null(unom, row.col_2463, 'fond_type')
        add_if_not_null(unom, row.col_3163, 'mkd_status')
    return dict(unom_to_mkd_tech_meta)


def add_mkd_tech_features(pool, unom_to_mkd_tech_meta):
    def get_if_exists_else(unom, name, else_val, apply_f):
        if unom in unom_to_mkd_tech_meta:
            if name in unom_to_mkd_tech_meta[unom]:
                return apply_f(unom_to_mkd_tech_meta[unom][name])
        return else_val

    pool['area'] = pool.UNOM.apply(lambda unom: get_if_exists_else(unom, 'area', 'unknown', str))
    pool['mkd_tech_n_floors'] = pool.UNOM.apply(lambda unom: get_if_exists_else(unom, 'mkd_tech_n_floors', None, int))
    pool['n_entrances'] = pool.UNOM.apply(lambda unom: get_if_exists_else(unom, 'n_entrances', None, int))
    pool['n_flats'] = pool.UNOM.apply(lambda unom: get_if_exists_else(unom, 'n_flats', None, int))
    pool['mkd_tech_square'] = pool.UNOM.apply(lambda unom: get_if_exists_else(unom, 'mkd_tech_square', None, float))
    pool['mkd_tech_living_square'] = pool.UNOM.apply(
        lambda unom: get_if_exists_else(unom, 'mkd_tech_living_square', None, float))
    pool['mkd_tech_nonliving_square'] = pool.UNOM.apply(
        lambda unom: get_if_exists_else(unom, 'mkd_tech_nonliving_square', None, float))
    pool['wear_and_tear'] = pool.UNOM.apply(lambda unom: get_if_exists_else(unom, 'wear_and_tear', None, float))
    pool['mkd_tech_wall_materials'] = pool.UNOM.apply(
        lambda unom: get_if_exists_else(unom, 'mkd_tech_wall_materials', 'unknown', int))
    pool['mkd_tech_is_emergency'] = pool.UNOM.apply(
        lambda unom: get_if_exists_else(unom, 'mkd_tech_is_emergency', 'unknown', int))
    pool['n_elevators'] = pool.UNOM.apply(lambda unom: get_if_exists_else(unom, 'n_elevators', None, float))
    pool['n_big_elevators'] = pool.UNOM.apply(lambda unom: get_if_exists_else(unom, 'n_big_elevators', None, float))
    pool['roof_cleaning_order'] = pool.UNOM.apply(
        lambda unom: get_if_exists_else(unom, 'roof_cleaning_order', 'unknown', int))
    pool['roof_material'] = pool.UNOM.apply(lambda unom: get_if_exists_else(unom, 'roof_material', 'unknown', int))
    pool['fond_type'] = pool.UNOM.apply(lambda unom: get_if_exists_else(unom, 'fond_type', 'unknown', int))
    pool['mkd_status'] = pool.UNOM.apply(lambda unom: get_if_exists_else(unom, 'mkd_status', 'unknown', int))
