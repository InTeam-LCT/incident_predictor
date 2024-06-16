import pandas as pd
from enums import USAGE_PRIORITY_1, USAGE_PRIORITY_2, USAGE_PRIORITY_3
from btiFeatures import add_bti_features, get_unom_to_bti_meta
from mkdTechFeatures import add_mkd_tech_features, get_unom_to_mkd_tech_meta
from addressFeatures import add_address_features, get_unom_address_meta, get_distance_to_moscow_center
from openmeteoFeatures import add_openmeteo_features, get_openmeteo_meta
from moekFeatures import add_moek_features, get_unom_moek_meta
from featureEnums import FEATURES_COLS, CAT_FEATURES_COLS
from catboost import Pool
from collections import defaultdict


def predict_all(
        prediction_date,
        sql_engine,
        openmeteo,
        models,
):
    prediction_date = pd.to_datetime(prediction_date).date()
    bti_df = pd.read_sql_query('select * from hak."characteristic_structure"', con=sql_engine)
    bti_df = set_usage_priority_type(bti_df)
    unom_to_bti_meta = get_unom_to_bti_meta(bti_df)
    target_unoms = get_target_unoms(bti_df)

    unom_to_mkd_tech_meta = get_unom_to_mkd_tech_meta(sql_engine)
    unom_address_meta = get_unom_address_meta(sql_engine)
    openmeteo_meta = get_openmeteo_meta(openmeteo, prediction_date)
    unom_moek_meta = get_unom_moek_meta(sql_engine)

    pool = get_features(
        target_unoms,
        prediction_date,
        unom_to_bti_meta,
        unom_to_mkd_tech_meta,
        unom_address_meta,
        openmeteo_meta,
        unom_moek_meta
    )
    unom_to_incident_pred_count = predict(pool, models)

    prediction_df = write_predictions(
        unom_to_incident_pred_count,
        prediction_date,
        unom_to_bti_meta,
        unom_to_mkd_tech_meta,
        unom_address_meta,
        openmeteo_meta
    )
    prediction_json = str(prediction_df.to_dict('records'))\
        .replace('nan', 'null')\
        .replace('\'', '\"')

    return prediction_json


def set_usage_priority_type(bti_df):
    bti_df['usage_priority_type'] = bti_df.assignment_structure.apply(lambda x: 1 if x in USAGE_PRIORITY_1 else
                                                                                2 if x in USAGE_PRIORITY_2 else
                                                                                3 if x in USAGE_PRIORITY_3 else None)
    return bti_df


def get_target_unoms(bti_df):
    return set(bti_df[bti_df.usage_priority_type.notna()].unom)


def get_features(
        unoms,
        prediction_date,
        unom_to_bti_meta,
        unom_to_mkd_tech_meta,
        unom_to_address_meta,
        openmeteo_meta,
        unom_moek_meta
):
    pool = pd.DataFrame([{'prediction_date': pd.to_datetime(prediction_date), 'UNOM': unom} for unom in unoms])
    add_bti_features(pool, unom_to_bti_meta)
    add_mkd_tech_features(pool, unom_to_mkd_tech_meta)
    add_address_features(pool, unom_to_address_meta)
    add_openmeteo_features(pool, openmeteo_meta)
    add_moek_features(pool, unom_moek_meta)

    return pool


def predict(pool, models):
    pool = pool.set_index('UNOM')
    unoms = pool.index
    unom_to_incident_pred_count = defaultdict(int)
    cb_pool = Pool(data=pool[FEATURES_COLS], cat_features=CAT_FEATURES_COLS)
    for model in models:
        predictions = model.predict(cb_pool)
        for unom, prediction in zip(unoms, predictions):
            unom_to_incident_pred_count[unom] += prediction

    return dict(unom_to_incident_pred_count)


def add_prediction_meta_fields(
        prediction_df,
        unom_to_bti_meta,
        unom_to_mkd_tech_meta,
        unom_address_meta,
        openmeteo_meta
):
    def get_or_else(unom, source, key, else_val):
        if unom in source:
            if key in source[unom]:
                return source[unom][key]
        return else_val

    prediction_df['usage_priority_type'] = prediction_df.unom.apply(
        lambda unom: get_or_else(unom, unom_to_bti_meta, 'usage_priority_type', 4))
    prediction_df['square'] = prediction_df.unom.apply(
        lambda unom: get_or_else(unom, unom_to_bti_meta, 'square', None))
    prediction_df['n_flats'] = prediction_df.unom.apply(
        lambda unom: get_or_else(unom, unom_to_mkd_tech_meta, 'n_flats', None))
    prediction_df['material'] = prediction_df.unom\
        .apply(lambda unom: get_or_else(unom, unom_to_bti_meta, 'material', None))\
        .apply(lambda x: x.replace('\'', '').replace('\"', ''))
    prediction_df['assignment_structure'] = prediction_df.unom.apply(
        lambda unom: get_or_else(unom, unom_to_bti_meta, 'assignment_structure', None))
    prediction_df['distance_to_moscow_center'] = prediction_df.unom.apply(
        lambda unom: get_distance_to_moscow_center(unom, unom_address_meta))
    prediction_df['distance_to_moscow_center'] = prediction_df['distance_to_moscow_center'].apply(
        lambda dist: float(f'{dist:.1f}') if dist is not None else dist)
    prediction_df['temp_mean_day'] = float(f'{openmeteo_meta["temp_mean_day"]:.1f}')
    prediction_df['weather'] = openmeteo_meta['weather']


def write_predictions(
        unom_to_incident_pred_count,
        prediction_date,
        unom_to_bti_meta,
        unom_to_mkd_tech_meta,
        unom_address_meta,
        openmeteo_meta
):
    df = [{'unom': unom, 'prediction': prediction, 'prediction_date': str(prediction_date)}
          for unom, prediction in unom_to_incident_pred_count.items()]
    prediction_df = pd.DataFrame(df)
    add_prediction_meta_fields(
        prediction_df,
        unom_to_bti_meta,
        unom_to_mkd_tech_meta,
        unom_address_meta,
        openmeteo_meta
    )
    return prediction_df
