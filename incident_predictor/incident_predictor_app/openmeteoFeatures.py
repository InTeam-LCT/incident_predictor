import pandas as pd
from enums import WEATHER_CODE_MAPPING, MOSCOW_LAT, MOSCOW_LON


def get_openmeteo_meta(openmeteo, prediction_date):
    openmeteo_params = {
        "latitude": MOSCOW_LAT,
        "longitude": MOSCOW_LON,
        "start_date": f"{str((pd.to_datetime(prediction_date) - pd.Timedelta(7, unit='d')).date())}",
        "end_date": f"{prediction_date}",
        "hourly": ["temperature_2m", "weather_code"]
    }
    openmeteo_response = openmeteo.weather_api('https://api.open-meteo.com/v1/forecast', params=openmeteo_params)[0]

    hourly = openmeteo_response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_weather_code = hourly.Variables(1).ValuesAsNumpy()

    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ),
        "temperature_2m": hourly_temperature_2m,
        "weather_code": hourly_weather_code
    }

    hourly_df = pd.DataFrame(data=hourly_data)
    hourly_df.date = hourly_df.date.dt.date

    openmeteo_meta = {}
    date = pd.to_datetime(prediction_date).date()
    temp_7_days_around = hourly_df[(hourly_df.date - date).isin(pd.timedelta_range('-7 day', periods=8))].temperature_2m
    temp_3_days_around = hourly_df[(hourly_df.date - date).isin(pd.timedelta_range('-3 day', periods=4))].temperature_2m
    temp_day = hourly_df[hourly_df.date == date].temperature_2m
    weather_code = hourly_df[hourly_df.date == date].weather_code
    max_weather_code = weather_code.apply(lambda code: code if code in WEATHER_CODE_MAPPING else -1).max()
    weather = WEATHER_CODE_MAPPING[max_weather_code] \
        if max_weather_code in WEATHER_CODE_MAPPING else WEATHER_CODE_MAPPING[0]

    temp_min_7_days = temp_7_days_around.min()
    temp_max_7_days = temp_7_days_around.max()
    temp_var_7_days = temp_max_7_days - temp_min_7_days
    temp_mean_7_days = temp_7_days_around.mean()
    temp_median_7_days = temp_7_days_around.median()
    temp_shift_7_days = temp_7_days_around.iloc[-1] - temp_7_days_around.iloc[0]

    temp_min_3_days = temp_3_days_around.min()
    temp_max_3_days = temp_3_days_around.max()
    temp_var_3_days = temp_max_3_days - temp_min_3_days
    temp_mean_3_days = temp_3_days_around.mean()
    temp_median_3_days = temp_3_days_around.median()
    temp_shift_3_days = temp_3_days_around.iloc[-1] - temp_3_days_around.iloc[0]

    temp_min_day = temp_day.min()
    temp_max_day = temp_day.max()
    temp_var_day = temp_max_day - temp_min_day
    temp_mean_day = temp_day.mean()
    temp_median_day = temp_day.median()
    temp_shift_day = temp_day.iloc[-1] - temp_day.iloc[0]

    openmeteo_meta['weather'] = weather
    openmeteo_meta['temp_min_7_days'] = temp_min_7_days
    openmeteo_meta['temp_max_7_days'] = temp_max_7_days
    openmeteo_meta['temp_var_7_days'] = temp_var_7_days
    openmeteo_meta['temp_mean_7_days'] = temp_mean_7_days
    openmeteo_meta['temp_median_7_days'] = temp_median_7_days
    openmeteo_meta['temp_shift_7_days'] = temp_shift_7_days

    openmeteo_meta['temp_min_3_days'] = temp_min_3_days
    openmeteo_meta['temp_max_3_days'] = temp_max_3_days
    openmeteo_meta['temp_var_3_days'] = temp_var_3_days
    openmeteo_meta['temp_mean_3_days'] = temp_mean_3_days
    openmeteo_meta['temp_median_3_days'] = temp_median_3_days
    openmeteo_meta['temp_shift_3_days'] = temp_shift_3_days

    openmeteo_meta['temp_min_day'] = temp_min_day
    openmeteo_meta['temp_max_day'] = temp_max_day
    openmeteo_meta['temp_var_day'] = temp_var_day
    openmeteo_meta['temp_mean_day'] = temp_mean_day
    openmeteo_meta['temp_median_day'] = temp_median_day
    openmeteo_meta['temp_shift_day'] = temp_shift_day

    return openmeteo_meta


def add_openmeteo_features(pool, openmeteo_meta):
    pool['temp_min_7_days'] = openmeteo_meta['temp_min_7_days']
    pool['temp_max_7_days'] = openmeteo_meta['temp_max_7_days']
    pool['temp_var_7_days'] = openmeteo_meta['temp_var_7_days']
    pool['temp_mean_7_days'] = openmeteo_meta['temp_mean_7_days']
    pool['temp_median_7_days'] = openmeteo_meta['temp_median_7_days']
    pool['temp_shift_7_days'] = openmeteo_meta['temp_shift_7_days']

    pool['temp_min_3_days'] = openmeteo_meta['temp_min_3_days']
    pool['temp_max_3_days'] = openmeteo_meta['temp_max_3_days']
    pool['temp_var_3_days'] = openmeteo_meta['temp_var_3_days']
    pool['temp_mean_3_days'] = openmeteo_meta['temp_mean_3_days']
    pool['temp_median_3_days'] = openmeteo_meta['temp_median_3_days']
    pool['temp_shift_3_days'] = openmeteo_meta['temp_shift_3_days']

    pool['temp_min_day'] = openmeteo_meta['temp_min_day']
    pool['temp_max_day'] = openmeteo_meta['temp_max_day']
    pool['temp_var_day'] = openmeteo_meta['temp_var_day']
    pool['temp_mean_day'] = openmeteo_meta['temp_mean_day']
    pool['temp_median_day'] = openmeteo_meta['temp_median_day']
    pool['temp_shift_day'] = openmeteo_meta['temp_shift_day']

    pool['weather'] = openmeteo_meta['weather']
