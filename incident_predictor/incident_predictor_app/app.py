from flask import Flask, request
from datetime import date
from sqlalchemy import create_engine
from catboost import CatBoostClassifier
import os
import predictAll
import requests
import openmeteo_requests
import requests_cache
from retry_requests import retry
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)
scheduler = BackgroundScheduler(daemon=True)


@app.route('/predict')
def predict():
    prediction_date = request.args.get('date', default=str(date.today()))
    return predictAll.predict_all(
        prediction_date,
        sql_engine,
        openmeteo,
        models,
    )


@app.route('/predict_for_today_and_save')
def predict_for_today_and_save():
    prediction_json = predict()
    requests.post(f'http://{backend_ip}:8080/v0/predications/save',
                  data=prediction_json,
                  headers={'content-type': 'application/json'})
    return prediction_json


def call_predict_for_today_and_save():
    with app.test_request_context('/'):
        predict_for_today_and_save()


@app.route('/ping')
def ping():
    return "pong"


scheduler.add_job(call_predict_for_today_and_save, 'interval', minutes=10)
scheduler.start()


if __name__ == '__main__':
    global backend_ip
    backend_ip = os.getenv('BACKEND_IP')
    global sql_engine
    sql_engine = create_engine(
        f"postgresql://postgres:6fg99sd6cx9m3@{backend_ip}:5432/"
    )
    global openmeteo
    openmeteo = openmeteo_requests.Client(
        session=retry(
            requests_cache.CachedSession('.cache', expire_after=3600),
            retries=5,
            backoff_factor=0.2
        )
    )

    global models
    models = []
    for model_path in os.listdir('models'):
        model = CatBoostClassifier()
        model.load_model(f'models/{model_path}')
        models.append(model)

    app.run(debug=False, host='0.0.0.0')
    call_predict_for_today_and_save()
