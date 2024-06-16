FROM python:3
RUN apt-get update -y && apt-get install -y build-essential
COPY . /incident_predictor_app
WORKDIR /incident_predictor_app
RUN pip install -r requirements.txt
ENTRYPOINT ["python"]
CMD ["app.py"]