FROM python:3
RUN apt-get update -y && apt-get install -y build-essential
COPY . /incident_predictor_app
WORKDIR /incident_predictor_app
RUN pip install -r requirements.txt
ENV BACKEND_URL=http://158.160.170.236
ENV BACKEND_PORT=8080
ENV BD_HOST=10.131.0.12
ENV BD_PORT=5432
ENV BD_USER=postgres
ENV BD_PASS=6fg99sd6cx9m3
ENTRYPOINT ["python"]
CMD ["app.py"]