docker stop incident_predictor
docker rm -v incident_predictor
docker build -t incident_predictor:latest incident_predictor_app
docker run --name incident_predictor -d -p 5000:5000 incident_predictor:latest
docker ps -a