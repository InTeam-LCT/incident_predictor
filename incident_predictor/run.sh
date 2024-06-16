docker stop incident_predictor
docker rm -v incident_predictor
docker build -t artemveshkin/incident_predictor:latest incident_predictor_app
docker run --name incident_predictor --env BD_HOST=158.160.170.236 -d -p 5000:5000 artemveshkin/incident_predictor:latest
docker ps -a