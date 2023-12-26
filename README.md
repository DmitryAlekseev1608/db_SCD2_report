python -m venv /home/oem/Desktop/db_SCD2_report/venv
source activate /home/oem/Desktop/db_SCD2_report/venv/bin/

pip install -r requirements.txt

sudo docker-compose up
http://localhost:8080
login airflow
password airflow
sudo docker-compose down --volumes --rmi all
fuser -k 8080/tcp

AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT: 600

docker ps
docker exec -it c2d969adde7a bash
docker exec -it c2d969adde7a sh