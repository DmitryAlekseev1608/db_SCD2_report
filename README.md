python -m venv /home/oem/Desktop/Project_DE/venv
source activate /home/oem/Desktop/Project_DE/venv

pip install -r requirements.txt

sudo docker-compose up airflow-init
sudo docker-compose up
http://localhost:8080
login airflow
password airflow
sudo docker-compose down --volumes --rmi all