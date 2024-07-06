#etape1
mkdir -p ./dags ./logs ./plugins ./config /.script
echo -e "AIRFLOW_UID=$(id -u)" > .env
#etape2creation de network(une seul fois)
docker network create my_shared_network
#etape2(une seul fois)
docker-compose build
#etape3
docker compose up
