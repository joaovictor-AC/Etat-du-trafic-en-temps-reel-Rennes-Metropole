rm -rf datalake 
cd producer
docker compose down -v
cd ..
docker compose down -v
./start-projet.sh