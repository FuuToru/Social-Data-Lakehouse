How to run

docker build -t spark-base:latest /docker/spark/spark-base

docker compose up -d --build

access localhost:8080: admin pass: admin

chose Admin -> conections -> setup to connect spark -> 

Connection id: spark-conn
Connection type: Spark
host: spark-master
port: 7077

Now you can run your Spark jobs on this connection.

Note: Make sure you have Docker and Docker Compose installed on your machine.