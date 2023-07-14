### Airflow ETL Pipeline for IMDb Data

How to run this Airflow pipeline:

* Have Docker CE and Docker Compose installed in your machine. 
* Clone this repo `git clone https://github.com/cgomezsu/imdb_etl_pipeline.git` into your home directory.
* Run these commands:
    - `cd /home/imdb_etl_pipeline`
    - `sudo docker compose -f docker-compose.yaml up airflow-init`
* Once the containers are sucessfully created, run:
    - `sudo chmod -R 777 ./dags ./logs ./plugins`
    - `sudo docker compose -f docker-compose.yaml up -d`
* Check that the containers are up an running:
    - `sudo docker ps`
* Open your browser and go to the Airflow's webserver at `http://localhost:8080`. Login with user and password `airflow`.
* Unpause the `imdb_etl` DAG.
* Each run is scheduled for every 10 minutes. Once a run is completed, you can load the final transformed data from the `/home/imdb_data_ready.tsv` file.
* Alternatively, you can load the resulting data of each task by querying it in a PostgreSQL client using `airflow:airflow@localhost:5432` and from any of this tables in the database:
    - `merged_data`
    - `imputed_data`
    - `transformed_data`
    - `deduped_data`
* To stop the pipeline and containers, pause the DAG in the webserver and run in the terminal:
    - `sudo docker compose -f docker-compose.yaml down --volumes --rmi all`
