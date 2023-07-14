FROM apache/airflow:2.6.3-python3.9

ADD requirements.txt /home/imdb_etl_pipeline/requirements.txt
RUN pip install -r /home/imdb_etl_pipeline/requirements.txt

