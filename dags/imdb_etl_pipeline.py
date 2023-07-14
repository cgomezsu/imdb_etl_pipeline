from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine

connection_uri = 'postgresql+psycopg2://airflow:airflow@postgres:5432'
engine = create_engine(connection_uri, pool_pre_ping=True)

MERGED_DATA_TABLE = 'merged_data'
IMPUTED_DATA_TABLE = 'imputed_data'
TRANSFORMED_DATA_TABLE = 'transformed_data'
DEDUPED_DATA_TABLE = 'deduped_data'

def db_connection()-> None:
    """
    Establishes a connection to the database.
    """
    engine.connect()

def extract_data() -> None:
    """
    Extract data from the files in the URLs and merge them.
    """
    file_1 = pd.read_table('https://datasets.imdbws.com/title.basics.tsv.gz',  compression='gzip')
    file_2 = pd.read_table('https://datasets.imdbws.com/title.ratings.tsv.gz',  compression='gzip')

    data = file_1.merge(file_2, on='tconst')
    data.to_sql(MERGED_DATA_TABLE, engine, index=False, if_exists='replace')

def impute_values() -> None:
    """
    Impute missing values in the merged data.
    """
    data = pd.read_sql(f"SELECT * FROM {MERGED_DATA_TABLE}", engine)

    # Temporary df to shift values in columns 2 to 8 for fixing incorrect/missing values
    temp = data.loc[data.genres.isnull()]
    for i in range(8,2,-1):                     
        temp.iloc[:, i] = temp.iloc[:, i-1]
    data.dropna(subset='genres', inplace=True)
    data = pd.concat([data, temp])
    data.sort_index(inplace=True)
    
    data.to_sql(IMPUTED_DATA_TABLE, engine, index=False, if_exists='replace')

def transform_datatypes() -> None:
    """
    Transform data types of the imputed data.
    """
    data = pd.read_sql(f"SELECT * FROM {IMPUTED_DATA_TABLE}", engine)

    data.loc[data['startYear'] == '\\N', 'startYear'] = -1
    data.startYear = data.startYear.astype(int)
    data.loc[data['startYear'] == -1, 'startYear'] = pd.NA
    data.loc[data['endYear'] == '\\N', 'endYear'] = -1
    data.endYear = data.endYear.astype(int)
    data.loc[data['endYear'] == -1, 'endYear'] = pd.NA
    data.loc[data['runtimeMinutes'] == '\\N', 'runtimeMinutes'] = -1
    data.runtimeMinutes = data.runtimeMinutes.astype(int)
    data.loc[data['runtimeMinutes'] == -1, 'runtimeMinutes'] = pd.NA

    data.to_sql(TRANSFORMED_DATA_TABLE, engine, index=False, if_exists='replace')

def dedupe_tconst(path: str) -> None:
    """
    Dedupe the data based on the tconst column.
    """
    data = pd.read_sql(f"SELECT * FROM {TRANSFORMED_DATA_TABLE}", engine)

    if len(data) != data.tconst.nunique():
        data.drop_duplicates(subset=['tconst'], inplace=True)
        data.reset_index(drop=True, inplace=True)

    data.to_sql(DEDUPED_DATA_TABLE, engine, index=False, if_exists='replace')
    data.to_csv(path, sep="\t", index=False)

default_args = {
                'start_date': datetime(2023, 7, 13)
                }

with DAG(
        dag_id='imdb_etl',
        default_args=default_args,
        schedule=timedelta(minutes=10),
        description='ETL DAG for IMDB data',
        max_active_runs=1
        ) as dag:
    
    # Task 0
    task_db_connection = PythonOperator(
        task_id='db_connection',
        python_callable=db_connection
        )
    
    # Task 1
    task_extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
        )

    # Task 2
    task_impute_values = PythonOperator(
        task_id='impute_values',
        python_callable=impute_values
        )
    
    # Task 3
    task_transform_datatypes = PythonOperator(
        task_id='transform_datatypes',
        python_callable=transform_datatypes
        )
    
    # Task 4
    task_dedupe_tconst = PythonOperator(
        task_id='dedupe_tconst',
        python_callable=dedupe_tconst,
        op_kwargs={'path': './imdb_data_ready.tsv'}
        )
    
    task_db_connection >> task_extract_data >> task_impute_values >> task_transform_datatypes >> task_dedupe_tconst