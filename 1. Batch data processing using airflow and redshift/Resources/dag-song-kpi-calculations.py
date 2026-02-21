from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
import boto3
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Constants
BUCKET_NAME = 'ab-data-airflow'
SONGS_FILE_PATH = 'spotify_data/songs.csv'
USERS_FILE_PATH = 'spotify_data/users.csv'
STREAMS_PREFIX = 'spotify_data/streams/'
ARCHIVE_PREFIX = 'spotify_data/streams/archived/'

REQUIRED_COLUMNS = {
    'songs': [
        'id', 'track_id', 'artists', 'album_name', 'track_name', 'popularity',
        'duration_ms', 'explicit', 'danceability', 'energy', 'key', 'loudness',
        'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness',
        'valence', 'tempo', 'time_signature', 'track_genre'
    ],
    'streams': ['user_id', 'track_id', 'listen_time'],
    'users': ['user_id', 'user_name', 'user_age', 'user_country', 'created_at']
}

def list_s3_files(prefix, bucket=BUCKET_NAME):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [
        obj['Key']
        for obj in response.get('Contents', [])
        if obj['Key'].endswith('.csv')
    ]

def read_s3_csv(key, bucket=BUCKET_NAME):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj['Body'])

def validate_datasets_task():
    results = {}

    songs_df = read_s3_csv(SONGS_FILE_PATH)
    results['songs'] = not (
        set(REQUIRED_COLUMNS['songs']) - set(songs_df.columns)
    )

    users_df = read_s3_csv(USERS_FILE_PATH)
    results['users'] = not (
        set(REQUIRED_COLUMNS['users']) - set(users_df.columns)
    )

    stream_files = list_s3_files(STREAMS_PREFIX)
    for file in stream_files:
        streams_df = read_s3_csv(file)
        if set(REQUIRED_COLUMNS['streams']) - set(streams_df.columns):
            results['streams'] = False
            break
    else:
        results['streams'] = True

    logging.info(f"Validation results: {results}")
    return results

def branch_task(ti):
    results = ti.xcom_pull(task_ids='validate_datasets')
    return 'calculate_genre_level_kpis' if all(results.values()) else 'end_dag'

def upsert_to_redshift(df, table_name, id_columns):
    hook = PostgresHook(postgres_conn_id="redshift_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        data = [tuple(row) for row in df.to_numpy()]
        cols = ', '.join(df.columns)
        vals = ', '.join(['%s'] * len(df.columns))

        cursor.executemany(
            f"INSERT INTO reporting_schema.tmp_{table_name} ({cols}) VALUES ({vals})",
            data
        )

        condition = ' AND '.join(
            [f"tmp_{table_name}.{c} = {table_name}.{c}" for c in id_columns]
        )

        cursor.execute(f"""
            BEGIN;
            DELETE FROM reporting_schema.{table_name}
            USING reporting_schema.tmp_{table_name}
            WHERE {condition};

            INSERT INTO reporting_schema.{table_name}
            SELECT * FROM reporting_schema.tmp_{table_name};

            TRUNCATE TABLE reporting_schema.tmp_{table_name};
            COMMIT;
        """)

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def calculate_genre_level_kpis():
    streams = pd.concat(
        [read_s3_csv(f) for f in list_s3_files(STREAMS_PREFIX)],
        ignore_index=True
    )
    songs = read_s3_csv(SONGS_FILE_PATH)

    streams['listen_date'] = pd.to_datetime(streams['listen_time']).dt.date
    data = streams.merge(songs, on='track_id', how='left')

    counts = data.groupby(['listen_date', 'track_genre']).size().reset_index(name='listen_count')
    totals = data.groupby('listen_date').size().reset_index(name='total_listens')
    counts = counts.merge(totals, on='listen_date')
    counts['popularity_index'] = counts['listen_count'] / counts['total_listens']

    data['duration_seconds'] = data['duration_ms'] / 1000
    avg_duration = data.groupby(
        ['listen_date', 'track_genre']
    )['duration_seconds'].mean().reset_index(name='average_duration')

    popular = (
        data.groupby(['listen_date', 'track_genre', 'track_id'])
        .size()
        .reset_index(name='cnt')
        .sort_values(['listen_date', 'track_genre', 'cnt'], ascending=False)
        .drop_duplicates(['listen_date', 'track_genre'])
        .rename(columns={'track_id': 'most_popular_track_id'})
    )

    final = counts.merge(avg_duration, on=['listen_date', 'track_genre']) \
              .merge(
                  popular[['listen_date', 'track_genre', 'most_popular_track_id']],
                  on=['listen_date', 'track_genre']
              )

    # Drop intermediate calculation column
    final = final.drop(columns=['total_listens'])


    upsert_to_redshift(final, 'genre_level_kpis', ['listen_date', 'track_genre'])

def calculate_hourly_kpis():
    streams = pd.concat(
        [read_s3_csv(f) for f in list_s3_files(STREAMS_PREFIX)],
        ignore_index=True
    )
    songs = read_s3_csv(SONGS_FILE_PATH)
    users = read_s3_csv(USERS_FILE_PATH)

    streams['listen_date'] = pd.to_datetime(streams['listen_time']).dt.date
    streams['listen_hour'] = pd.to_datetime(streams['listen_time']).dt.hour

    data = streams.merge(songs, on='track_id').merge(users, on='user_id')

    unique_users = data.groupby(
        ['listen_date', 'listen_hour']
    )['user_id'].nunique().reset_index(name='unique_listeners')

    artist_counts = data.groupby(
        ['listen_date', 'listen_hour', 'artists']
    ).size().reset_index(name='listen_counts')

    top_artist = artist_counts.loc[
        artist_counts.groupby(['listen_date', 'listen_hour'])['listen_counts'].idxmax()
    ].rename(columns={'artists': 'top_artist'})

    final = unique_users.merge(top_artist, on=['listen_date', 'listen_hour'])

    upsert_to_redshift(final, 'hourly_kpis', ['listen_date', 'listen_hour'])

def move_processed_files():
    s3 = boto3.client('s3')
    for key in list_s3_files(STREAMS_PREFIX):
        dest = key.replace(STREAMS_PREFIX, ARCHIVE_PREFIX)
        s3.copy_object(Bucket=BUCKET_NAME, CopySource={'Bucket': BUCKET_NAME, 'Key': key}, Key=dest)
        s3.delete_object(Bucket=BUCKET_NAME, Key=key)

with DAG(
    dag_id='data_validation_and_kpi_computation',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    default_args=default_args,
) as dag:

    validate_datasets = PythonOperator(
        task_id='validate_datasets',
        python_callable=validate_datasets_task
    )

    check_validation = BranchPythonOperator(
        task_id='check_validation',
        python_callable=branch_task
    )

    calculate_genre = PythonOperator(
        task_id='calculate_genre_level_kpis',
        python_callable=calculate_genre_level_kpis
    )

    calculate_hourly = PythonOperator(
        task_id='calculate_hourly_kpis',
        python_callable=calculate_hourly_kpis
    )

    move_files = PythonOperator(
        task_id='move_processed_files',
        python_callable=move_processed_files
    )

    end_dag = EmptyOperator(task_id='end_dag')

    validate_datasets >> check_validation >> [calculate_genre, end_dag]
    calculate_genre >> calculate_hourly >> move_files
