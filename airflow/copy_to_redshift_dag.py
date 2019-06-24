from datetime import datetime

from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from airflow.models import DAG

args = {
    'owner': 'Adil',
    'start_date': datetime(2019, 6, 20),
    'retries': 1,
}


with DAG(dag_id='nyc_taxi_to_redshift', default_args=args, schedule_interval=None) as dag:
    wait_for_s3_file = S3KeySensor(
        bucket_name='mktg-redshift-exchange',
        bucket_key='nyc-taxi/temp-taxi-data',
        wildcard_match=False,
        dag=dag
    )
    upload_to_redshift = S3ToRedshiftTransfer(
        schema='public',
        table='temp-taxi-data',
        s3_bucket='mktg-redshift-exchange',
        s3_key='nyc-taxi',
        copy_options=['CSV', 'IGNOREHEADER 2']
    )

    wait_for_s3_file >> upload_to_redshift
