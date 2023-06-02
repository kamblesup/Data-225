from datetime import timedelta, datetime
from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator



#GOOGLE_CONN_ID = "bigquery_default"
PROJECT_ID="db-accidents-group-7"
GCS_PATH = "accidents/"
BUCKET_NAME = 'db_accidents_group_7'
STAGING_DATASET = "staging_dataset"
DATASET = "accidents_dataset"
LOCATION = "us-west1"

default_args = {
    'owner': 'Rithwik Maramraju',
    'depends_on_past': False,
    'email_on_failure': ['rithwik.maramraju`@sjsu.edu'],
    'email_on_retry': False,
    'retries': 1,
    'start_date':  days_ago(2),
    'retry_delay': timedelta(minutes=5),
}

with DAG('ETL_UsingAirflow', schedule_interval=timedelta(days=1), default_args=default_args) as dag:
    Initiate_pipeline = DummyOperator(
        task_id = 'Initiate_pipeline',
        dag = dag
        )


    data_into_staging_dataset = DummyOperator(
        task_id = 'data_into_staging_dataset',
        dag = dag
        )    
    
    loading_accidents = GCSToBigQueryOperator(
        task_id = 'loading_accidents',
        bucket = BUCKET_NAME,
        source_objects = ['accidents/us_accidents.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.US_Accidents',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'ID', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'Severity', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'Start_Time', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {'name': 'End_Time', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {'name': 'Start_Lat', 'type': 'BIGNUMERIC', 'mode': 'NULLABLE'},
        {'name': 'Start_Lng', 'type': 'BIGNUMERIC', 'mode': 'NULLABLE'},
        {'name': 'End_Lat', 'type': 'BIGNUMERIC', 'mode': 'NULLABLE'},
        {'name': 'End_Lng', 'type': 'BIGNUMERIC', 'mode': 'NULLABLE'},
        {'name': 'Distance', 'type': 'BIGNUMERIC', 'mode': 'NULLABLE'},
        {'name': 'Descrip', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Num', 'type': 'BIGNUMERIC', 'mode': 'NULLABLE'},
        {'name': 'Street', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Side', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'City', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'County', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'State', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'Zipcode', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Country', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Timezone', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Airport_Code', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'Weather_Timestamp', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Temperature', 'type': 'BIGNUMERIC', 'mode': 'NULLABLE'},
        {'name': 'Wind_Chill', 'type': 'BIGNUMERIC', 'mode': 'NULLABLE'},
        {'name': 'Humidity', 'type': 'BIGNUMERIC', 'mode': 'NULLABLE'},
        {'name': 'Pressure', 'type': 'BIGNUMERIC', 'mode': 'NULLABLE'},
        {'name': 'Visibility', 'type': 'BIGNUMERIC', 'mode': 'NULLABLE'},
        {'name': 'Wind_Direction', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Wind_Speed', 'type': 'BIGNUMERIC', 'mode': 'NULLABLE'},
        {'name': 'Precipitation', 'type': 'BIGNUMERIC', 'mode': 'NULLABLE'},
        {'name': 'Weather_Condition', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Amenity', 'type': 'BOOL', 'mode': 'NULLABLE'},
        {'name': 'Bump', 'type': 'BOOL', 'mode': 'NULLABLE'},
        {'name': 'Crossing', 'type': 'BOOL', 'mode': 'NULLABLE'},
        {'name': 'Give_Way', 'type': 'BOOL', 'mode': 'NULLABLE'},
        {'name': 'Junction', 'type': 'BOOL', 'mode': 'NULLABLE'},
        {'name': 'No_Exit', 'type': 'BOOL', 'mode': 'NULLABLE'},
        {'name': 'Railway', 'type': 'BOOL', 'mode': 'NULLABLE'},
        {'name': 'Roundabout', 'type': 'BOOL', 'mode': 'NULLABLE'},
        {'name': 'Station', 'type': 'BOOL', 'mode': 'NULLABLE'},
        {'name': 'Stops', 'type': 'BOOL', 'mode': 'NULLABLE'},
        {'name': 'Traffic_Calming', 'type': 'BOOL', 'mode': 'NULLABLE'},
        {'name': 'Traffic_Signal', 'type': 'BOOL', 'mode': 'NULLABLE'},
        {'name': 'Turning_Loop', 'type': 'BOOL', 'mode': 'NULLABLE'},
        {'name': 'Sunrise_Sunset', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Civil_Twilight', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Nautical_Twilight', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Astronomical_Twilight', 'type': 'STRING', 'mode': 'NULLABLE'},
          ]
        )

    loading_state = GCSToBigQueryOperator(
        task_id = 'loading_state',
        bucket = BUCKET_NAME,
        source_objects = ['accidents/State.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.State',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'state_name', 'type': 'STRING', 'mode': 'REQUIRED'},
           {'name': 'state_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            ]
        )

    loading_severity = GCSToBigQueryOperator(
        task_id = 'loading_severity',
        bucket = BUCKET_NAME,
        source_objects = ['accidents/Severity.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.Severity',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'Severity', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'Id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            ]
        )
    
    loading_airports = GCSToBigQueryOperator(
        task_id = 'loading_airports',
        bucket = BUCKET_NAME,
        source_objects = ['accidents/Airports.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.Airports',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'airport_code', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'airport_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            ]
        )   
    
    check_accidents = BigQueryCheckOperator(
        task_id = 'check_accidents',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.US_Accidents`'
        )

    check_state = BigQueryCheckOperator(
        task_id = 'check_state',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.State`'
        )

    check_severity = BigQueryCheckOperator(
        task_id = 'check_severity',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.Severity`'
        ) 

    check_airports = BigQueryCheckOperator(
        task_id = 'check_airports',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.Airports`'
        )
 
    creating_Dimension_Tables = DummyOperator(
        task_id = 'Creating_Dimension_Tables',
        dag = dag
        )

    creating_accidents = BigQueryOperator(
        task_id = 'creating_accidents',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_accidents.sql'
        )

    creating_state = BigQueryOperator(
        task_id = 'creating_state',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_state.sql'
        )   

    creating_airports = BigQueryOperator(
        task_id = 'creating_airports',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_airports.sql'
        )

    creating_severity = BigQueryOperator(
        task_id = 'creating_severity',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_severity.sql'
        )

    create_Fact_accidents = BigQueryOperator(
        task_id = 'create_Fact_accidents',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/F_accidents.sql'
        )

    check_Fact_accidents = BigQueryCheckOperator(
        task_id = 'check_Fact_accidents',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.F_accidents`'
    )

    end_pipeline = DummyOperator(
        task_id = 'end_pipeline',
        dag = dag
        ) 
Initiate_pipeline >> data_into_staging_dataset

data_into_staging_dataset >> [loading_accidents, loading_severity, loading_state, loading_airports]

loading_accidents >> check_accidents
loading_severity >> check_severity
loading_state >> check_state
loading_airports >> check_airports

[check_accidents, check_severity, check_state, check_airports] >> creating_Dimension_Tables

creating_Dimension_Tables >> [creating_accidents, creating_state, creating_severity, creating_airports]

[creating_accidents, creating_state, creating_severity, creating_airports] >> create_Fact_accidents

create_Fact_accidents >> check_Fact_accidents >> end_pipeline

