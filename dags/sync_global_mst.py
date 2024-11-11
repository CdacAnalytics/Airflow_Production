from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.generic_transfer import GenericTransfer
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from decimal import Decimal

import logging
import csv
from datetime import datetime, timedelta
import pendulum
from airflow.utils.email import send_email
from email.mime.text import MIMEText
import smtplib
import psycopg2

def export_data_staging(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='Mang_UAT_source_conn',schema = 'aiims_manglagiri')
    fetch_col = PostgresHook(postgres_conn_id='fetch_columns',schema = 'aiims_manglagiri')
    #destination_hook = PostgresHook(postgres_conn_id='abdm_uat_connection', schema='aiimsnew')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    '''NO: Change the query to sysdate and currect date ''' 
    query = ''' 
            SELECT * 
            FROM aiims_basic_stats 
            WHERE TRUNC(gdt_entry_date) = TO_DATE('2024-09-24', 'YYYY-MM-DD');   
            '''
   
    cursor.execute(query)
    rows = cursor.fetchall()
    print('printing the rows before sending it to stagging area:',rows,'Type of Rows',type(rows))

    # Convert Decimal types to float/int
    def convert_decimal(row):
        return [float(x) if isinstance(x, Decimal) else x for x in row]
    
    rows = [convert_decimal(row) for row in rows]

    #fetching the columns:
    col_conn = fetch_col.get_conn()
    col_cursor = col_conn.cursor()

    col_query = '''
                select str_column_names 
                from sync_table_name_mst
                where str_table_name = 'AIIMS_basic_stats'      
                and gnum_hospital_code = 100;
                '''
    col_cursor.execute(col_query)
    
    columns = col_cursor.fetchall()
    columns = [col[0] for col in columns]  # Extract the column names from tuples
    print('Printing the columns name:', columns)
    # writing the data and after transfer, the data is deleted
    # Writing the data to CSV using dynamic column names
    with open('/tmp/staging_data.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(columns)  # Use dynamic column names as the header
        writer.writerows(rows)
    
    #checking if the data is present at stagging area
    with open('/tmp/staging_data.csv', 'r') as file:
        read = csv.reader(file)
        print('printing the rows from stagging area:')
        for rows in read:
            print(rows)

def transfering_to_abdm_DB():
    hook = PostgresHook(postgres_conn_id='abdm_connector', schema='abdm')
    fetch_col = PostgresHook(postgres_conn_id='fetch_columns',schema = 'aiims_manglagiri')
    col_conn = fetch_col.get_conn()
    col_cursor = col_conn.cursor()

    col_query = '''
                select str_column_names 
                from sync_table_name_mst
                where str_table_name = 'AIIMS_basic_stats'      
                and gnum_hospital_code = 100;
                '''
    col_cursor.execute(col_query)
    # Get the columns (assuming str_column_names is a TEXT[] array)
    columns = col_cursor.fetchone()[0]  # Fetch the first (and presumably only) row and access the array

    # If columns is returned as a list, just print and confirm
    print('Printing the columns name:', columns)
    # Build the SQL insert statement dynamically using the column names
    # Build the SQL insert statement dynamically using the column names
    insert_sql = f"""
        INSERT INTO AIIMS_basic_stats ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(columns))});
        """

    # Getting the connection from the hook (ensure hook is defined and valid)
    conn = hook.get_conn()

    # Proceed with inserting data into PostgreSQL from the CSV file
    try:
        with open('/tmp/staging_data.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header row
        
            # Iterate over CSV rows and insert into PostgreSQL
            for row in reader:
                # Convert empty strings to None
                row = [None if col == '' else col for col in row]
                hook.run(insert_sql, parameters=row)
    
        conn.commit()

    finally:
        print("Closing the connection after inserting the data")  # Logging output
        conn.close()


# setting the time as indian standard time. We have to set this if we want to schedule a pipeline 
time_zone = pendulum.timezone("Asia/Kolkata")
default_args = {
    'owner' : 'Gaurav',
    'start_date' : datetime(2023,11,22,tzinfo = time_zone),
    'retries' : 1,
    'retry_delay' : timedelta(minutes = 1),
    #'on_falure_callback' : send_alert,
}

# Define the DAG
with DAG(
        dag_id="sync_global",
        default_args=default_args,
        description="Transfering data with triggers",
        schedule_interval='0 0 * * *',  # Schedule interval set to every day at midnight
        # 5 - Mins , 11-Hours ,* - any day of week ,*- any month,*-any day of week 
        catchup=False
    ) as dag:

    # Extracting the data from source and loading it into the staging area
    export_task = PythonOperator(
        task_id='export_data_to_csv',
        python_callable=export_data_staging,
        dag=dag,
        #on_failure_callback=send_alert
    )

    load_data = PythonOperator(
    task_id='load_data',
    python_callable=transfering_to_abdm_DB,
    dag=dag,
    #on_success_callback=send_success_alert,
    #on_failure_callback=send_alert
)
export_task >> load_data 