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
from airflow.exceptions import AirflowSkipException


import logging
import csv
from datetime import datetime, timedelta
import pendulum
from airflow.utils.email import send_email
from email.mime.text import MIMEText
import smtplib
import psycopg2


# setting the time as indian standard time. We have to set this if we want to schedule a pipeline 
time_zone = pendulum.timezone("Asia/Kolkata")

def send_alert(context):
    print('Task failed sending an Email')
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = context.get('dag').dag_id
    execution_date = str(context.get('execution_date'))
    exception = str(context.get('exception'))  # Convert the exception to a string
    No_of_retries = default_args['retries']
    retry_delay = default_args['retry_delay']
    
    ti = context['ti']
    hospital_code = ti.xcom_pull(key='hospital_code', task_ids='export_data_to_csv')
    hospital_name = ti.xcom_pull(key='hospital_name', task_ids='export_data_to_csv')
    print()

    subject = f'Airflow Alert: Task Failure in {dag_id}'
    body = f"""
    <br>Task ID: {task_id}</br>
    <br>DAG ID: {dag_id}</br>
    <br>Execution Date: {execution_date}</br>
    <br>Retries: {No_of_retries}</br>
    <br>Delay_between_retry: {retry_delay}</br>
    <br>Task failed and retries exhausted. Manual intervention required.</br>

    """
    
    # Using Airflow's send_email function for consistency and better integration
    send_email(
        to='gauravnagraleofficial@gmail.com',
        subject=subject,
        html_content=body
    )
    
    # Log failure to DB, ensuring exception is converted to string
    log_failure_to_db(task_id, dag_id, execution_date, exception, No_of_retries, hospital_code, hospital_name)

def send_success_alert(context):
    print('Task succeeded sending a notification')
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = context.get('dag').dag_id
    execution_date_time = context.get('execution_date')
    # Fetch hospital_code and hospital_name from XCom
    ti = context['ti']
    hospital_code = ti.xcom_pull(key='hospital_code', task_ids='export_data_to_csv')
    hospital_name = ti.xcom_pull(key='hospital_name', task_ids='export_data_to_csv')
    
    success_message = f'Task {task_id} in DAG {dag_id} succeeded on {execution_date_time}.'
    log_success_to_db(dag_id, execution_date_time, success_message,hospital_code,hospital_name)

def log_success_to_db(dag_id, execution_date_time, success_message,hospital_code,hospital_name):
    hook = PostgresHook(postgres_conn_id='Mang_UAT_source_conn',schema = 'aiims_manglagiri')
    conn = hook.get_conn()
    try:
        insert_sql = """
        INSERT INTO sync_success_log (dag_id, execution_date_time, success_message, hospital_code, hospital_name)
        VALUES (%s, %s, %s, %s, %s);
        """
        # Execute the SQL insert
        hook.run(insert_sql, parameters=(dag_id, execution_date_time, success_message, hospital_code, hospital_name))
        conn.commit()
    finally:
        logging.log('Closing the Connection after inserting the Success meta-data')
        conn.close()


def log_failure_to_db(task_id, dag_id, execution_date_time, exception,No_of_retries,hospital_code,hospital_name):
    print('Entering the Data into the fail table as the task fails')
    hook = PostgresHook(postgres_conn_id='Mang_UAT_source_conn',schema = 'aiims_manglagiri')
    conn = hook.get_conn()
    try:
        insert_sql2 = """
        INSERT INTO sync_fail_log (task_id, dag_id, execution_date_time, error_message,retry_count,hospital_code, hospital_name)
        VALUES (%s, %s, %s, %s,%s,%s,%s);
        """
        hook.run(insert_sql2, parameters=(task_id, dag_id, execution_date_time, exception,No_of_retries,hospital_code,hospital_name))
        conn.commit()
    finally:
        logging.log('Closing the Connection after inserting the failure meta-data')
        conn.close()

#main logic -----------------------------------------------------------------------------------------------------------------------------------------------------------

def export_data_staging(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='Mang_UAT_source_conn',schema = 'aiims_manglagiri')
    fetch_col = PostgresHook(postgres_conn_id='fetch_columns',schema = 'aiims_manglagiri')
    #destination_hook = PostgresHook(postgres_conn_id='abdm_uat_connection', schema='aiimsnew')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    '''NO: Change the query to sysdate or currect date ''' 
    row_cnt = '''
                SELECT count(*) 
                FROM aiims_basic_stats 
                WHERE gdt_entry_date BETWEEN NOW() - INTERVAL '10 minutes' AND NOW();
            '''
    cursor.execute(row_cnt)
    rows = cursor.fetchall()

    print('rows before 10 mins',rows)
    if rows[0][0] > 0:
        query = ''' 
            SELECT * 
            FROM aiims_basic_stats 
            WHERE gdt_entry_date BETWEEN NOW() - INTERVAL '10 minutes' AND NOW();   
            '''
   
        cursor.execute(query)
        rows = cursor.fetchall()
        print('printing the rows before sending it to stagging area:',rows,'Type of Rows',type(rows))

        # Convert Decimal types to float/int
        def convert_decimal(row):
            # isinstant is used for checking if that variable is Decimal or not 
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
            #This creates the writer object which will be used to erite the csv file
            writer = csv.writer(f)
            #writes a single row to the CSV file, which will be the header row. 
            writer.writerow(columns)  
            writer.writerows(rows)
    
        #checking if the data is present at stagging area
        with open('/tmp/staging_data.csv', 'r') as file:
            read = csv.reader(file)
            print('printing the rows from stagging area:')
            for rows in read:
                print(rows)
    else:
        print("No data found in the last 10 minutes. Terminating the DAG.")
        raise AirflowSkipException("Terminating the DAG due to no data found.")

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
    
def transfering_to_aiimsnew_DB():
    hook = PostgresHook(postgres_conn_id='aiimsnew_destination_connection', schema='aiimsnew')
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
        INSERT INTO aiims_basic_stats ({', '.join(columns)})
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
    'on_falure_callback' : send_alert,
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
        on_failure_callback=send_alert
    )

    load_data_ABDM = PythonOperator(
    task_id='load_data_ABDM_DB',
    python_callable=transfering_to_abdm_DB,
    dag=dag,
    on_success_callback=send_success_alert,
    on_failure_callback=send_alert
    )

    load_data_aiimsnew = PythonOperator(
    task_id='load_data_aiimsnew_DB',
    python_callable=transfering_to_aiimsnew_DB,
    dag=dag,
    on_success_callback=send_success_alert,
    on_failure_callback=send_alert
    )
export_task >> [load_data_ABDM, load_data_aiimsnew]