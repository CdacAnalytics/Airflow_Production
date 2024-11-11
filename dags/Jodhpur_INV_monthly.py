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

    print('Hospital Code:', hospital_code)
    print('Hospital Name:', hospital_name)

    success_message = f'Task {task_id} in DAG {dag_id} succeeded on {execution_date_time}.'
    log_success_to_db(dag_id, execution_date_time, success_message,hospital_code,hospital_name)


def log_success_to_db(dag_id, execution_date_time, success_message,hospital_code,hospital_name):
    hook = PostgresHook(postgres_conn_id='aiimsnew_destination_connection')
    conn = hook.get_conn()
    try:
        insert_sql = """
        INSERT INTO airflow_success_log (dag_id, execution_date_time, success_message, hospital_code, hospital_name)
        VALUES (%s, %s, %s, %s,%s);
        """
        hook.run(insert_sql, parameters=(dag_id, execution_date_time, success_message,hospital_code,hospital_name))
        conn.commit()
    finally:
        logging.log('Closing the Connection after inserting the Success meta-data')
        conn.close()


def log_failure_to_db(task_id, dag_id, execution_date_time, exception,No_of_retries,hospital_code,hospital_name):
    print('Entering the Data into the fail table as the task fails')
    hook = PostgresHook(postgres_conn_id='aiimsnew_destination_connection')
    conn = hook.get_conn()
    try:
        insert_sql2 = """
        INSERT INTO airflow_fail_log (task_id, dag_id, execution_date_time, error_message,retry_count,hospital_code, hospital_name)
        VALUES (%s, %s, %s, %s,%s,%s,%s);
        """
        hook.run(insert_sql2, parameters=(task_id, dag_id, execution_date_time, exception,No_of_retries,hospital_code,hospital_name))
        conn.commit()
    finally:
        logging.log('Closing the Connection after inserting the failure meta-data')
        conn.close()

def export_data_staging(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='Mang_UAT_source_conn',schema = 'aiims_manglagiri')
    #destination_hook = PostgresHook(postgres_conn_id='abdm_uat_connection', schema='aiimsnew')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    query = ''' 
        select
	        Count(a.hivtnum_req_dno) as Total_req,
	        Count(a.hivdt_coll_date_time) as total_smp_coll,
	        Count(a.hivtdt_res_val_date) as total_res_print,
	        a.gnum_hospital_code as Hospital_code,
	        H.gstr_hospital_name as hospital_name
        from hivt_requisition_dtl a, gblt_hospital_mst H
        where TRUNC(hivdt_requisition_date_reqd) >= TRUNC(ADD_MONTHS(SYSDATE, -1), 'MM')  
        AND a.gnum_hospital_code = H.gnum_hospital_code
        and TRUNC(a.hivdt_requisition_date_reqd) <= TRUNC(SYSDATE, 'MM')-1
        and a.gnum_hospital_code = 37913
        group by Hospital_code,hospital_name;
            '''
    
    cursor.execute(query)
    rows = cursor.fetchall()
    print('printing the rows:',rows,'Type of Rows',type(rows))

    # Convert Decimal types to float/int
    def convert_decimal(row):
        return [float(x) if isinstance(x, Decimal) else x for x in row]
    
    rows = [convert_decimal(row) for row in rows]

    hospital_code = rows[0][3]
    hospital_name = rows[0][4] 

    # Adding hospital_code and hospital_name to the context
    kwargs['ti'].xcom_push(key='hospital_code', value=hospital_code)
    kwargs['ti'].xcom_push(key='hospital_name', value=hospital_name)

    # writing the data and after transfer, the data is deleted
    with open('/tmp/staging_data.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['Total_req','total_smp_coll','total_res_print','Hospital_code','Month','Year'])
        writer.writerows(rows)
    
    with open('/tmp/staging_data.csv', 'r') as file:
        read = csv.reader(file)
        print('printing the rows from Stagging area:')
        for rows in read:
            print(rows)

def load_csv_to_postgres():
    # Create a PostgresHook instance
    hook = PostgresHook(postgres_conn_id='aiimsnew_destination_connection',schema = 'aiimsnew')
    conn = hook.get_conn()
    # Open the CSV file
    try:
        with open('/tmp/staging_data.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header row
        
            # Prepare the SQL update query
            update_sql = """
            UPDATE AIIMS_basic_stats
            SET gnum_total_req_raise = %s, gnum_total_sample_collected = %s, gnum_total_report_generated = %s
            WHERE trunc(gdt_entry_date) = trunc(sysdate);
            """
        
            # Iterate over the CSV rows and update data in PostgreSQL
            for row in reader:
                hook.run(update_sql, parameters=(row[0], row[1], row[2]))
        conn.commit()
    finally:
        conn.close()



# Default arguments for the DAG
default_args = {
    'owner': 'Gaurav',
    'start_date': datetime(2023, 11, 22, tzinfo=time_zone),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': send_alert,
}

# Define the DAG
with DAG(
        dag_id="jodhpur_INV_monthly",
        default_args=default_args,
        description="Transferring the data from ABDM UAT to development",
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

    load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_csv_to_postgres,
    dag=dag,
    on_success_callback=send_success_alert,
    on_failure_callback=send_alert
)

    export_task >> load_data 