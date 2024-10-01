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
    insert_sql1 = """
    INSERT INTO airflow_success_log (dag_id, execution_date_time, success_message, hospital_code, hospital_name)
    VALUES (%s, %s, %s, %s,%s);
    """
    hook.run(insert_sql1, parameters=(dag_id, execution_date_time, success_message,hospital_code,hospital_name))


def log_failure_to_db(task_id, dag_id, execution_date_time, exception,No_of_retries,hospital_code,hospital_name):
    print('Entering the Data into the fail table as the task fails')
    hook = PostgresHook(postgres_conn_id='aiimsnew_destination_connection')
    insert_sql2 = """
    INSERT INTO airflow_fail_log (task_id, dag_id, execution_date_time, error_message,retry_count,hospital_code, hospital_name)
    VALUES (%s, %s, %s, %s,%s,%s,%s);
    """
    hook.run(insert_sql2, parameters=(task_id, dag_id, execution_date_time, exception,No_of_retries,hospital_code,hospital_name))
    hook.get_conn().commit() 

def export_data_staging(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='Mang_UAT_source_conn',schema = 'aiims_manglagiri')
    #destination_hook = PostgresHook(postgres_conn_id='abdm_uat_connection', schema='aiimsnew')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    query = ''' 
WITH admission AS (
    SELECT  
        count(*) AS Admissions,
        extract(month FROM Date) AS Month,
        extract(year FROM Date) AS Year,
        Hospital_code
    FROM 
    ( 
        SELECT 
            DISTINCT gnum_owndept_code AS dept_code,
            hipnum_admno,
            TRUNC(hipdt_admdatetime) AS Date,
            GNUM_HOSPITAL_CODE AS Hospital_code
        FROM hipt_patadmdisc_dtl p 
        WHERE gnum_isvalid = 1 
        AND GNUM_HOSPITAL_CODE = 37913
        AND TRUNC(gdt_entry_date) >= TRUNC(ADD_MONTHS(SYSDATE, -1), 'MM')  
        AND TRUNC(gdt_entry_date) < TRUNC(SYSDATE, 'MM') - 1
    )
    GROUP BY Month, Year, Hospital_code	
),
discharge AS (
    SELECT 
        COUNT(hipnum_admno) AS Discharge_count,
        extract(month FROM hipdt_disdatetime) AS dis_month,
        extract(year FROM hipdt_disdatetime) AS dis_year,
        GNUM_HOSPITAL_CODE
    FROM hipt_patadmdisc_dtl 
    WHERE gnum_isvalid = 1 
    AND hipdt_disdatetime IS NOT NULL 
    AND GNUM_HOSPITAL_CODE = 37913
    AND TRUNC(gdt_entry_date) >= TRUNC(ADD_MONTHS(SYSDATE, -1), 'MM')  
    AND TRUNC(gdt_entry_date) < TRUNC(SYSDATE, 'MM') - 1
    GROUP BY dis_month, dis_year, GNUM_HOSPITAL_CODE
),
discharge_summary AS (
    SELECT 
        COUNT(*) AS Discharge_summary_generated,
        monthss,
        year,
        GNUM_HOSPITAL_CODE 
    FROM 
    (
        SELECT 
            DISTINCT gnum_owndept_code AS dept_code, 
            hipnum_admno,
            TRUNC(adm.hipdt_disdatetime) AS date,
            TO_CHAR(TRUNC(adm.hipdt_disdatetime),'MM')::numeric AS monthss,
            TO_CHAR(TRUNC(adm.hipdt_disdatetime),'YYYY')::numeric AS year, 
            GNUM_HOSPITAL_CODE 
        FROM hipt_patadmdisc_dtl adm 
        WHERE gnum_isvalid = 1 
        AND gnum_hospital_code = 37913
        AND hipdt_disdatetime IS NOT NULL 
        AND hipnum_admno IN ( 
            SELECT hrgnum_admission_no 
            FROM hpmrt_pat_profile_dtl b 
            WHERE hpmrnum_profile_status = 2 
            AND gnum_hospital_code = adm.gnum_hospital_code 
            AND hpmrnum_profile_type = 16 
            AND gnum_isvalid = 1 
            AND b.hrgnum_puk = adm.hrgnum_puk 
            AND adm.hipnum_admno = b.hrgnum_admission_no
        )
        AND TRUNC(gdt_entry_date) >= TRUNC(ADD_MONTHS(SYSDATE, -1), 'MM')  
        AND TRUNC(gdt_entry_date) < TRUNC(SYSDATE, 'MM') - 1
    )
    GROUP BY monthss, year, GNUM_HOSPITAL_CODE
)
SELECT 
    A.Month,
    A.Year,
    A.Hospital_code,
    (SELECT gstr_hospital_name FROM gblt_hospital_mst WHERE A.Hospital_code = gnum_hospital_code) AS Hospital_name,
    SUM(A.Admissions) AS Total_admissions,
    SUM(D.Discharge_count) AS Total_discharges,
    SUM(DS.Discharge_summary_generated) AS Total_Discharge_summary,
    ROUND((SUM(DS.Discharge_summary_generated) / SUM(D.Discharge_count)), 2) * 100 AS discharge_percent
FROM admission A
JOIN discharge D 
    ON A.Hospital_code = D.gnum_hospital_code
    AND A.Month = D.dis_month
    AND A.Year = D.dis_year
JOIN discharge_summary DS 
    ON A.Hospital_code = DS.gnum_hospital_code
    AND A.Month = DS.monthss
    AND A.Year = DS.year
GROUP BY A.Month, A.Year, A.Hospital_code
ORDER BY Month;
            '''
    cursor.execute(query)
    rows = cursor.fetchall()
    print('printing the rows:',rows,'Type of Rows',type(rows))

    # Convert Decimal types to float/int
    def convert_decimal(row):
        return [float(x) if isinstance(x, Decimal) else x for x in row]
    
    rows = [convert_decimal(row) for row in rows]

    hospital_code = rows[0][2]
    hospital_name = rows[0][3] 

    print('hospital_code:',hospital_code)
    print('hospital_name:',hospital_name)   
    # Adding hospital_code and hospital_name to the context
    kwargs['ti'].xcom_push(key='hospital_code', value=hospital_code)
    kwargs['ti'].xcom_push(key='hospital_name', value=hospital_name)

    # writing the data and after transfer, the data is deleted
    with open('/tmp/staging_data.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['Month','Year','Hospital_code','Hospital_name','Total_admissions','Total_discharges','Total_Discharge_summary','discharge_percent'])
        writer.writerows(rows)
    
    with open('/tmp/staging_data.csv', 'r') as file:
        read = csv.reader(file)
        print('printing the rows:')
        for rows in read:
            print(rows)

def load_csv_to_postgres():
    # Create a PostgresHook instance
    hook = PostgresHook(postgres_conn_id='aiimsnew_destination_connection')
    
    # Open the CSV file
    with open('/tmp/staging_data.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip the header row
        
        # Prepare the SQL update query
        update_sql = """
        UPDATE AIIMS_basic_stats
        SET gnum_total_adm_count = %s, gnum_total_dis_count = %s, gnum_total_dis_summary_count = %s
        WHERE trunc(gdt_entry_date) = trunc(sysdate);
        """
        
        # Iterate over the CSV rows and update data in PostgreSQL
        for row in reader:
            print(row)
            hook.run(update_sql, parameters=(row[4], row[5], row[6]))
    
    # Commit the transaction
    hook.get_conn().commit()



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
        dag_id="jodhpur_ADT_monthly",
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