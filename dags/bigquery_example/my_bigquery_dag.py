"""
### My first dag to play around with airflow and bigquery.
"""

# imports
from airflow import DAG
from datetime import datetime, timedelta
# we need to import the bigquery operator - there are lots of cool operators for different tasks and systems, you can also build your own
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

# create a dictionary of default typical args to pass to the dag
default_args = {
    'owner': 'airflow',
    'depends_on_past': False, # does this dag depend on the previous run of the dag? best practice is to try have dags not depend on state or results of a previous run
    'start_date': datetime(2017, 3, 22), # from what date to you want to pretend this dag was born on? by default airflow will try backfill - be careful
    'email_on_failure': True, # should we send emails on failure?
    'email': ['andrew.maguire@pmc.com'], # who to email if fails i.e me :)
    'retries': 1, # if fails how many times should we retry?
    'retry_delay': timedelta(minutes=2), # if we need to retry how long should we wait before retrying?
}

# define the dag 
dag = DAG('my_bigquery_dag', # give the dag a name 
           schedule_interval='@once', # define how often you want it to run - you can pass cron expressions here
           default_args=default_args # pass the default args defined above or you can override them here if you want this dag to behave a little different
         )   
 
# define list of lobs we want to run for
lobs = ["lob001","lob002","lob003"]  

# loop through the lob's we want to use to build up our dag
for lob in lobs:
    
    # define the first task, in our case a big query operator    
    bq_task_1 = BigQueryOperator(
        dag = dag, # need to tell airflow that this task belongs to the dag we defined above
        task_id='my_bq_task_1_'+lob, # task id's must be uniqe within the dag
        bql='my_qry_1.sql', # the actual sql command we want to run on bigquery is in this file in the same folder. it is also templated
        params={"lob": lob}, # the sql file above have a template in it for a 'lob' paramater - this is how we pass it in
        destination_dataset_table='airflow.'+lob+'_test_task1', # we also in this example want our target table to be lob and task specific
        write_disposition='WRITE_TRUNCATE', # drop and recreate this table each time, you could use other options here
        bigquery_conn_id='my_gcp_connection' # this is the airflow connection to gcp we defined in the front end. More info here: https://github.com/alexvanboxel/airflow-gcp-examples
    )
    # add documentation for what this task does - this will be displayed in the Airflow UI
    bq_task_1.doc_md = """\
    Append a "Hello World!" message string to the table [airflow.<lob>_test_task1]
    """

    # define the second task, in our case another big query operator
    bq_task_2 = BigQueryOperator(
        dag = dag, # need to tell airflow that this task belongs to the dag we defined above
        task_id='my_bq_task_2_'+lob, # task id's must be uniqe within the dag
        bql='my_qry_2.sql', # the actual sql command we want to run on bigquery is in this file in the same folder. it is also templated
        params={"lob": lob}, # the sql file above have a template in it for a 'lob' paramater - this is how we pass it in
        destination_dataset_table='airflow.'+lob+'_test_task2', # we also in this example want our target table to be lob and task specific
        write_disposition='WRITE_TRUNCATE', # drop and recreate this table each time, you could use other options here
        bigquery_conn_id='my_gcp_connection' # this is the airflow connection to gcp we defined in the front end. More info here: https://github.com/alexvanboxel/airflow-gcp-examples
    )
    # add documentation for what this task does - this will be displayed in the Airflow UI
    bq_task_2.doc_md = """\
    Append a "Goodbye World!" message string to the table [airflow.<lob>_test_task2]
    """
        
    # set dependencies so for example 'bq_task_2' wont start until 'bq_task_1' is completed with success
    bq_task_2.set_upstream(bq_task_1) 