-- this is just some dummy bql to create a little example table
SELECT 
  string('{{ params.lob }}') as lob, -- here we define the jinja template for lob
  string('{{ ds }}') as airflow_execution_date, -- here we leverage predefined variables airflow has more info: http://airflow.readthedocs.io/en/latest/code.html?highlight=ds_nodash#default-variables
  string('{{ ds_nodash }}') as airflow_execution_date_yyyymmdd, -- here we leverage predefined variables airflow has more info: http://airflow.readthedocs.io/en/latest/code.html?highlight=ds_nodash#default-variables
  string('{{ ts }}') as airflow_execution_timestamp, -- here we leverage predefined variables airflow has more info: http://airflow.readthedocs.io/en/latest/code.html?highlight=ds_nodash#default-variables  
  current_timestamp() as bq_timestamp, -- get the current time from bigquery so we can see any differences between airflow execution date (in the case of backfill's) as opposed to when we actually ran this code
  'Goodbye Word!' as msg -- just a dummy field