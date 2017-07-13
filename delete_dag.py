"""
Script to remove a dag from airflow database.
"""

import sys
import MySQLdb

dag_input = sys.argv[1]

query = {'delete from xcom where dag_id = "' + dag_input + '"',
        'delete from task_instance where dag_id = "' + dag_input + '"',
        'delete from sla_miss where dag_id = "' + dag_input + '"',
        'delete from log where dag_id = "' + dag_input + '"',
        'delete from job where dag_id = "' + dag_input + '"',
        'delete from dag_run where dag_id = "' + dag_input + '"',
        'delete from dag where dag_id = "' + dag_input + '"' }

def connect(query):
        db = MySQLdb.connect(host="XXX.XXX.XXX.XXX:3306", user="USER", passwd="PASSWORD", db="airflow")
        cur = db.cursor()
        cur.execute(query)
        db.commit()
        db.close()
        return

for value in query:
        print value
        connect(value)
