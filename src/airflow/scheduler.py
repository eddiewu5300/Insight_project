from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta, datetime
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today() - timedelta(minutes=20),
    # datetime.now() - timedelta(minutes=20) # could be useful for the future
    'email': ['airflow@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),

}
dag = DAG('bash_test', default_args=default_args,
          schedule_interval=timedelta(days=1))
user = 'ubuntu'
host = 'ec2-3-214-12-177.compute-1.amazonaws.com'
command = "bash spark-submit --conf spark.cassandra.connection.host='10.0.0.13, 10.0.0.7, 10.0.0.5'\
            --master spark://ip-10-0-0-11:7077\
            --conf spark.executor.memoryOverhead=600\
            --executor-memory 5G \
            --driver-memory 5G daily_update.py"

run_bash = BashOperator(
    task_id='daily update',
    bash_command='ssh -i .ssh/id_rsa ' + user + '@' + host + ' ' + command,
    dag=dag)
