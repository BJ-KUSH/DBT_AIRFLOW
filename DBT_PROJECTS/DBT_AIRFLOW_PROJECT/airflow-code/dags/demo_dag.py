import pendulum

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule


@task()
def a_func():
    raise AirflowException


@task(
    trigger_rule=TriggerRule.ALL_FAILED,
)
def b_func():
    pass


@dag(schedule="@once", start_date=pendulum.datetime(2021, 1, 1, tz="UTC"))
def my_dag():
    a = a_func()
    b = b_func()

    a >> b


dag = my_dag()
