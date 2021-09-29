import time
from datetime import timedelta
from textwrap import dedent

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago


doc = dedent(
    """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

"""
)

with DAG("gate_dag",
         description="A test DAG",
         tags=["example"],
         doc_md=doc,
         start_date=days_ago(2),
         schedule_interval=timedelta(minutes=1)) as dag:

    @task(doc_md=doc)
    def gate_build():
        print("running gate build")
        for i in range(100):
            print(f"Progress {i} %")
            time.sleep(1)

        return "mybuildid"

    @task
    def test_build():
        print("running test build")
        return "mybuildid"

    @task
    def gate_test(build_id, test_id):
        print("running gate test with", build_id, test_id)
        pass


    gate_test(gate_build(), test_build())
