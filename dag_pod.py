from datetime import datetime, timedelta
from pprint import pprint

from airflow import DAG
from airflow import configuration as conf
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.cncf.kubernetes.backcompat.volume import Volume
from airflow.providers.cncf.kubernetes.backcompat.volume_mount import VolumeMount
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

from lib.HaleyTekKubeOperator import HaleyTekKubeOperator
from lib.kubernetes_utils import get_available_pvc, free_pvc

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

namespace = conf.get('kubernetes', 'NAMESPACE')

# This will detect the default namespace locally and read the
# environment namespace when deployed to Astronomer.
if namespace == 'default':
    config_file = '/opt/bitnami/airflow/.kube/config'
    in_cluster = False
else:
    in_cluster = True
    config_file = None

dag = DAG('kube_pod',
          schedule_interval='@once',
          default_args=default_args)

# This is where we define our desired resources.
compute_resources = \
    {'request_cpu': '800m',
     'request_memory': '3Gi',
     'limit_cpu': '800m',
     'limit_memory': '3Gi'}

with dag:
    def branch_func(**kwargs):
        pprint(kwargs['dag_run'].conf)
        trigger_params = kwargs['dag_run'].conf
        if "branch" in trigger_params.keys():
            if trigger_params.get("branch") == "first":
                return "first_task_pv_allocation"
            elif trigger_params.get("branch") == "second":
                return "second_task"
        else:
            return ["first_task_pv_allocation", "second_task"]


    branch_op = BranchPythonOperator(
        task_id="branch_task",
        python_callable=branch_func,
        provide_context=True
    )


    def create_first_task(**context):
        pvc = get_available_pvc()  # having this one level up will run everytime the dag is loaded to airflow
        print("Allocated PVC: " + str(pvc))
        pod_operation = KubernetesPodOperator(
            namespace=namespace,
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=[
                "ls -la /usr/local/tmp && echo hello world >> /usr/local/tmp/PV.txt && ls -la /usr/local/tmp && cat /usr/local/tmp/PV.txt"],
            # arguments=["ls", "-la", "/usr/local/tmp", "&&", "echo", "hello world", ">>", "/usr/local/tmp/PV.txt", "&&", "ls", "-la", "/usr/local/tmp", "&&", "cat", "/usr/local/tmp/PV.txt"],
            labels={"foo": "bar"},
            name="airflow-test-pod",
            task_id="first_task",
            # if set to true, will look in the cluster, if false, looks for file
            in_cluster=in_cluster,
            # is ignored when in_cluster is set to True
            cluster_context='airflowpool-admin',
            config_file=config_file,
            resources=compute_resources,
            is_delete_operator_pod=True,
            get_logs=True,
            volumes=[
                Volume("azure-managed-disk-haleytek-gate",
                       {
                           "persistentVolumeClaim":
                               {
                                   "claimName": pvc
                               }
                       })
            ],
            volume_mounts=[
                VolumeMount("azure-managed-disk-haleytek-gate",
                            "/usr/local/tmp", sub_path=None, read_only=False)
            ],
            dag=dag
        )
        try:
            pod_operation.execute(context)
        finally:
            free_pvc([pvc])


    first_task_pv_allocation = PythonOperator(task_id="first_task_pv_allocation", python_callable=create_first_task,
                                              provide_context=True)

    second_task = HaleyTekKubeOperator(
        namespace=namespace,
        image="ubuntu:16.04",
        cmds=["bash", "-cx"],
        arguments=[
            "cd /usr/local/tmp && apt-get -y update && apt-get -y install git build-essential && rm -rf dag_test_repo_to_sync &&git clone https://github.com/haleytek/dag_test_repo_to_sync.git && cd dag_test_repo_to_sync && make && ./hellomake"],
        # arguments=["echo", "hello"],
        labels={"foo": "bar"},
        name="airflow-test-pod",
        task_id="second_task",
        # if set to true, will look in the cluster, if false, looks for file
        in_cluster=in_cluster,
        # is ignored when in_cluster is set to True
        cluster_context='airflowpool-admin',
        config_file=config_file,
        resources=compute_resources,
        is_delete_operator_pod=True,
        get_logs=True,
    )

    branch_op >> [first_task_pv_allocation, second_task]
