from datetime import datetime, timedelta
from pprint import pprint

from airflow import DAG
from airflow import configuration as conf
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.backcompat.volume import Volume
from airflow.providers.cncf.kubernetes.backcompat.volume_mount import VolumeMount
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.trigger_rule import TriggerRule

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

dag = DAG('notAKube_pod',
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
                return "first_task"
            else:
                return "second_task"
        else:
            return "second_task"
        #return first_task

    branch_op = BranchPythonOperator(
        task_id="branch_task",
        python_callable=branch_func,
        #python_callable="first_task",
        provide_context=True
    )

    first_task = KubernetesPodOperator(
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
                               "claimName": "azure-managed-disk-haleytek-gate"
                           }
                   })
        ],
        volume_mounts=[
            VolumeMount("azure-managed-disk-haleytek-gate",
                        "/usr/local/tmp", sub_path=None, read_only=False)
        ]
    )

    second_task = KubernetesPodOperator(
        namespace=namespace,
        image="ubuntu:16.04",
        cmds=["bash", "-cx"],
        arguments=[
            "cd /usr/local/tmp && apt-get -y update && apt-get -y install git build-essential && rm -rf dag_test_repo_to_sync &&git clone https://github.com/haleytek/dag_test_repo_to_sync.git && cd dag_test_repo_to_sync && make && ./hellomake"],
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
        volumes=[
            Volume("azure-managed-disk-haleytek-gate",
                   {
                       "persistentVolumeClaim":
                           {
                               "claimName": "azure-managed-disk-haleytek-gate"
                           }
                   })
        ],
        volume_mounts=[
            VolumeMount("azure-managed-disk-haleytek-gate",
                        "/usr/local/tmp", sub_path=None, read_only=False)
        ]
    )

    third_task = BashOperator(
        task_id = 'third_example',
        #bash_command = 'git clone https://github.com/haleytek/dag_public_repo.git /opt/bitnami/airflow/auysfv && echo this_actually_works',
        #bash_command = 'GIT_SSH_COMMAND=\'ssh -i /opt/bitnami/airflow/id_rsa\' && git clone git@github.com:haleytek/dag_public_repo.git /opt/bitnami/airflow/auysfv && echo THIS_ACTUALLY_WORKS',
        #bash_command = 'git clone @github.com:haleytek/dag_public_repo.git /opt/bitnami/airflow/auysfv  && echo THIS_ACTUALLY_WORKS',
        bash_command = 'echo "PRINTING USER BELOW" && whoami && ls -all /home/airflow/ && echo "PRINTING THIS BEFORE \
                STARTING THE SSH RUN" && ssh -p 29418 -i "/home/airflow/.ssh/id_rsa" \
                -o "StrictHostKeyChecking=no" -p 29418 "vishrut@qa-source-secure.haleytek.net" \
                gerrit review --code-review +1 1,2 -vv',
        dag = dag,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    branch_op >> [first_task, second_task] >> third_task
    #third_task
    #first_task >> third_task
