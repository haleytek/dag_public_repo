from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow import configuration as conf

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
if namespace =='default':
    config_file = '/opt/bitnami/airflow/.kube/config'
    in_cluster=False
else:
    in_cluster=True
    config_file=None

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
    k = KubernetesPodOperator(
        namespace=namespace,
        image="ubuntu:16.04",
        cmds=["bash", "-cx"],
        arguments=["echo", "hello world", ">>", "PV.txt", "&&", "ls", "-la", "/usr/local/tmp" ],
        labels={"foo": "bar"},
        name="airflow-test-pod",
        task_id="task-one",
        in_cluster=in_cluster, # if set to true, will look in the cluster, if false, looks for file
        cluster_context='airflowpool-admin', # is ignored when in_cluster is set to True
        config_file=config_file,
        resources=compute_resources,
        is_delete_operator_pod=True,
        get_logs=True,
        volumes=[
            Volume("azure_managed_disk_haleytek_gate",
                {
                "persistentVolumeClaim":
                {
                    "claimName": "azure-managed-disk-haleytek-gate"
                }
        })
        ],
        volume_mounts=[
            VolumeMount("azure_managed_disk_haleytek_gate", “/usr/local/tmp", sub_path=None, read_only=False)
        ]
    )
