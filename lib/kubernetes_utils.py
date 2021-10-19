from subprocess import PIPE, run
from typing import List

from kubernetes import client, config


def get_available_pvc() -> List[str]:
    kube_config = "/opt/bitnami/airflow/.kube/config"
    config.load_kube_config(config_file=kube_config)
    kubectl = client.CoreV1Api()
    pvcs = kubectl.list_persistent_volume_claim_for_all_namespaces()

    def out(command):
        result = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=True)
        return result.stdout

    for pvc in pvcs.items:
        if 'taken' in pvc.metadata.labels.keys() and pvc.metadata.labels['taken'] != 'True':
            #ret = out("kubectl describe pvc " + pvc.metadata.name + " --kubeconfig=" + kube_config)
            # this requires kubectl installed on all airflow hosts
            #for line in ret.splitlines():
            #    if line.startswith("Used By:"):
            #        if "<none>" in line:
                        kubectl.patch_namespaced_persistent_volume_claim(name=pvc.metadata.name,
                                                                         namespace=pvc.metadata.namespace,
                                                                         body={
                                                                             'metadata': {'labels': {'taken': 'True'}}})
                        return pvc.metadata.name
    return None


def free_pvc(pvc_names: List[str], pvc_namespace: str = 'default'):
    kube_config = "/opt/bitnami/airflow/.kube/config"
    config.load_kube_config(config_file=kube_config)
    kubectl = client.CoreV1Api()
    for pvc_name in pvc_names:
        kubectl.patch_namespaced_persistent_volume_claim(name=pvc_name,
                                                         namespace=pvc_namespace,
                                                         body={'metadata': {'labels': {'taken': 'False'}}})
    return pvc_names
