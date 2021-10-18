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
        ret = out("kubectl describe pvc " + pvc.metadata.name + " --kubeconfig=" + kube_config)
        for line in ret.splitlines():
            if line.startswith("Used By:"):
                if "<none>" in line:
                    return pvc.metadata.name
    return None
