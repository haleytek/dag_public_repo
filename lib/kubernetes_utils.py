from subprocess import PIPE, run
from typing import List

from kubernetes import client, config


def get_available_pvc() -> List[str]:
    config.load_kube_config()
    kubectl = client.CoreV1Api()
    pvcs = kubectl.list_persistent_volume_claim_for_all_namespaces()

    def out(command):
        result = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=True)
        return result.stdout

    list_of_available_pvcs: str = []
    for pvc in pvcs.items:
        ret = out("kubectl describe pvc " + pvc.metadata.name)
        for line in ret.splitlines():
            if line.startswith("Used By:"):
                if "<none>" in line:
                    return pvc.metadata.name

    return None
