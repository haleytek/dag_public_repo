from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from lib.kubernetes_utils import get_available_pvc, free_pvc
from airflow.providers.cncf.kubernetes.backcompat.volume import Volume
from airflow.providers.cncf.kubernetes.backcompat.volume_mount import VolumeMount


class HaleyTekKubeOperator(KubernetesPodOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.pvcs = []

    def execute(self, context):
        self.pvcs = get_available_pvc()
        self.volumes = []
        self.volume_mounts = []
        for pvc in self.pvcs:
            self.volumes.append(
                Volume("mounted_volume_name",
                       {
                           "persistentVolumeClaim":
                               {
                                   "claimName": pvc
                               }
                       })
            )
        for volume in self.volumes:
            # TODO fix the mount path
            self.volume_mounts.append(
                VolumeMount(volume.name,
                            "/usr/local/tmp", sub_path=None, read_only=False)
            )
        super().execute(context)

    def on_kill(self) -> None:
        super().on_kill()
        free_pvc(self.pvcs)
