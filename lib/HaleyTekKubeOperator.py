from typing import Optional

from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from lib.kubernetes_utils import get_available_pvc, free_pvc
from airflow.providers.cncf.kubernetes.backcompat.volume import Volume
from airflow.providers.cncf.kubernetes.backcompat.volume_mount import VolumeMount
from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import (
    convert_volume,
    convert_volume_mount,
)


class HaleyTekKubeOperator(KubernetesPodOperator):
    template_fields = ["pvcs", "volumes", "volume_mounts"]

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.pvcs = []

    def execute(self, context) -> Optional[str]:
        self.pvcs = [get_available_pvc()]
        self.volumes = [convert_volume(Volume("mounted_volume_name",
                                              {
                                                  "persistentVolumeClaim":
                                                      {
                                                          "claimName": self.pvcs[0]
                                                      }
                                              }))]
        # TODO fix the mount path
        self.volume_mounts = [
            convert_volume_mount(VolumeMount(self.volumes[0].name,
                                             "/usr/local/tmp", sub_path=None, read_only=False))]

        super().execute(context)

    def on_kill(self) -> None:
        super().on_kill()
        free_pvc(self.pvcs)
