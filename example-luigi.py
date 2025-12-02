

import datetime
import subprocess as sp
import luigi
import os

class GlobalParams(luigi.Config):
    CurrentTimestampParams = luigi.DateSecondParameter(default=datetime.datetime.now())


class dbtDebug(luigi.Task):
    get_current_timestamp = GlobalParams().CurrentTimestampParams

    def requires(self):
        pass

    def output(self):
        timestamp_str = self.get_current_timestamp.strftime('%Y%m%d_%H%M%S')

        return luigi.LocalTarget(f"logs/dbt_debug_log_{timestamp_str}.log")

    def run(self):
        os.makedirs("logs", exist_ok=True)

        with open(self.output().path, "a") as f:
            p1 = sp.run("dbt debug",
                        stdout=f,
                        stderr=sp.PIPE,
                        text=True,
                        shell=True,
                        check=True
                        )
            if p1.returncode == 0:
                print("Success Run dbt debug process")
            else:
                print("Error Run dbt debug process")


if __name__ == "__main__":
    luigi.run(main_task_cls=dbtDebug, local_scheduler=True)