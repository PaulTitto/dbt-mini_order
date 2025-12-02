import datetime
import logging
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

        return luigi.LocalTarget(f"logs/dbt_debug/dbt_debug_log_{timestamp_str}.log")

    def run(self):
        os.makedirs("logs", exist_ok=True)

        try:
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
        except Exception as e:
            logging.error(e)


class dbtDeps(luigi.Task):
    get_current_timestamp = GlobalParams().CurrentTimestampParams

    def requires(self):
        pass

    def output(self):
        timestamp_str = self.get_current_timestamp.strftime('%Y%m%d_%H%M%S')

        return luigi.LocalTarget(f"logs/dbt_deps/dbt_debug_log_{timestamp_str}.log")

    def run(self):
        os.makedirs("logs", exist_ok=True)

        try:
            with open(self.output().path, "a") as f:
                p1 = sp.run("dbt deps",
                            stdout=f,
                            stderr=sp.PIPE,
                            text=True,
                            shell=True,
                            check=True
                            )
                if p1.returncode == 0:
                    print("Success Run dbt deps process")
                else:
                    print("Error Run dbt deps process")
        except Exception as e:
            logging.error(e)

class dbtRun(luigi.Task):
    get_current_timestamp = GlobalParams().CurrentTimestampParams

    def requires(self):
        pass

    def output(self):
        timestamp_str = self.get_current_timestamp.strftime('%Y%m%d_%H%M%S')

        return luigi.LocalTarget(f"logs/dbt_run/dbt_debug_log_{timestamp_str}.log")

    def run(self):
        os.makedirs("logs", exist_ok=True)

        try:
            with open(self.output().path, "a") as f:
                p1 = sp.run("dbt run",
                            stdout=f,
                            stderr=sp.PIPE,
                            text=True,
                            shell=True,
                            check=True
                            )
                if p1.returncode == 0:
                    print("Success Run dbt run process")
                else:
                    print("Error Run dbt run process")
        except Exception as e:
            logging.error(e)

class dbtTest(luigi.Task):
    get_current_timestamp = GlobalParams().CurrentTimestampParams

    def requires(self):
        pass

    def output(self):
        timestamp_str = self.get_current_timestamp.strftime('%Y%m%d_%H%M%S')

        return luigi.LocalTarget(f"logs/dbt_test/dbt_debug_log_{timestamp_str}.log")

    def run(self):
        os.makedirs("logs", exist_ok=True)

        try:
            with open(self.output().path, "a") as f:
                p1 = sp.run("dbt test",
                            stdout=f,
                            stderr=sp.PIPE,
                            text=True,
                            shell=True,
                            check=True
                            )
                if p1.returncode == 0:
                    print("Success Run dbt test process")
                else:
                    print("Error Run dbt test process")
        except Exception as e:
            logging.error(e)

if __name__ == "__main__":
    luigi.build([
        dbtDebug(),
        dbtDeps(),
        dbtRun(),
        dbtTest()
    ],
        local_scheduler=True,
    )