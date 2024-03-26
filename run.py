import pltLib
from pltLib.module import Config

if __name__ == '__main__':
    cfg = Config("/home/jmb/code_central/python/configs/config_file_streamsets_na01_hub.yaml")
    cfg.load()
    control_hub = cfg.connectSCH()
    jobs = [job for job in control_hub.jobs.get_all(order="ASC", filter_text="j__prd__")]
    for job in jobs:
        print(job.job_name)
exit(0)