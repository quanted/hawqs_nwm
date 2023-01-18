from nwm import NWM
from utils import DB
import pandas as pd
import os
import time
import logging

logger = logging.getLogger("NWM-Data-logger")

HUC8_FILE_NAME = os.getenv("HUC8_FILE", "comid_huc8_boundary_conditions.csv")
HUC8_FILE = os.path.join("data", HUC8_FILE_NAME)
DASK_SCHEDULER = os.getenv("DASK_SCHEDULER", "nwm-dask-scheduler:8786")


class Manager:
    def __init__(self, n: int, start_date: str, end_date: str, source_file: str = None):

        if source_file is None:
            comid_df = pd.read_csv(HUC8_FILE)
        else:
            comid_df = pd.read_csv(source_file)
        run_comids = []     # only attempts to download a comid data once

        all_completed = False
        t0 = time.time()
        comid_huc8 = []
        total_comids = comid_df.shape[0]
        for index, row in comid_df.iterrows():
            comid = int(row["comid"])
            huc8 = str(row["huc8_from"])
            comid_huc8.append((comid, huc8))

        runtimes = []
        while not all_completed:
            t1 = time.time()
            comid_list = []
            while len(comid_list) == n:
                comid, huc8 = comid_huc8.pop()
                if len(huc8) == 7:
                    huc8 = f"0{huc8}"
                comid_completed = DB.check_comid(comid)
                if not comid_completed:
                    comid_list.append(comid)
                    run_comids.append(comid)
                    DB.add_huc8(huc8=huc8, comid=comid)
                    DB.create_status(comid=comid)
            nwm = NWM(start_date=start_date, end_date=end_date, scheduler=DASK_SCHEDULER, comids=comid_list)
            nwm.get()
            nwm.upload()
            t2 = time.time()
            runtime = round((t2-t1)/60, 2)
            runtimes.append(runtime)
            logger.info(f"Completed {len(run_comids)}/{total_comids} COMIDS, Runtime: {runtime} min(s), "
                        f"Avg Runtime: {round(sum(runtimes)/len(runtimes), 2)} min(s) for {n} COMIDS")
        t3 = time.time()
        logger.info(f"Completed all {len(run_comids)}/{total_comids} COMIDS, Total Runtime: {round((t3-t0)/3600, 2)} hr(s)")


