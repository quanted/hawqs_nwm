import os
import argparse
import logging
from nwm import NWM
from manager import Manager
from utils import DB

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("NWM-Data-logger")

parser = argparse.ArgumentParser(
    description="NWM Data Downloader for catchment level storage in s3 buckets as hdf5 files."
)
parser.add_argument("--start_date", default=False, type=str, help='The start date for the NWM data. Format: yyyy-mm-dd')
parser.add_argument("--end_date", default=False, type=str, help='The end date for the NWM data. Format: yyyy-mm-dd')
parser.add_argument("--comids", default=False, type=int, nargs="+", help="The list of COMIDS to download data from NWM.")
parser.add_argument("--download_all_HUC8", action='store_true',
                    help="Automatically download all data for comids found in the HUC8 file.")
parser.add_argument("-n", default=10, type=int, help="Number of catchments to download at a time.")
parser.add_argument("--test", action='store_true',
                    help="Run download all huc8 catchment boundaries in test mode (only complete 50).")
parser.add_argument("--file", default=False, type=str, help="Path to csv file containing COMIDS to download.")
parser.add_argument("--db_cleanup", action='store_true', help="Remove all database entries which are not marked as completed.")

# TODO: Append Data - check db for COMID, if COMPLETED

DASK_SCHEDULER = os.getenv("DASK_SCHEDULER", "nwm-dask-scheduler:8786")


if __name__ == '__main__':
    args = parser.parse_args()
    start_date_default = "2000-01-01"
    end_date_default = "2022-12-31"
    if args.start_date:
        start_date = str(args.start_date)
    else:
        start_date = start_date_default
    if args.end_date:
        end_date = str(args.end_date)
    else:
        end_date = end_date_default
    if args.comids:
        comids = args.comids
        nwm = NWM(start_date=start_date, end_date=end_date, scheduler=DASK_SCHEDULER, comids=comids)
        nwm.get()
        nwm.upload()
    elif args.download_all:
        file_path = None
        if args.file:
            file_path = args.file
        n = args.n
        manager = Manager(n=args.n, start_date=start_date, end_date=end_date, source_file=file_path)
    elif args.db_cleanup:
        DB.cleanup()






