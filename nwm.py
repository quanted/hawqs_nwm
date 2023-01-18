import logging
import warnings
import datetime
import s3fs
import xarray as xr
import numpy as np
import dask
import time
import boto3
from botocore.exceptions import ClientError
import os
from utils import DB
from dask.distributed import Client

logger = logging.getLogger("NWM-Data-logger")

NWM_URL = "s3://noaa-nwm-retrospective-2-1-zarr-pds/chrtout.zarr"
NWM_VARIABLES = ["streamflow", "velocity"]

MISSING_DATA = -9999


class NWM:
    def __init__(self, start_date: str, end_date: str, scheduler: str, comid: int = None, comids: list = None):
        self.start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

        self.comid_collection = False
        self.comid = comid
        if comids is not None:
            self.comids = [np.int32(c) for c in comids]
            self.comid_collection = True
        else:
            self.comids = [self.comid]
        logger.info(f"Process initiated for NWM data download, COMIDS: {comids}, start date: {start_date}, end date: {end_date}")

        self.scheduler = scheduler
        self.client = Client(self.scheduler)

        self.valid_comids = None
        self.missing_comids = None
        self.nearest_comids = None
        self.data = None

    def get(self):
        t0 = time.time()
        warnings.filterwarnings("ignore", category=ResourceWarning)
        logger.info(f"Requesting NWM URL: {NWM_URL}")
        if not self.comid_collection:
            logger.info(f"COMID: {self.comid}")
        else:
            logger.info(f"COMIDS: {self.comids}")

        s3 = s3fs.S3FileSystem(anon=True)
        store = s3fs.S3Map(root=NWM_URL, s3=s3, check=False)
        ds = xr.open_zarr(store=store, consolidated=True)

        logger.info(f"Checking if COMIDS are available from NWM...")
        valid_comids = []
        missing_comids = []
        nearest_comids = {}

        for comid in self.comids:
            try:
                _check = ds.sel(feature_id=comid, method='nearest')
                _found_comid = int(_check.feature_id)
                valid_comids.append(_found_comid)
                DB.create_status(_found_comid)
                if comid != _found_comid:
                    nearest_comids[_found_comid] = comid
                    DB.add_missing(comid, _found_comid)
            except Exception as e:
                missing_comids.append(comid)
                self.comids.remove(comid)
                logger.warn(f"Unable to find NWM data for comid: {comid}.")
                DB.add_missing(comid, None)
        self.valid_comids = valid_comids
        self.missing_comids = missing_comids
        self.nearest_comids = nearest_comids

        t1 = time.time()
        logger.info(f"Opened connection to zarr data store. Runtime: {round(t1-t0, 2)} sec(s)")
        try:
            with dask.config.set(**{'array.slicing.split_large_chunks': True}):
                ds_feature = ds[NWM_VARIABLES].sel(
                    time=slice(f"{self.start_date.year}-{self.start_date.month}-{self.start_date.day}",
                               f"{self.end_date.year}-{self.end_date.month}-{self.end_date.day}")
                ).sel(
                    feature_id=self.comids, method='nearest'
                ).load(optimize_graph=True, traverse=False)
        except Exception as e:
            logger.warn(f"Failed to download data from NWM for COMIDS: {self.comids}, error: {e}")
            return
        t2 = time.time()
        logger.info(f"Data download complete. Runtime: {round((t2-t1)/60, 2)} min(s)")
        try:
            self.data = ds_feature.to_dataframe()
        except Exception as e:
            logger.warn(f"Failed to convert data to pandas dataframe. Error: {e}")
            return
        t3 = time.time()
        logger.info(f"Data converted to dataframe. Runtime: {round(t3-t2, 2)} sec(s)")

    def upload(self):
        logger.info(f"Uploading hdf5 files to s3 bucket")

        # s3 bucket upload
        bucket = os.getenv("NWM_BUCKET", "hms-nwm-data")
        aws_access_key = os.getenv("AWS_ACCESS_KEY")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

        comid_list = self.data.index.unique(level=1).to_list()

        for comid in comid_list:
            comid_df = self.data.query(f"feature_id=={comid}")[NWM_VARIABLES]
            comid_df = comid_df.reset_index()
            comid_df = comid_df.set_index(["time"])

            # Perform data check, such as size test
            if comid_df.shape[0] == 0:
                logger.warn(f"No data found in dataframe for comid: {comid}")
                DB.update_status(comid,
                                 status="INCOMPLETE",
                                 message=f"Data appears to be incomplete, data count: {comid_df.shape[0]}")
                continue

            file_name = f"{comid}.h5"
            file_path = os.path.join("data", "temp", file_name)
            key_path = f"nwm/{comid}.h5"
            if not os.path.exists(os.path.join("data", "temp")):
                os.mkdir(os.path.join("data", "temp"))

            # write file to temporary directory
            try:
                comid_df.to_hdf(file_path, key="df", mode="w")
            except Exception as e:
                status_message = f"Failed to write h5 file. Error: {e}\n"
                DB.update_status(comid, status="FAILED", message=status_message)
                logger.warn(status_message)
                continue

            # upload file
            s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_access_key
            )
            try:
                response = s3_client.upload_file(Filename=file_path, Bucket=bucket, Key=key_path)
            except ClientError as e:
                logger.warn(f"Error attempting upload h5 file for COMID: {comid}. Message: {e}")
                status_message = f"Failed to upload h5 file to s3 bucket. Error: {e}"
                DB.update_status(comid, status="FAILED", message=status_message)
                continue

            # on success delete temp file
            os.remove(file_path)

            # update status database for comid
            DB.update_status(comid, status="COMPLETED", message="Data downloaded and hdf5 file uploaded to s3 bucket.",
                             file_path=key_path, start_date=self.start_date.strftime("%Y-%m-%d %H:%M:%S"),
                             end_date=self.end_date.strftime("%Y-%m-%d %H:%M:%S"))
            logger.info(f"Completed COMID file {file_name} upload to {key_path}")
