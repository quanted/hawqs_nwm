from nwm import NWM
from dask.distributed import Client, LocalCluster


if __name__ == "__main__":

    start_date = "2000-01-01"
    end_date = "2005-12-31"

    comids = [6080293, 6163267]

    scheduler = LocalCluster(n_workers=10, threads_per_worker=2, processes=False)

    nwm = NWM(start_date=start_date, end_date=end_date, scheduler=scheduler, comids=comids)
    nwm.get()
    nwm.upload()
