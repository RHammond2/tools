"""Script to download ERA5 data over a specified year range and area.

Please read the TODO items before attempting to run this script!

Requirements
------------
cdsapi (see first TODO below)
pandas
xarray
dask
netcdf4
"""

import shutil
import datetime
import argparse
import itertools
import multiprocessing as mp
from pathlib import Path
from functools import partial
from multiprocessing import Pool

import cdsapi
import urllib3
import numpy as np
import xarray as xr
from tqdm import tqdm


# Turn off the annoying warning for:
#   InsecureRequestWarning: Unverified HTTPS request is being made to host 'cds.climate.copernicus.eu'
#   Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# TODO: Register for a free API key from ECMWF for ERA5 access
# NOTE: tutorial located here: https://cds.climate.copernicus.eu/api-how-to
cds_client = cdsapi.Client()


column_map = {
    "time": "datetime",
    "swh": "waveheight",
    "u100": "windspeed_100m_u",
    "v100": "windspeed_100m_v",
    "u10": "windspeed_10m_u",
    "v10": "windspeed_10m_v",
    "shts": "waveheight_swell",
    "shww": "waveheight_wind",
    "pp1d": "wave_period",
    "p140209": "air_density",
    # "rhoao": "air_density",
    "sst": "surface_temperature",
    "sp": "surface_pressure",
}

column_order = [
    "windspeed",
    "waveheight",
    "wind_direction",
    "wind_direction_100m",
    "wind_direction_10m",
    "windspeed_100m",
    "windspeed_10m",
    "windspeed_100m_u",
    "windspeed_100m_v",
    "windspeed_10m_u",
    "windspeed_10m_v",
    "waveheight_swell",
    "waveheight_wind",
    "wave_period",
    "air_density",
    "surface_temperature",
    "surface_pressure",
]


def retrieve_era5_for_year(year, *, data_path, base_fn, area, c=cds_client):
    fn = f"{base_fn}_{year}.grib"

    if (data_path / fn).exists():
        print(f"Skipping {year} because it already exists")
        return

    c.retrieve(
        "reanalysis-era5-single-levels",
        {
            "product_type": "reanalysis",
            "format": "grib",
            "area": area,
            "year": f"{year}",
            "grid": "0.5/0.5",
            "time": [
                "00:00",
                "01:00",
                "02:00",
                "03:00",
                "04:00",
                "05:00",
                "06:00",
                "07:00",
                "08:00",
                "09:00",
                "10:00",
                "11:00",
                "12:00",
                "13:00",
                "14:00",
                "15:00",
                "16:00",
                "17:00",
                "18:00",
                "19:00",
                "20:00",
                "21:00",
                "22:00",
                "23:00",
            ],
            "day": [
                "01",
                "02",
                "03",
                "04",
                "05",
                "06",
                "07",
                "08",
                "09",
                "10",
                "11",
                "12",
                "13",
                "14",
                "15",
                "16",
                "17",
                "18",
                "19",
                "20",
                "21",
                "22",
                "23",
                "24",
                "25",
                "26",
                "27",
                "28",
                "29",
                "30",
                "31",
            ],
            "month": [
                "01",
                "02",
                "03",
                "04",
                "05",
                "06",
                "07",
                "08",
                "09",
                "10",
                "11",
                "12",
            ],
            "variable": [
                "100m_u_component_of_wind",
                "100m_v_component_of_wind",
                "10m_u_component_of_wind",
                "10m_v_component_of_wind",
                "air_density_over_the_oceans",
                "peak_wave_period",
                "sea_surface_temperature",
                "significant_height_of_combined_wind_waves_and_swell",
                "significant_height_of_total_swell",
                "significant_height_of_wind_waves",
                "surface_pressure",
            ],
        },
        fn,
    )
    fn = Path(fn).resolve()
    shutil.move(fn, data_path)
    print(f"{year} downloaded")


def load_grib(base_fn, year):
    return xr.load_dataset(data_path / f"{base_fn}_{year}.grib")


def calculate_additional_columns(ds):
    ds = ds.assign(
        windspeed_10m=np.sqrt(ds.u10**2 + ds.v10**2),
        windspeed_100m=np.sqrt(ds.u100**2 + ds.v100**2),
        wind_direction_10m=180 + np.arctan2(ds.u10, ds.v10) * 180 / np.pi,
        wind_direction_100m=180 + np.arctan2(ds.u100, ds.v100) * 180 / np.pi,
    )
    ds["wind_direction_10m"] = xr.where(
        ds.wind_direction_10m != 360, ds.wind_direction_10m, 0
    )
    ds["wind_direction_100m"] = xr.where(
        ds.wind_direction_100m != 360, ds.wind_direction_100m, 0
    )
    ds = ds.assign(
        windspeed=ds.windspeed_100m,
        wind_direction=ds.wind_direction_100m,
    )
    return ds


if __name__ == "__main__":
    # Create the argument parser
    parser = argparse.ArgumentParser(description="ERA5 download and combination script")
    parser.add_argument(
        "-p",
        "--path-name",
        dest="data_path",
        type=str,
        help="Path to save all downloaded and combined data.",
    )
    parser.add_argument(
        "-f",
        "--file-name",
        dest="base_fn",
        type=str,
        help="Base filename to use for all downloaded files, e.g., <base_fn>_year.csv.",
    )
    parser.add_argument(
        "-s",
        "--start-year",
        dest="start_year",
        type=int,
        help="First year to gather data, must not be lower than 1959",
    )
    parser.add_argument(
        "-e",
        "--end_year",
        dest="end_year",
        type=int,
        help="Last year to gather data, must not be greater than the current year.",
    )
    parser.add_argument(
        "-a",
        "--area",
        dest="area",
        nargs=4,
        type=float,
        help="The N, W, S, and E boundaries of the grid. N and S, and E and W can be the same for a single coordinate pair.",
    )
    parser.add_argument(
        "-n",
        "--nodes",
        dest="nodes",
        type=int,
        default=1,
        help="Number of CPU cores to run on, should not be total number of cores or your computer will have issues. Defaults to 1.",
    )

    args = parser.parse_args()
    data_path = Path(args.data_path).resolve()
    base_fn = args.base_fn
    start_year = args.start_year
    end_year = args.end_year
    area = args.area
    nodes = args.nodes

    era5_call = partial(
        retrieve_era5_for_year,
        data_path=data_path,
        base_fn=base_fn,
        c=cds_client,
        area=area,
    )

    # Downloads the data
    year_list = list(range(start_year, end_year + 1))
    N = len(year_list)

    with Pool(nodes) as pool:
        pool.map(era5_call, year_list)

    # Loads in existing NetCDF data, or loads and saves the data to NetCDF
    if (fn := data_path / f"{base_fn}.nc").exists():
        print("Loading existing dataset")
        ds = xr.load_dataset(fn, engine="netcdf4")
    else:
        ds_list = []
        load_grib_partial = partial(load_grib, base_fn)
        with Pool(nodes) as pool:
            with tqdm(total=N, desc="Loading grib data") as pbar:
                for ds in pool.imap_unordered(load_grib_partial, year_list):
                    ds_list.append(ds)
                    pbar.update()

        print("Combining years into a single data set")
        ds = xr.concat(ds_list, dim="time").sortby("time")

        print("Computing additional columns")
        ds = calculate_additional_columns(ds)
        ds = ds.rename(column_map)

        print("Saving combined data")
        ds.to_netcdf(data_path / f"{base_fn}.nc", engine="netcdf4")

    # Saves each coordinate as its own CSV file
    print("Saving each coordinate as a separate CSV")
    print("\n==============================================================\n")
    combinations = itertools.product(ds.latitude.values, ds.longitude.values)
    for latitude, longitude in combinations:
        df = ds.where(
            (ds.latitude == latitude) & (ds.longitude == longitude), drop=True
        ).to_dataframe()
        df.index = df.index.droplevel(["longitude", "latitude"])

        # TODO: CHANGE FOR THE APPROPRIATE TIMEZONE
        df.index -= datetime.timedelta(hours=5)

        df = df.drop(["number", "step", "valid_time", "meanSea"], axis=1)
        df = df[column_order]
        print(latitude, longitude)
        print(df.shape)
        print(df.describe().T)
        fn = data_path / f"{base_fn}_{latitude}_{longitude}.csv"
        df.to_csv(fn, index_label="datetime", date_format="%m-%d-%Y %H:%M")
        print()
        print(f"saved to: {fn}")
        print("\n==============================================================\n")
