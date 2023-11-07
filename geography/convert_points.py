import argparse
from pprint import pprint
from pathlib import Path

import pandas as pd
from pyproj import CRS
from pyproj.aoi import AreaOfInterest
from pyproj.database import query_utm_crs_info
from pyproj.transformer import Transformer

def convert_points(reference_latitude: float, reference_longitude: float, epsg_code: str, file_name: Path, relative_points: bool) -> None:
    
    # Build the transformers and get the reference point
    crs = CRS.from_epsg(epsg_code)
    proj = Transformer.from_crs(crs.geodetic_crs, crs)
    proj_reverse = Transformer.from_crs(crs, crs.geodetic_crs)
    base_easting, base_northing = proj.transform(reference_longitude, reference_latitude)
    
    # Load in the data
    df = pd.read_csv(file_name)

    # Get the coordinate pairs
    spatial_coordinates = df[["easting", "northing"]].values
    if relative_points:
        # If the points are relative to the reference coordinate, put them in the correct frame
        spatial_coordinates += (base_easting + base_northing)
    
    # Convert to the WGS-84 and add to the dataframe
    coordinates = [proj_reverse.transform(e, n) for (e, n) in spatial_coordinates]
    df[["latitude", "longitude"]] = coordinates
    
    new_fn = file_name.with_stem(f"{file_name.stem}_converted_coordinates")
    df.to_csv(new_fn, index=False)
    print(f"Updated data saved to: {new_fn}")


def get_reference_EPSG(reference_latitude: float, reference_longitude: float) -> str:
    utm_crs_list = query_utm_crs_info(
        datum_name="WGS 84",
        area_of_interest=AreaOfInterest(
            west_lon_degree=reference_longitude,
            east_lon_degree=reference_longitude,
            north_lat_degree=reference_latitude,
            south_lat_degree=reference_latitude,
        ),
    )
    
    # Get the CRS information and extract the numeric code
    result = CRS.from_epsg(utm_crs_list[0].code)
    epsg_code = result.srs.split(":")[1]
    
    # Print the CRS data and return the code
    header = "Reference Point Details"
    print(f"{header}\n{'='*len(header)}")
    pprint(result)
    return epsg_code


if __name__ == "__main__":
    
    # Create the argument parser
    parser = argparse.ArgumentParser(description="Easting & Northing Coordinate Conversion")
    parser.add_argument(
        "-f",
        "--file-name",
        dest="file_name",
        default="",
        type=str,
        help="Filename of the CSV containing the columns 'easting' and 'northing' that need to be converted to 'longitude' and 'latitude'.",
    )
    parser.add_argument(
        "-lon",
        "--longitude",
        dest="longitude",
        type=float,
        help="The reference longitude (E/W) in WGS84 decimal format.",
    )
    parser.add_argument(
        "-lat",
        "--latitude",
        dest="latitude",
        type=float,
        help="The reference latitude (N/S) in WGS84 decimal format.",
    )
    parser.add_argument(
        "-e",
        "--epsg",
        dest="epsg_code",
        type=str,
        default="",
        help="Leave blank for unknown. The reference EPSG Code to convert the the easting, northing pairs into the correct longitude, latitude pairs.",
    )
    parser.add_argument(
        "-F",
        "--find-code",
        dest="find_code",
        action="store_true",
        help="If used, the coordinate reference system information for the reference coordinate pair will be printed, and the program will exit.",
    )
    parser.add_argument(
        "-r",
        "--relative-points",
        dest="relative_points",
        action="store_true",
        help="If used, the easting and northing data will be considered relative to the reference point. The base assumption is that the easting, northing pairs are in their correct coordinate reference system.",
    )

    args = parser.parse_args()
    file_name = Path(args.file_name).resolve()
    reference_longitude = args.longitude
    reference_latitude = args.latitude
    epsg_code = args.epsg_code
    find_code = args.find_code
    relative_points = args.relative_points

    if find_code:
        _ = get_reference_EPSG(reference_latitude=reference_latitude, reference_longitude=reference_longitude)
    else:
        if epsg_code == "":
            epsg_code = get_reference_EPSG(reference_latitude=reference_latitude, reference_longitude=reference_longitude)
        convert_points(reference_latitude=reference_latitude, reference_longitude=reference_longitude, epsg_code=epsg_code, file_name=file_name, relative_points=relative_points)
