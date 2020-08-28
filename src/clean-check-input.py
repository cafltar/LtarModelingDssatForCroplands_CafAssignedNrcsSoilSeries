import pathlib
import pandas as pd
import geopandas as gpd
from shapely import ops
import datetime

def assign_series_names(df:pd.DataFrame):
    df_out = df.copy()

    df_out["SeriesName"] = ""

    df_out.loc[(df["Series"] == 1), "SeriesName"] = "Caldwell"
    df_out.loc[(df["Series"] == 2), "SeriesName"] = "Unknown"
    df_out.loc[(df["Series"] == 3), "SeriesName"] = "Latah"
    df_out.loc[(df["Series"] == 4), "SeriesName"] = "Naff"
    df_out.loc[(df["Series"] == 5), "SeriesName"] = "Naff"
    df_out.loc[(df["Series"] == 6), "SeriesName"] = "Palouse"
    df_out.loc[(df["Series"] == 7), "SeriesName"] = "Palouse"
    df_out.loc[(df["Series"] == 8), "SeriesName"] = "Staley"
    df_out.loc[(df["Series"] == 9), "SeriesName"] = "Thatuna"
    df_out.loc[(df["Series"] == 10), "SeriesName"] = "Buried/Altered"

    return df_out

def convert_coord_to_wgs84(df:pd.DataFrame):
    df_gpd = gpd.GeoDataFrame(
        df.copy(), 
        crs = "EPSG:32611",
        geometry=gpd.points_from_xy(df.Easting, df.Northing))
    
    df_gpd = df_gpd.to_crs("EPSG:4326")
    df_gpd = df_gpd.assign(
        Latitude = df_gpd.geometry.y,
        Longitude = df_gpd.geometry.x)

    df_out = (df.copy()
        .assign(
            Latitude = df_gpd.Latitude,
            Longitude = df_gpd.Longitude
        ))

    return df_out

def assign_id2_by_nearest_neighbor(
    df:pd.DataFrame, 
    grid_points:gpd.GeoDataFrame):
    # find the nearest point and return the corresponding ID2 value
    # From: https://gis.stackexchange.com/a/222388

    df_gpd = gpd.GeoDataFrame(
        df.copy(), 
        crs = "EPSG:4326",
        geometry=gpd.points_from_xy(df.Longitude, df.Latitude))

    pts = grid_points.unary_union

    df_gpd["ID2"] = df_gpd.apply(lambda x: near(grid_points, x.geometry, pts), axis = 1)

    df_out = pd.DataFrame(df_gpd)
    return df_out


def near(grid_points, point, pts):
     # find the nearest point and return the corresponding Place value
     # From: https://gis.stackexchange.com/a/222388
     nearest = grid_points.geometry == ops.nearest_points(point, pts)[1]
     result = grid_points[nearest]["ID2"].values[0]
     return result



def main(
    pathSoiSeries: pathlib.Path,
    pathGeorefPoints: pathlib.Path,
    outputDir: pathlib.Path
    ):
    ## Data Transformation
    df_in = pd.read_excel(
        pathSoiSeries, 
        sheet_name="Sheet1",
        usecols=[0,1,2,4],
        names=[
            "ID",
            "Easting",
            "Northing",
            "SeriesName"
        ],
        converters={
            "ID":int, 
            "Easting":float, 
            "Northing":float, 
            "SeriesName":str})
    
    georefPointsIn = gpd.read_file(pathGeorefPoints)

    df = (df_in
        .pipe(convert_coord_to_wgs84)
        .pipe(assign_id2_by_nearest_neighbor, georefPointsIn)
        .sort_values(by="ID2")
    )

    # QC: Checked that IDs match

    # Write files
    date_today = datetime.datetime.now().strftime("%Y%m%d")

    df[["ID2", "SeriesName", "Latitude", "Longitude"]].to_csv(
        outputDir / "CookEastNrcsSoilSeries_{}_P1A1.csv".format(date_today),
        index = False)



if __name__ == "__main__":
    # params
    inputDir = pathlib.Path.cwd() / "input"
    inputWardellSoilSeries = inputDir / "CAF_soil_type.xls"
    inputPathGeorefPoints = inputDir / "cookeast_georeferencepoint_20190924.geojson"
    outputDir = pathlib.Path.cwd() / "output"

    main(
        inputWardellSoilSeries,
        inputPathGeorefPoints,
        outputDir
    )