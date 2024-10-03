import os
import requests
import rasterio
import tarfile
import numpy as np 
from pathlib import Path

from datetime import datetime, timedelta

from pyproj import Transformer

IMGW_HOST="danepubliczne.imgw.pl/datastore/getfiledown/"
IMGW_MODE_OPER="Oper/Nowcasting/"
IMGW_MODE_ARCH="Arch/Nowcasting/"
IMGW_GRS_DATA="RainGRS/grs_60_asc/"
TIME_DELTA_DEFAULT = timedelta(hours=1) # 1 hour
INCORRECT_FILE_STR="<title>404 Not Found</title>"

GRS_RES=1000
GRS_X_OFF=50000
GRS_Y_OFF=30000

INPUT_CRS="EPSG:4326"
OUTPUT_CRS="EPSG:2180"
TRANSFORMER=Transformer.from_crs(INPUT_CRS, OUTPUT_CRS)


def parse_metadata(file_path):
    with open(file_path, 'r') as f:
        # Read the first 6 lines
        header_lines = [next(f) for _ in range(6)]

    # Parse metadata from header lines
    metadata = {}
    for line in header_lines:
        key, value = line.split()
        metadata[key.strip()] = value.strip()

    return {
        "ncols": int(metadata['ncols']),
        "nrows": int(metadata['nrows']),
        "xllcorner": float(metadata['xllcorner']),
        "yllcorner": float(metadata['yllcorner']),
        "cellsize": int(metadata['cellsize']),
        "nodata_value": float(metadata['NODATA_value'])
    }


def load_data(file_path):
    with rasterio.open(file_path) as src:
        grs_rain_data = src.read(1, masked=True)

    metadata = parse_metadata(file_path)
    
    return np.ma.masked_equal(grs_rain_data, metadata['nodata_value'])


def file_exists(local_save_path: str): 
    if os.path.exists(local_save_path):
        return True
    
    return False


# file downloader
def download_grs_files(start_datetime: datetime, end_datetime: datetime):

    def get_single_file(filename: str):
        """_summary_

        Args:
            filename (str): _description_

        Returns:
            _type_: _description_
        """
        file_url = f"https://{IMGW_HOST}{IMGW_MODE_OPER}{IMGW_GRS_DATA}{filename}"    
        
        # get file
        response = requests.get(file_url)
        
        # check if response is 200 and if file content is correct
        if response.status_code == 200 \
            and INCORRECT_FILE_STR not in str(response.content):
                
                return response
            
        return None   
    
    
    def get_tar_file(year_now, month_now ,tarname: str):

        archive_url =  f"https://{IMGW_HOST}{IMGW_MODE_ARCH}{IMGW_GRS_DATA}{year_now}/{month_now}/{tarname}"
        
        # get file
        response = requests.get(archive_url)
        
        if response.status_code == 200 \
            and INCORRECT_FILE_STR not in str(response.content):
            
            return response

        return None
    

    def extract_tar_file(tar_file_path, extract_to_path):
        # Check if the .tar file exists
        if os.path.exists(tar_file_path):
            # Open the tar file
            with tarfile.open(tar_file_path, "r") as tar:
                # Extract all the contents to the target directory
                tar.extractall(path=extract_to_path)
                print(f"- Extracted all files to {extract_to_path}")
        else:
            print(f"- The file {tar_file_path} does not exist.")
    

    # checklist , empty dict
    checklist = {}
    
    current_datetime = start_datetime # datetime now
    
    # iterate through datetime
    while current_datetime <= end_datetime:
        
        print(f"--------------------{current_datetime}--------------------")
        
        # desired file status
        filestatus = False
        
        # construct filename
        filename = f"{current_datetime.strftime("%Y%m%d%H%M")}_acc0060_grs.asc"
        print(f"--{filename}")
        
        # construct savepath
        file_save_path = os.path.join(
            Path(__file__).parent / "grs_asc", filename)
        
        # check if file already exists in local dir
        if not file_exists(file_save_path):
            # does not exist
            # [1] request file from OPER datastore
            response = get_single_file(filename)
            print("--request from OPER")
            
            # check response content
            if response is not None:
                
                # save file
                os.makedirs(os.path.dirname(file_save_path), exist_ok=True)
                with open(file_save_path, 'wb') as file:
                    file.write(response.content)
                    
                # check existence again
                # OPTIONAL
                if file_exists(file_save_path):
                    filestatus = True
                    print("--exists")
                    # CONTINUE loop
                    continue
                
            else:
                # invalid content of file
                print("--invalid content")
                
            
            # [2] request tar file from ARCH datastore
            # tmp data
            datetime_now = current_datetime
            datetime_now += timedelta(days=2)
    
            year_now = datetime_now.strftime("%Y")
            month_now = datetime_now.strftime("%m")
            day_now = datetime_now.strftime("%d") 
            
            tarname = f"grs_60_asc_{year_now}-{month_now}-{day_now}.tar" 
            
            response = get_tar_file(year_now, month_now, tarname)
            print("--request from ARCH")

            # check response content
            if response is not None:
                
                # save tar
                tar_save_path = os.path.join(
                    Path(__file__).parent / "grs_asc/", tarname)
                
                os.makedirs(os.path.dirname(tar_save_path), exist_ok=True)
                with open(tar_save_path, 'wb') as file:
                    file.write(response.content)
                print("--tar saved")
                
                # extract files    
                tar_extract_save_path = os.path.join(
                    Path(__file__).parent / "grs_asc/")
                
                extract_tar_file(tar_save_path, tar_extract_save_path)
                print("--tar extracted")
                
                # check file existence
                if file_exists(file_save_path):
                    print("--exists")
                    filestatus=True
                else:
                    datetime_now = current_datetime
                    datetime_now += timedelta(days=1)
            
                    year_now = datetime_now.strftime("%Y")
                    month_now = datetime_now.strftime("%m")
                    day_now = datetime_now.strftime("%d") 
                    tarname = f"grs_60_asc_{year_now}-{month_now}-{day_now}.tar"
                    response = get_tar_file(year_now, month_now, tarname)
                    
                    # check response content
                    if response is not None:
                        
                        # save tar
                        tar_save_path = os.path.join(
                            Path(__file__).parent / "grs_asc/", tarname)
                        
                        os.makedirs(os.path.dirname(tar_save_path), exist_ok=True)
                        with open(tar_save_path, 'wb') as file:
                            file.write(response.content)
                        print("--tar saved")
                        
                        # extract files    
                        tar_extract_save_path = os.path.join(
                            Path(__file__).parent / "grs_asc/")
                        
                        extract_tar_file(tar_save_path, tar_extract_save_path)
                        print("--tar extracted")
                        
                        # check file existence
                        if file_exists(file_save_path):
                            print("--exists")
                            filestatus=True
            
            else:
                print("--invalid tar content")
                            
        else:
            # exists
            filestatus=True
            print("--exists")
            
        # update checklist
        checklist[filename] = filestatus 
        
        # increment with time_delta
        current_datetime += TIME_DELTA_DEFAULT
    
    # return filechecklist    
    return checklist    


def grs_value(start_datetime, end_datetime, points_list):
    
    current_datetime = start_datetime
    
    output = []
    
    while current_datetime <= end_datetime:
         
        filename = f"{current_datetime.strftime("%Y%m%d%H%M")}_acc0060_grs.asc"
        file_save_path = os.path.join(
            Path(__file__).parent / "grs_asc", filename)
        
        if file_exists(file_save_path):
            grs = load_data(file_save_path)
            
            for point in points_list:
                
                grs_pcpn = float(grs[point[0], point[1]])
                output.append([
                    current_datetime.strftime("%Y-%m-%d-%H:%M"), 
                    point[0], point[1], 
                    point[2], point[3], 
                    grs_pcpn])
        
        current_datetime += TIME_DELTA_DEFAULT
    
    return output
   

def point_to_grs_point(y, x):
    """
    Estimate GRS Grid point based on (X,Y) coordinates in EPSG:2180
    """
    gridX0 = int(round((x - GRS_X_OFF) / GRS_RES))
    gridY0 = int(round((y - GRS_Y_OFF) / GRS_RES))

    return gridY0, gridX0

                          
# Transformer functions
def transform_epsg(transformer, lattitude, longitude):
    return transformer.transform(lattitude, longitude)


# 1 point estimate
def estimate_1_point(grid_y, grid_x):  
    return point_to_grs_point(grid_y, grid_x)


# 4 point estimate TODO
def grs_4point(grid_y, grid_x):
    pass   


# MAJOR function
def imgw_raingrs_data(
    start_datetime: datetime,
    end_datetime: datetime,
    points_of_interest: list
) -> list:
    """_summary_

    Args:
        start_datetime (datetime): _description_
        end_datetime (datetime): _description_
        points_of_interest (list): [lattitude, longitude]

    Returns:
        list: list of lists ["datetime", "grs_x", "grs_y", "lat", "lon", "value"]
    """
    
    output = [["datetime", "grs_x", "grs_y", "lat", "lon", "value"]]
    
    file_checklist = download_grs_files(start_datetime, end_datetime)
    
    if points_of_interest:
        
        for point in points_of_interest:
            
            # longitude, lattitude
            lat, lon = point[0], point[1]
            
            # lon, lat -> grid points
            grid_y, grid_x = transform_epsg(TRANSFORMER, lat, lon)
            
            # estimate grs grid point (1)
            grs_grid_y, grs_grid_x = estimate_1_point(grid_y, grid_x)
            
            # estimate grs grid points (4)
            # TODO
            
            # retrive grs pcpn values
            output += grs_value(
                start_datetime, 
                end_datetime, 
                [(grs_grid_x, grs_grid_y, lon, lat)])
            
    else:
        pass
    
    return output
            
    
# example use

start = datetime(2024, 7, 20, 15, 00)
end = datetime(2024, 7, 27, 15, 00)
time_delta = timedelta(hours=1)

# example lon/lat input (point_of_interest)
# poi_dict = {
#     "IUNG1": (21.965275, 51.413447),
#     "IUNG10": (23.492164, 50.803253),
#     "IUNG107": (16.32475, 50.93678),
#     "IUNG108": (15.06074, 51.24185),
#     "IUNG109": (16.33423, 51.56973),
#     "IUNG11": (23.75, 50.7487),
#     "IUNG110": (17.794408, 51.216392),
# }
poi_list = [
    (51.413447, 21.965275),
    (50.803253, 23.492164),
    (50.93678, 16.32475),
]

# Final result
output = imgw_raingrs_data(start, end, poi_list)

for record in output:
    print(record, end="\n")

# construct coordinates validator
# based on: https://epsg.io/transform#s_srs=4326&t_srs=2180&x=21.9652750&y=51.4134470