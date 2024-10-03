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

def transform_epsg(transformer, lattitude, longitude):
    return transformer.transform(lattitude, longitude)

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


def file_exists_locally(local_save_path: str): 
    if os.path.exists(local_save_path):
        return True
    
    return False


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
        if not file_exists_locally(file_save_path):
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
                if file_exists_locally(file_save_path):
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
                if file_exists_locally(file_save_path):
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
                        if file_exists_locally(file_save_path):
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


def get_grs_value(start_datetime, end_datetime, *args):
    
    current_datetime = start_datetime
    
    output = []
    
    while current_datetime <= end_datetime:
        
        # load file 
        filename = f"{current_datetime.strftime("%Y%m%d%H%M")}_acc0060_grs.asc"
        
        file_save_path = os.path.join(
            Path(__file__).parent / "grs_asc", filename)
        
        if file_exists_locally(file_save_path):
            grs = load_data(file_save_path)
            
            for arg in args:
                grid_points, grs_value = rainfall_sum_1point(arg[1], arg[0], grs)
                
                output.append([current_datetime, arg[0], arg[1], grid_points, grs_value])
        
        current_datetime += TIME_DELTA_DEFAULT
    
    return output
   

def estimate_grs_grid_point(pointX, pointY):
    """
    Estimate GRS Grid point based on (X,Y) coordinates in EPSG:2180
    """
    gridX0 = int(round((pointX - GRS_X_OFF) / GRS_RES))
    gridY0 = int(round((pointY - GRS_Y_OFF) / GRS_RES))

    return gridX0, gridY0

                  
def rainfall_sum_1point(pointY, pointX, rainGrsData):

    grsGridPoints=[]
    # estimate 1 coresponding grid point
    gridX0, gridY0 = estimate_grs_grid_point(pointX, pointY)
    grsGridPoints.append((gridX0, gridY0))

    grsValue =  float(rainGrsData[gridY0, gridX0])

    return grsGridPoints, grsValue



def get_imgw_raingrs_data(start_datetime:datetime, end_datetime:datetime, 
                          points_of_interest):
    
    output = []
    
    # get files
    checklist = download_grs_files(start_datetime, end_datetime)   
       
    # checklist pritn
    for key in checklist:
        print(key, checklist[key], end="\n") 
        
    # [2] create records for poi
    if type(points_of_interest) == type(dict()):
        print("poi is dict")
        
        
    elif type(points_of_interest) == type(list()):
        print("poi is list")
        
        for point in points_of_interest:
            
            lat, lon = point[2], point[1]
            
            # print(point, "id=", point[0], "lon=", point[1], "lat=", point[2], end="\n")
            
            # lat long -> grs point
            cordX, cordY = transform_epsg(TRANSFORMER, lat, lon)
            
            # 1 point
            # calculate grs point X, Y
            
            
            # 4 point
            # calculate 4 grs point X, Y
            
            
            # use that grs to get values
            output += get_grs_value(start_datetime, end_datetime, (cordX, cordY))
            
    
    return output
        

########################################################################       
# example use

start = datetime(2024, 7, 26, 15, 00)
end = datetime(2024, 7, 27, 15, 00)
time_delta = timedelta(hours=1)

# example lat/long input (point_of_interest)
poi_dict = {
    "IUNG1": (21.965275, 51.413447),
    "IUNG10": (23.492164, 50.803253),
    "IUNG107": (16.32475, 50.93678),
    "IUNG108": (15.06074, 51.24185),
    "IUNG109": (16.33423, 51.56973),
    "IUNG11": (23.75, 50.7487),
    "IUNG110": (17.794408, 51.216392),
}
poi_list = [
    ("IUNG1", 21.965275, 51.413447),
    ("IUNG10", 23.492164, 50.803253),
    ("IUNG107", 16.32475, 50.93678),
]

output = get_imgw_raingrs_data(start, end, poi_list)

print(output)
