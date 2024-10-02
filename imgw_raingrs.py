import os
import requests
import tarfile
from pathlib import Path

from datetime import datetime, timedelta

IMGW_HOST="danepubliczne.imgw.pl/datastore/getfiledown/"
IMGW_MODE_OPER="Oper/Nowcasting/"
IMGW_MODE_ARCH="Arch/Nowcasting/"
IMGW_GRS_DATA="RainGRS/grs_60_asc/"
TIME_DELTA_DEFAULT = timedelta(hours=1) # 1 hour
INCORRECT_FILE_STR="<title>404 Not Found</title>"


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


def get_tar_file(year_now, month_now ,tarname: str):

    archive_url =  "https://" \
                +IMGW_HOST \
                +IMGW_MODE_ARCH \
                +IMGW_GRS_DATA \
                +year_now+"/" \
                +month_now+"/" \
                +tarname
    
    # get file
    response = requests.get(archive_url)
    
    if response.status_code == 200 \
        and INCORRECT_FILE_STR not in str(response.content):
        
        return response

    return None


def get_single_file(filename: str):
    
    file_url = "https://" \
                +IMGW_HOST \
                +IMGW_MODE_OPER \
                +IMGW_GRS_DATA \
                +filename    
    
    # get file
    response = requests.get(file_url)
    
    # check if response is 200 and if file content is correct
    if response.status_code == 200 \
        and INCORRECT_FILE_STR not in str(response.content):
            
            return response
        
    return None
             


def file_exists_locally(local_save_path: str):
    
    if os.path.exists(local_save_path):
        return True
    
    return False


def get_imgw_raingrs_data(start_datetime:datetime, end_datetime:datetime, 
                          points_of_interest=[]):
    
    # checklist
    checklist = {}
    
    # assign current time variable
    current_datetime = start_datetime
    
    # iterate through datetime
    while current_datetime <= end_datetime:
        
        filestatus = False
        
        # tmp data
        datetime_now = current_datetime
        datetime_now += timedelta(days=2)
    
        year_now = datetime_now.strftime("%Y")
        month_now = datetime_now.strftime("%m")
        day_now = datetime_now.strftime("%d")       
        
        # dirname = f"{current_datetime.strftime("%Y%m%d")}/"
        filename = f"{current_datetime.strftime("%Y%m%d%H%M")}_acc0060_grs.asc"        
        tarname = f"grs_60_asc_{year_now}-{month_now}-{day_now}.tar"
    
        # save paths
        file_save_path = os.path.join(Path(__file__).parent / "grs_asc/", filename)
        tar_save_path = os.path.join(Path(__file__).parent / "grs_asc/", tarname)
        tar_extract_save_path = os.path.join(Path(__file__).parent / "grs_asc/")
        
        # 
        print(f"------------------{filename}------------------")
        
        # [1] Check if file already exists locally
        if not file_exists_locally(file_save_path):
            
            print("- Does not exist locally")
            
            # [1a] Request file from OPERational Datastore
            response = get_single_file(filename)
            
            print("- OPER request")
            
            if response is not None:
                
                # save file locally
                os.makedirs(os.path.dirname(file_save_path), exist_ok=True)
                with open(file_save_path, 'wb') as file:
                    file.write(response.content)
                    
                # continue loop
                print("- CONTINUE")
                filestatus=True
                continue
            
            else:
                print("- OPER: invalid content")
            
            # [1b] Request tar, extract it and serach file from ARCHive Datastore
            response = get_tar_file(year_now, month_now, tarname)
            
            print("- ARCH: request")
            
            if response is not None:
                
                # save tar locally
                os.makedirs(os.path.dirname(tar_save_path), exist_ok=True)
                with open(tar_save_path, 'wb') as file:
                    file.write(response.content)
                    
                print("- new tar saved")
                       
                # extract tar files
                extract_tar_file(tar_save_path, tar_extract_save_path)
                
                # check file existence again
                if file_exists_locally(file_save_path):
                    filestatus=True
                    print("- file missing")
                
                
            else:
                print("- ARCH: invalid content")
                
            
        else:
            print(f"- already exists locally!")
            filestatus=True
               
        # update checklist
        checklist[filename] = filestatus 
        
        # increment with time_delta
        current_datetime += TIME_DELTA_DEFAULT       
        
    print(checklist)
        
        
# example use

start = datetime(2024, 9, 26, 15, 00)
end = datetime(2024, 10, 2, 15, 00)
time_delta = timedelta(hours=1)

get_imgw_raingrs_data(start, end)
