import ftplib
import os
import time
from datetime import datetime
from RICA_parameters.models import ricaparameter
from tqdm import tqdm

def get_ftp_host():
    try:
        param = ricaparameter.objects.get(ricaName="FTP_HOST")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        print("No FTP_HOST found in rica_parameter table.")
        return None
    
def get_ftp_port():
    try:
        param = ricaparameter.objects.get(ricaName="FTP_PORT")
        return int(param.ricaValue)
    except ricaparameter.DoesNotExist:
        print("No FTP_PORT found in rica_parameter table.")
        return 21  # Default FTP port
    
def get_ftp_user():
    try:
        param = ricaparameter.objects.get(ricaName="FTP_USER")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        print("No FTP_USER found in rica_parameter table.")
        return None
    
def get_ftp_pass():
    try:
        param = ricaparameter.objects.get(ricaName="FTP_PASS")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        print("No FTP_PASS found in rica_parameter table.")
        return None
    
def get_ftp_remote_dir():
    try:
        param = ricaparameter.objects.get(ricaName="FTP_FOLDER")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        print("No FTP_FOLDER found in rica_parameter table.")
        return None
    
def get_download_path_from_db():
    try:
        param = ricaparameter.objects.get(ricaName="downloadPath")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        print("No downloadPath found in rica_parameter table.")
        return None
    

def get_sanction_filename():
    try:
        param = ricaparameter.objects.get(ricaName="SANCTION_FILENAME")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        print("No SANCTION_FILENAME found in rica_parameter table.")
        return None

def get_family_filename():
    try:
        param = ricaparameter.objects.get(ricaName="FAMILY_FILENAME")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        print("No FAMILY_FILENAME found in rica_parameter table.")
        return None


def get_index_filename():
    try:
        param = ricaparameter.objects.get(ricaName="INDEX_FILENAME")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        print("No INDEX_FILENAME found in rica_parameter table.")
        return None


def get_abbreviations_filename():
    try:
        param = ricaparameter.objects.get(ricaName="ABBREVIATIONS_FILENAME")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        print("No ABBREVIATIONS_FILENAME found in rica_parameter table.")
        return None

def get_faiss_index_filename():
    try:
        param = ricaparameter.objects.get(ricaName="FAISS_INDEX_FILENAME")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        print("No FAISS_INDEX_FILENAME found in rica_parameter table.")
        return None

def get_faiss_metadata_filename():
    try:
        param = ricaparameter.objects.get(ricaName="FAISS_METADATA_FILENAME")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        print("No FAISS_METADATA_FILENAME found in rica_parameter table.")
        return None


def download_specific_file_ftp(filename, local_directory=get_download_path_from_db(), max_retries=3):
    """
    Download a specific file from the FTP server.
    """
    FTP_HOST = get_ftp_host()
    FTP_PORT = get_ftp_port()
    FTP_USER = get_ftp_user()
    FTP_PASS = get_ftp_pass()
    REMOTE_DIR = get_ftp_remote_dir()

    os.makedirs(local_directory, exist_ok=True)

    for retry in range(max_retries):
        if retry > 0:
            print(f"\n🔄 Retry attempt {retry + 1}/{max_retries}")
            time.sleep(2)

        ftp_conn = None
        try:
            ftp_conn = ftplib.FTP()
            print(f"🔗 Connecting to {FTP_HOST}:{FTP_PORT} (Plain FTP)")
            ftp_conn.connect(FTP_HOST, FTP_PORT, timeout=30)
            ftp_conn.login(FTP_USER, FTP_PASS)
            ftp_conn.set_pasv(True)
            print(f"✅ Connected to FTP: {FTP_HOST}")

            try:
                ftp_conn.cwd(REMOTE_DIR)
                print(f"✅ Changed to {REMOTE_DIR} on remote server")
            except Exception as e:
                print(f"⚠️ Could not change to {REMOTE_DIR}: {e}")
                print("📁 Listing files in root directory instead")

            # Check if the file exists on the server
            files = ftp_conn.nlst()
            if filename not in files:
                print(f"❌ File '{filename}' not found on server.")
                ftp_conn.quit()
                return False

            print(f"\n📥 Downloading file: {filename}")
            local_filepath = os.path.join(local_directory, filename)

            # Get file size for progress bar
            file_size = None
            try:
                file_size = ftp_conn.size(filename)
            except Exception:
                pass  # Not all FTP servers support SIZE

            with open(local_filepath, "wb") as local_file:
                if file_size:
                    pbar = tqdm(total=file_size, unit='B', unit_scale=True, desc=filename)
                    def write_with_progress(data):
                        local_file.write(data)
                        pbar.update(len(data))
                    ftp_conn.retrbinary(f"RETR {filename}", write_with_progress)
                    pbar.close()
                else:
                    # Fallback: no progress bar if file size unknown
                    ftp_conn.retrbinary(f"RETR {filename}", local_file.write)

            print(f"✅ Successfully downloaded: {local_filepath}")

            # Verify download
            if os.path.exists(local_filepath):
                local_size = os.path.getsize(local_filepath)
                print(f"✅ File size: {local_size} bytes")
            
            ftp_conn.quit()
            return local_filepath

        except Exception as e:
            print(f"❌ Download attempt failed: {e}")
            if ftp_conn:
                try:
                    ftp_conn.quit()
                except:
                    pass

            if retry < max_retries - 1:
                continue
            else:
                print(f"❌ All download attempts failed after {max_retries} retries")
                return False


def download_data():
    sanction_filename = get_sanction_filename()
    family_filename = get_family_filename()
    abbreviation_filename = get_abbreviations_filename()
    faiss_index_filename = get_faiss_index_filename()
    faiss_metadata_filename = get_faiss_metadata_filename()

    all_filenames = [sanction_filename, family_filename, abbreviation_filename, faiss_index_filename, faiss_metadata_filename]
    all_filepath = []

    for filename in all_filenames:
        if filename:
            result = download_specific_file_ftp(filename)
            if result:
                all_filepath.append(result)
                print(f"\n🎯 Success! Downloaded: {result}")
            else:
                print("\n❌ Download failed!")
        else:
            print("No filename provided. Skipping specific file download.")

    return all_filepath

if __name__ == "__main__":
    
    # Download a specific file (user provides the filename)
    print("\n2️⃣ Downloading specific file...")
    filename = "RadarSanctionData.json"
    if filename:
        result = download_specific_file_ftp(filename)
        if result:
            print(f"\n🎯 Success! Downloaded: {result}")
        else:
            print("\n❌ Download failed!")
    else:
        print("No filename provided. Skipping specific file download.")

# from bots.SanctionListService.SanctionGetter.ftp_download import download_specific_file_ftp
# # Download a specific file (user provides the filename)
# print("\n2️⃣ Downloading specific file...")
# filename = "SanctionData.json"
# if filename:
#     result = download_specific_file_ftp(filename)
#     if result:
#         print(f"\n🎯 Success! Downloaded: {result}")
#     else:
#         print("\n❌ Download failed!")
# else:
#     print("No filename provided. Skipping specific file download.")