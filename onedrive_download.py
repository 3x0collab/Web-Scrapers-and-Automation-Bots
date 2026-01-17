import os
import time
import requests
from datetime import datetime
from RICA_parameters.models import ricaparameter
from tqdm import tqdm

def get_onedrive_client_id():
    """Get OneDrive Client ID from database"""
    try:
        param = ricaparameter.objects.get(ricaName="ONEDRIVE_CLIENT_ID")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        print("No ONEDRIVE_CLIENT_ID found in rica_parameter table.")
        return None

def get_onedrive_client_secret():
    """Get OneDrive Client Secret from database"""
    try:
        param = ricaparameter.objects.get(ricaName="ONEDRIVE_CLIENT_SECRET")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        print("No ONEDRIVE_CLIENT_SECRET found in rica_parameter table.")
        return None

def get_onedrive_tenant_id():
    """Get OneDrive Tenant ID from database"""
    try:
        param = ricaparameter.objects.get(ricaName="ONEDRIVE_TENANT_ID")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        print("No ONEDRIVE_TENANT_ID found in rica_parameter table.")
        return None

def get_onedrive_user_email():
    """Get OneDrive User Email from database"""
    try:
        param = ricaparameter.objects.get(ricaName="ONEDRIVE_USER_EMAIL")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        print("No ONEDRIVE_USER_EMAIL found in rica_parameter table.")
        return None

def get_onedrive_folder_path():
    """Get OneDrive remote folder path from database"""
    try:
        param = ricaparameter.objects.get(ricaName="ONEDRIVE_FOLDER")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        print("No ONEDRIVE_FOLDER found in rica_parameter table.")
        return None
    
def get_download_path_from_db():
    """Get local download path from database"""
    try:
        param = ricaparameter.objects.get(ricaName="downloadPath")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        print("No downloadPath found in rica_parameter table.")
        return None
    

def get_sanction_filename():
    """Get sanction filename from database"""
    try:
        param = ricaparameter.objects.get(ricaName="SANCTION_FILENAME")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        print("No SANCTION_FILENAME found in rica_parameter table.")
        return None

def get_family_filename():
    """Get family filename from database"""
    try:
        param = ricaparameter.objects.get(ricaName="FAMILY_FILENAME")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        print("No FAMILY_FILENAME found in rica_parameter table.")
        return None


def get_index_filename():
    """Get index filename from database"""
    try:
        param = ricaparameter.objects.get(ricaName="INDEX_FILENAME")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        print("No INDEX_FILENAME found in rica_parameter table.")
        return None


def get_abbreviations_filename():
    """Get abbreviations filename from database"""
    try:
        param = ricaparameter.objects.get(ricaName="ABBREVIATIONS_FILENAME")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        print("No ABBREVIATIONS_FILENAME found in rica_parameter table.")
        return None

def get_faiss_index_filename():
    """Get FAISS index filename from database"""
    try:
        param = ricaparameter.objects.get(ricaName="FAISS_INDEX_FILENAME")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        print("No FAISS_INDEX_FILENAME found in rica_parameter table.")
        return None

def get_faiss_metadata_filename():
    """Get FAISS metadata filename from database"""
    try:
        param = ricaparameter.objects.get(ricaName="FAISS_METADATA_FILENAME")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        print("No FAISS_METADATA_FILENAME found in rica_parameter table.")
        return None


def get_access_token():
    """
    Get access token for Microsoft Graph API using client credentials flow.
    """
    client_id = get_onedrive_client_id()
    client_secret = get_onedrive_client_secret()
    tenant_id = get_onedrive_tenant_id()
    
    if not all([client_id, client_secret, tenant_id]):
        print("❌ Missing OneDrive credentials in database")
        return None
    
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    
    token_data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'https://graph.microsoft.com/.default',
        'grant_type': 'client_credentials'
    }
    
    try:
        response = requests.post(token_url, data=token_data)
        response.raise_for_status()
        return response.json().get('access_token')
    except Exception as e:
        print(f"❌ Failed to get access token: {e}")
        return None


def download_specific_file_onedrive(filename, local_directory=None, max_retries=3):
    """
    Download a specific file from OneDrive using Microsoft Graph API.
    
    Args:
        filename: Name of the file to download
        local_directory: Local directory to save the file (defaults to downloadPath from DB)
        max_retries: Maximum number of retry attempts
    
    Returns:
        str: Local filepath if successful, False otherwise
    """
    if local_directory is None:
        local_directory = get_download_path_from_db()
    
    if not local_directory:
        print("❌ No local directory specified")
        return False
    
    os.makedirs(local_directory, exist_ok=True)
    
    user_email = get_onedrive_user_email()
    onedrive_folder = get_onedrive_folder_path()
    
    if not user_email:
        print("❌ No OneDrive user email configured")
        return False
    
    for retry in range(max_retries):
        if retry > 0:
            print(f"\n🔄 Retry attempt {retry + 1}/{max_retries}")
            time.sleep(2)
        
        try:
            # Get access token
            access_token = get_access_token()
            if not access_token:
                print("❌ Could not obtain access token")
                if retry < max_retries - 1:
                    continue
                return False
            
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            
            print(f"🔗 Connecting to OneDrive for user: {user_email}")
            
            # Construct the file path
            if onedrive_folder:
                # Remove leading/trailing slashes and normalize path
                folder_path = onedrive_folder.strip('/').strip('\\')
                file_path = f"{folder_path}/{filename}" if folder_path else filename
            else:
                file_path = filename
            
            # Build the Graph API URL
            # Using /users/{user-id}/drive/root:/{path}:/content endpoint
            graph_url = f"https://graph.microsoft.com/v1.0/users/{user_email}/drive/root:/{file_path}:/content"
            
            print(f"📥 Downloading file: {filename}")
            print(f"📂 From path: {file_path}")
            
            # Get file metadata first to check size
            metadata_url = f"https://graph.microsoft.com/v1.0/users/{user_email}/drive/root:/{file_path}"
            metadata_response = requests.get(metadata_url, headers=headers)
            
            if metadata_response.status_code == 404:
                print(f"❌ File '{filename}' not found on OneDrive.")
                return False
            
            metadata_response.raise_for_status()
            file_metadata = metadata_response.json()
            file_size = file_metadata.get('size', 0)
            
            # Download the file with progress bar
            response = requests.get(graph_url, headers=headers, stream=True)
            response.raise_for_status()
            
            local_filepath = os.path.join(local_directory, filename)
            
            with open(local_filepath, 'wb') as local_file:
                if file_size > 0:
                    pbar = tqdm(total=file_size, unit='B', unit_scale=True, desc=filename)
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            local_file.write(chunk)
                            pbar.update(len(chunk))
                    pbar.close()
                else:
                    # Fallback: no progress bar if file size unknown
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            local_file.write(chunk)
            
            print(f"✅ Successfully downloaded: {local_filepath}")
            
            # Verify download
            if os.path.exists(local_filepath):
                local_size = os.path.getsize(local_filepath)
                print(f"✅ File size: {local_size} bytes")
            
            return local_filepath
            
        except requests.exceptions.HTTPError as e:
            print(f"❌ HTTP error occurred: {e}")
            if e.response.status_code == 401:
                print("❌ Authentication failed. Please check your credentials.")
            elif e.response.status_code == 403:
                print("❌ Access denied. Please check permissions.")
            elif e.response.status_code == 404:
                print(f"❌ File or path not found: {file_path}")
            
            if retry < max_retries - 1:
                continue
            else:
                print(f"❌ All download attempts failed after {max_retries} retries")
                return False
                
        except Exception as e:
            print(f"❌ Download attempt failed: {e}")
            
            if retry < max_retries - 1:
                continue
            else:
                print(f"❌ All download attempts failed after {max_retries} retries")
                return False


def download_data():
    """
    Download all configured sanction data files from OneDrive.
    
    Returns:
        list: List of local filepaths for successfully downloaded files
    """
    sanction_filename = get_sanction_filename()
    family_filename = get_family_filename()
    abbreviation_filename = get_abbreviations_filename()
    faiss_index_filename = get_faiss_index_filename()
    faiss_metadata_filename = get_faiss_metadata_filename()

    all_filenames = [sanction_filename, family_filename, abbreviation_filename, faiss_index_filename, faiss_metadata_filename]
    all_filepath = []

    for filename in all_filenames:
        if filename:
            result = download_specific_file_onedrive(filename)
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
    print("\n📥 Downloading specific file from OneDrive...")
    filename = "RadarSanctionData.json"
    if filename:
        result = download_specific_file_onedrive(filename)
        if result:
            print(f"\n🎯 Success! Downloaded: {result}")
        else:
            print("\n❌ Download failed!")
    else:
        print("No filename provided. Skipping specific file download.")

# Usage example:
# from bots.SanctionListService.SanctionGetter.onedrive_download import download_specific_file_onedrive
# 
# # Download a specific file
# print("\n📥 Downloading specific file from OneDrive...")
# filename = "RadarSanctionData.json"
# if filename:
#     result = download_specific_file_onedrive(filename)
#     if result:
#         print(f"\n🎯 Success! Downloaded: {result}")
#     else:
#         print("\n❌ Download failed!")
# else:
#     print("No filename provided. Skipping specific file download.")
