import os
import requests
import datetime
import logging
from tqdm import tqdm
from RICA_parameters.models import ricaparameter


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_download_path_from_db():
    try:
        param = ricaparameter.objects.get(ricaName="downloadPath")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        logger.error("No downloadPath found in rica_parameter table.")
        return None

def get_download_status():
    try:
        param = ricaparameter.objects.get(ricaName="downloadStatus")
        return param.ricaValue
    except ricaparameter.DoesNotExist:
        logger.error("No downloadStatus found in rica_parameter table.")
        return None

def download_file_as_single(url, download_path, base_filename=None):
    """Download a file as a single file with retries and temp file logic"""
    import time
    max_retries = 10
    try:
        os.makedirs(download_path, exist_ok=True)
        if base_filename is None:
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            base_filename = f"opensanctions_{timestamp}"
        filename = f"{base_filename}_single.json"
        temp_filename = filename + ".part"
        filepath = os.path.join(download_path, filename)
        temp_filepath = os.path.join(download_path, temp_filename)

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Downloading entire file as single download... (Attempt {attempt}/{max_retries})")
                logger.info(f"Saving to: {temp_filepath}")
                response = requests.get(url, stream=True, timeout=30)
                response.raise_for_status()
                total_size = int(response.headers.get('content-length', 0))
                downloaded_size = 0
                last_100mb = 0
                with open(temp_filepath, 'wb') as f:
                    if total_size > 0:
                        with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Downloading {os.path.basename(filepath)}") as pbar:
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                                    downloaded_size += len(chunk)
                                    pbar.update(len(chunk))
                                    # Log every 100MB downloaded
                                    if downloaded_size // (100*1024*1024) > last_100mb:
                                        last_100mb = downloaded_size // (100*1024*1024)
                                        logger.info(f"Downloaded {last_100mb * 100} MB...")
                    else:
                        logger.info("Downloading without progress indication...")
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                downloaded_size += len(chunk)
                                if downloaded_size // (100*1024*1024) > last_100mb:
                                    last_100mb = downloaded_size // (100*1024*1024)
                                    logger.info(f"Downloaded {last_100mb * 100} MB...")

                actual_size = os.path.getsize(temp_filepath)
                if total_size > 0 and actual_size < total_size:
                    logger.warning(f"Download incomplete: expected {total_size} bytes, got {actual_size} bytes.")
                    raise Exception("Incomplete download, will retry.")
                # Rename temp file to final file
                os.replace(temp_filepath, filepath)
                logger.info(f"✅ Download completed: {actual_size:,} bytes ({actual_size/1024/1024:.2f} MB)")
                return filepath
            except Exception as e:
                logger.error(f"Error in download attempt {attempt}: {e}")
                if os.path.exists(temp_filepath):
                    os.remove(temp_filepath)
                if attempt < max_retries:
                    logger.info("Retrying in 5 seconds...")
                    time.sleep(5)
                else:
                    logger.error("Max retries reached. Download failed.")
                    return None
    except Exception as e:
        logger.error(f"Error in download_file_as_single: {e}")
        return None

def set_download_status(value):
    try:
        param, _ = ricaparameter.objects.get_or_create(ricaName="downloadStatus")
        param.ricaValue = value
        param.save()
    except Exception as e:
        logger.error(f"Failed to set downloadStatus: {e}")

        
def download_opensanction(url, base_filename=None):
    """Download the file from the given URL to the path from the DB. Returns the file path or None."""
    try:
        download_path = get_download_path_from_db()
    except:
        download_path = "./downloads/"
    try:
        status = get_download_status()
    except:
        status = None

    if not download_path:
        logger.error("Download path could not be determined. Exiting.")
        return None
    # Empty the download folder before downloading
    try:
        for filename in os.listdir(download_path):
            file_path_to_remove = os.path.join(download_path, filename)
            if os.path.isfile(file_path_to_remove) or os.path.islink(file_path_to_remove):
                os.unlink(file_path_to_remove)
            elif os.path.isdir(file_path_to_remove):
                import shutil
                shutil.rmtree(file_path_to_remove)
        logger.info(f"Emptied download folder: {download_path}")
    except Exception as e:
        logger.error(f"Failed to empty download folder: {e}")
        return None
    
    try:
        set_download_status("downloading")

    except Exception as e:
        pass

    file_path = download_file_as_single(url, download_path, base_filename)
    if file_path:
        file_size = os.path.getsize(file_path)
        if file_size < 1024 * 1024:
            logger.warning("Downloaded file is suspiciously small. The download may have failed or the file may be empty.")
        try:
            set_download_status("complete")
        except Exception as e:
            pass

        return file_path
    else:
        set_download_status("failed")
        return None



if __name__ == "__main__":
    # Example usage: pass a URL as argument or hardcode for testing
    url = "https://data.opensanctions.org/datasets/20250814/ru_acf_bribetakers/entities.ftm.json"
    download_opensanction(url)
