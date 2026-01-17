import ftplib
import os
import time
from datetime import datetime
from RICA_parameters.models import ricaparameter

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
        # Common FTPS ports: 21 (explicit FTPS), 990 (implicit FTPS), or custom port
        # Update FTP_PORT in database to use alternative port if 21 is blocked
        return 990  # Default to implicit FTPS port
    
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


def test_ftp_connectivity():
    """Test basic FTP connectivity without SSL/TLS"""
    import socket
    
    FTP_HOST = get_ftp_host()
    
    ports_to_test = [21, 990, 2121, 10021]
    
    print(f"🔍 Testing connectivity to {FTP_HOST}...\n")
    
    for port in ports_to_test:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((FTP_HOST, port))
            sock.close()
            
            if result == 0:
                print(f"✅ Port {port}: OPEN")
            else:
                print(f"❌ Port {port}: CLOSED/FILTERED")
        except Exception as e:
            print(f"❌ Port {port}: ERROR - {e}")
    
    print("\n💡 If all ports show CLOSED/FILTERED, check Windows Firewall settings")


def download_specific_file_ftp(filename, local_directory=get_download_path_from_db(), max_retries=3):
    """
    Download a specific file from the FTP server with multiple connection strategies.
    """
    import socket
    import ssl
    
    FTP_HOST = get_ftp_host()
    FTP_USER = get_ftp_user()
    FTP_PASS = get_ftp_pass()
    REMOTE_DIR = get_ftp_remote_dir()

    os.makedirs(local_directory, exist_ok=True)

    # Different connection strategies - trying explicit FTPS first (most likely to work)
    connection_strategies = [
        {"port": 21, "passive": True, "implicit": False, "timeout": 30, "name": "Explicit FTPS (port 21, passive)"},
        {"port": 21, "passive": False, "implicit": False, "timeout": 30, "name": "Explicit FTPS (port 21, active)"},
        {"port": 990, "passive": True, "implicit": True, "timeout": 30, "name": "Implicit FTPS (port 990, passive)"},
    ]

    for retry, strategy in enumerate(connection_strategies[:max_retries]):
        if retry > 0:
            print(f"\n🔄 Retry attempt {retry + 1}/{max_retries}")
            time.sleep(2)

        ftp_conn = None
        try:
            print(f"\n🔗 Trying: {strategy['name']}")
            print(f"   Host: {FTP_HOST}:{strategy['port']}")
            
            ftp_conn = ftplib.FTP_TLS()
            
            # Set SSL/TLS version (important for compatibility)
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            ftp_conn.ssl_version = ssl.PROTOCOL_TLS
            
            # Connect with timeout
            print(f"   ⏳ Connecting (timeout: {strategy['timeout']}s)...")
            ftp_conn.connect(FTP_HOST, strategy['port'], timeout=strategy['timeout'])
            
            print("   ✓ Connected, authenticating...")
            ftp_conn.login(FTP_USER, FTP_PASS)
            
            print("   ✓ Logged in, securing data channel...")
            ftp_conn.prot_p()  # Enable protection for data channel
            
            print(f"   ✓ Setting {'passive' if strategy['passive'] else 'active'} mode...")
            ftp_conn.set_pasv(strategy['passive'])
            
            print(f"✅ Successfully connected using: {strategy['name']}")

            try:
                ftp_conn.cwd(REMOTE_DIR)
                print(f"✅ Changed to {REMOTE_DIR} on remote server")
            except Exception as e:
                print(f"⚠️ Could not change to {REMOTE_DIR}: {e}")
                print("📁 Listing files in root directory instead")

            # Check if the file exists on the server
            print("   📋 Listing files...")
            files = ftp_conn.nlst()
            if filename not in files:
                print(f"❌ File '{filename}' not found on server.")
                print(f"   Available files (first 10): {files[:10]}")
                ftp_conn.quit()
                return False

            print(f"\n📥 Downloading file: {filename}")
            local_filepath = os.path.join(local_directory, filename)

            # Download the file
            with open(local_filepath, "wb") as local_file:
                ftp_conn.retrbinary(f"RETR {filename}", local_file.write)

            print(f"✅ Successfully downloaded: {local_filepath}")

            # Verify download
            if os.path.exists(local_filepath):
                local_size = os.path.getsize(local_filepath)
                print(f"✅ File size: {local_size} bytes")
            
            ftp_conn.quit()
            return local_filepath

        except socket.timeout as e:
            print(f"❌ Connection timeout on port {strategy['port']}: {e}")
            print(f"   💡 Tip: If passive mode fails, Windows Firewall may block data ports")
        except ConnectionRefusedError as e:
            print(f"❌ Connection refused on port {strategy['port']}: {e}")
        except Exception as e:
            print(f"❌ Download attempt failed: {e}")
            print(f"   Error type: {type(e).__name__}")
        finally:
            if ftp_conn:
                try:
                    ftp_conn.quit()
                except:
                    pass

        if retry < len(connection_strategies) - 1:
            continue
        else:
            print(f"\n❌ All download attempts failed after {max_retries} retries")
            print("\n🔍 Since this worked before, check:")
            print("   1. Recent Windows Updates (may have changed firewall)")
            print("   2. Contact 1&1/IONOS - they may have changed FTPS config")
            print("   3. Try logging in with FileZilla from this server")
            print("   4. Check if server IP changed or got blocked")
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

# if __name__ == "__main__":
    
#     # Download a specific file (user provides the filename)
#     print("\n2️⃣ Downloading specific file...")
#     filename = "RadarSanctionData.json"
#     if filename:
#         result = download_specific_file_ftp(filename)
#         if result:
#             print(f"\n🎯 Success! Downloaded: {result}")
#         else:
#             print("\n❌ Download failed!")
#     else:
#         print("No filename provided. Skipping specific file download.")

# from bots.SanctionListService.SanctionGetter.ftp_download import download_specific_file_ftp
# # Download a specific file (user provides the filename)
# print("\n2️⃣ Downloading specific file...")
# filename = "SanctionData.csv"
# if filename:
#     result = download_specific_file_ftp(filename)
#     if result:
#         print(f"\n🎯 Success! Downloaded: {result}")
#     else:
#         print("\n❌ Download failed!")
# else:
#     print("No filename provided. Skipping specific file download.")