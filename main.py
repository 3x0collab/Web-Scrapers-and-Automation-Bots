
# Worker executed in a ThreadPool to map or skip a single record.

from asyncio import subprocess
import xml.etree.ElementTree as ET
import os
import sys
import logging
import pandas as pd
import tempfile
import requests
import re
import functools
import copy
import csv
import pdfplumber
import tabula
from urllib.parse import urlparse
import zipfile
from pathlib import Path

import threading
import csv
from time import sleep
from typing import Optional, Dict
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed  # NEW
# Import our comprehensive multi-language translation system
from .multi_language_translator import translate_multi_language



# Add PDF extractor import
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
# Remove None values from sys.path to avoid multiprocessing issues on Windows
sys.path = [p for p in sys.path if p is not None]
import subprocess

# Define MAX_WORKERS for ThreadPoolExecutor usage
MAX_WORKERS = min(32, (os.cpu_count() or 1) * 5)


# --- Translation Imports ---
import unicodedata
import string
import traceback
from tempfile import NamedTemporaryFile
import threading
try:
    import argostranslate.package
    import argostranslate.translate
except ImportError:
    argostranslate = None


# Ensure the parent directory is in sys.path for absolute imports
PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

# Set Django settings before any Django/project imports
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ricabackend.settings")
os.environ.setdefault("DJANGO_ALLOW_ASYNC_UNSAFE", "true")

import django
django.setup()

# Now use absolute imports for project modules
from SanctionListService.config import time_formatter, date_formatter, DTTYPES, MAP, language, mapper
from SanctionListService.utils import logError, get_sqlite_con_dir, get_env_settings, process_relation_name, createWatchlistId
from pep_and_sanctions.models import rica_Watchlist, rica_watchlist_log, rica_temp_watchlist, rica_alias, RicaCrawlerRegistry
from RICA_sanctionRegulators.models import ricasanctionRegulators  
from rica_sanctionsubscriber.models import ricasanctionsubscriber
from RICA_spf.models import ricaspf
from RICA_Messages.models import ricaMessages
from django.core.exceptions import ObjectDoesNotExist
from reportsheet import gen_report as report
from django.db import connection
from django.apps import apps
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import functools
from asgiref.sync import sync_to_async
import copy
import csv
import requests
import django
import xml.etree.ElementTree as ET
from datetime import datetime as dt
from datetime import datetime
import time 
import re
import json
from django.core.wsgi import get_wsgi_application
import requests
import PyPDF2
import pdfplumber
import pandas as pd
import re
import os
import tempfile
import json
import functools
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging
# --- Auto-discover crawler functions ---
import importlib
import pkgutil
from pep_and_sanctions.models import RicaCrawlerRegistry






def update_family_relationships_with_fullnames_generic(target_model):
    """
    Populate ricaSpouse, ricaChildren, ricaParents, and ricaRelative fields in the given model with full names from relationships.
    target_model: Django model class (e.g., rica_Watchlist or rica_temp_watchlist)
    """
    from collections import defaultdict
    from pep_and_sanctions.models import rica_watchlist_family
    spouse_map = defaultdict(list)
    children_map = defaultdict(list)
    parents_map = defaultdict(list)
    relative_map = defaultdict(list)
    # Preload all full names for fast lookup
    id_to_fullname = dict(target_model.objects.values_list('ricaWatchlistId', 'ricaFullName'))
    # Fetch all family relationships
    families = rica_watchlist_family.objects.all()
    for rel in families:
        person_id = rel.ricaPerson
        relative_id = rel.ricaRelative
        relationship = rel.ricaRelationship or ''
        relative_fullname = id_to_fullname.get(relative_id, None)
        if not relative_fullname:
            continue
        if 'spouse' in relationship.lower():
            spouse_map[person_id].append(relative_fullname)
        elif relationship == 'Child/Parent':
            children_map[person_id].append(relative_fullname)
        elif relationship == 'Parent/Child':
            parents_map[person_id].append(relative_fullname)
        else:
            relative_map[person_id].append(relative_fullname)
    all_ids = set(list(spouse_map.keys()) + list(children_map.keys()) + list(parents_map.keys()) + list(relative_map.keys()))
    for person_id in all_ids:
        try:
            obj = target_model.objects.get(ricaWatchlistId=person_id)
            obj.ricaSpouse = ', '.join(spouse_map[person_id]) if spouse_map[person_id] else None
            obj.ricaChildren = ', '.join(children_map[person_id]) if children_map[person_id] else None
            obj.ricaParents = ', '.join(parents_map[person_id]) if parents_map[person_id] else None
            obj.ricaRelative = ', '.join(relative_map[person_id]) if relative_map[person_id] else None
            obj.save()
        except target_model.DoesNotExist:
            continue
    print(f"Family relationships (full names) updated for {target_model.__name__}.")

def update_family_relationships_with_fullnames():
    """
    Populate ricaSpouse, ricaChildren, and ricaParents fields in rica_Watchlist with full names from relationships.
    """
    from pep_and_sanctions.models import rica_Watchlist
    update_family_relationships_with_fullnames_generic(rica_Watchlist)

def update_temp_family_relationships_with_fullnames():
    """
    Populate ricaSpouse, ricaChildren, ricaParents, and ricaRelative fields in rica_temp_watchlist with full names from relationships.
    """
    from pep_and_sanctions.models import rica_temp_watchlist
    update_family_relationships_with_fullnames_generic(rica_temp_watchlist)




def get_memory_usage():
    """Get current memory usage in MB."""
    try:
        import psutil
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024
    except ImportError:
        return 0



def _process_record(rec, temp_table_records):
    watchlist_id = rec.get("ricaWatchlistId")

    try:
       
        exists = rica_Watchlist.objects.filter(ricaWatchlistId=watchlist_id).exists()
    except Exception as e:

        import traceback
        traceback.print_exc()
        raise

    if not exists:
        
        try:
            map_data(rec, temp_table_records=temp_table_records)
            return ("new", rec)
        except Exception as e:
            
            import traceback
            traceback.print_exc()
            raise
    else:
        
        reason = "Duplicate ricaWatchlistId" if watchlist_id else "Missing ricaWatchlistId"
        return ("skip", rec, reason)
    
    
    
def discover_crawlers():
    crawler_map = {}
    crawler_pkg = 'SanctionListService.crawler'
    crawler_dir = os.path.join(os.path.dirname(__file__), 'crawler')
    for _, module_name, _ in pkgutil.iter_modules([crawler_dir]):
        if module_name.startswith("__"):
            continue
        
        try:
            mod = importlib.import_module(f"{crawler_pkg}.{module_name}")
            # Find all functions starting with 'run_'
            for attr in dir(mod):
                if attr.startswith("run_") and callable(getattr(mod, attr)):
                    # Use uppercase function name as key (e.g., RUN_UN_LIST)
                    crawler_map[attr.upper()] = getattr(mod, attr)
        except Exception as e:
            print(f"Failed to import {module_name}: {e}")
    return crawler_map

crawler_map = discover_crawlers()

sql_dir = get_sqlite_con_dir()
sys.path.append(sql_dir)
print('backend path: ',sql_dir)

application = get_wsgi_application()

running_flag = True
processed_titles = set()
skipped_table_set = {}

def configure_logging():
    log_file = os.path.join(os.path.dirname(__file__), 'app.log')
    logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


DATE_FIELDS = ["ricaActionDate","ricaDOB","ricaFlagDate"]

TIME_FIELDS = []


scanSubscriber =  ricasanctionsubscriber.objects.all()

# Global counters for tracking created/updated records
records_created = 0
records_updated = 0

# Global variable to store old record counts captured at the beginning
old_records_by_source = {}

def capture_old_records_by_source():
    """
    Capture existing record counts by source BEFORE any new processing starts.
    This should be called at the very beginning before temp table clearing.
    """
    global old_records_by_source
    from django.db.models import Count
    try:
        print("📊 Capturing old record counts by source...")
        
        # Group by ricaSource and count from main table
        source_counts = (rica_Watchlist.objects
                        .values('ricaSource')
                        .annotate(count=Count('ricaSource'))
                        .order_by('ricaSource'))
        
        # Convert to simple dict and store globally
        old_records_by_source = {item['ricaSource']: item['count'] for item in source_counts if item['ricaSource']}
        
        print(f"📊 Captured old records by source: {old_records_by_source}")
        return old_records_by_source
    except Exception as e:
        print(f"Error capturing old records by source: {e}")
        old_records_by_source = {}
        return {}

class WatchlistTempTable:
    """
    ORM-based temp table implementation using in-memory list to track new records.
    This approach is database-agnostic and maintains the same functionality as the raw SQL temp table.
    """
    def __init__(self):
        self.records = []  # In-memory storage
        self.temp_table_name = 'rica_temp_watchlist'
        # DO NOT call self.clear() here anymore - clearing only happens once at program start
        print("WatchlistTempTable initialized (without auto-clearing)")
    
    def clear(self):
        """Clear all records from both in-memory and ORM temp tables"""
        import traceback
        print("🔥 TEMP TABLE CLEAR CALLED - Stack trace:")
        traceback.print_stack()
        
        self.records = []
        try:
            cleared_count = rica_temp_watchlist.objects.count()
            rica_temp_watchlist.objects.all().delete()
            print(f"✅ Temp table cleared successfully - {cleared_count} records removed")
        except Exception as e:
            print(f"❌ Error clearing ORM temp table: {e}")
    

    def insert(self, data):
        """Insert data into both in-memory and ORM temp tables"""
        try:
            # Filter to only model fields for temp table
            model_fields = {f.name: f for f in rica_temp_watchlist._meta.get_fields() if hasattr(f, 'attname')}
            # Always include ricaSource, even if None or empty
            clean_data = {k: v for k, v in data.items() if k in model_fields}
            if not clean_data:
                return
            # Insert into ORM temp table
            try:
                rica_temp_watchlist.objects.create(**clean_data)
                self.records.append(clean_data)
                
                # Log every 1000 records to track accumulation
                if len(self.records) % 1000 == 0:
                    print(f"📊 Temp table accumulation: {len(self.records)} records in memory, {self.count_physical()} in database")
               
            except Exception as e:
                print(f"Warning: Failed to insert into ORM temp table: {e}")
                # Fallback: add to in-memory only
                self.records.append(clean_data)
       
        except Exception as e:
            print(f"Error in temp table insert: {e}")
            import traceback
            traceback.print_exc()
    
    def count(self):
        """Get count of records in temp table"""
        return len(self.records)
    
    def get_all_records(self):
        """Get all records from temp table"""
        return self.records.copy()
    
    def count_physical(self):
        """Get count of records in ORM temp table"""
        try:
            return rica_temp_watchlist.objects.count()
        except Exception as e:
            print(f"Error counting ORM temp table: {e}")
            return 0
    
    def get_all_records_physical(self):
        """Get all records from ORM temp table"""
        try:
            return list(rica_temp_watchlist.objects.all().values())
        except Exception as e:
            print(f"Error fetching from ORM temp table: {e}")
            return []
    
    def verify_sync(self):
        """Verify that in-memory and physical tables are in sync"""
        memory_count = self.count()
        physical_count = self.count_physical()
        is_synced = memory_count == physical_count
        
        print(f"Sync verification - Memory: {memory_count}, Physical: {physical_count}, Synced: {is_synced}")
        return is_synced
# Global temp table instance - SINGLETON PATTERN TO PREVENT ACCIDENTAL RE-CREATION
_watchlist_temp_table_instance = None

def get_watchlist_temp_table():
    """Get the singleton instance of watchlist temp table"""
    global _watchlist_temp_table_instance
    if _watchlist_temp_table_instance is None:
        _watchlist_temp_table_instance = WatchlistTempTable()
        print("🎯 Created new WatchlistTempTable singleton instance")
    return _watchlist_temp_table_instance

# Global temp table instance
watchlist_temp_table = get_watchlist_temp_table()



def update_family_relationships_with_fullnames():
    """
    Populate ricaSpouse, ricaChildren, and ricaParents fields in rica_Watchlist with full names from relationships.
    """
    from collections import defaultdict
    from pep_and_sanctions.models import rica_watchlist_family
    # Build mapping: person_id -> list of full names for each relationship type
    spouse_map = defaultdict(list)
    children_map = defaultdict(list)
    parents_map = defaultdict(list)
    relative_map = defaultdict(list)
    # Preload all watchlist full names for fast lookup
    id_to_fullname = dict(rica_Watchlist.objects.values_list('ricaWatchlistId', 'ricaFullName'))
    # Fetch all family relationships
    families = rica_watchlist_family.objects.all()
    for rel in families:
        person_id = rel.ricaPerson
        relative_id = rel.ricaRelative
        relationship = rel.ricaRelationship or ''
        relative_fullname = id_to_fullname.get(relative_id, None)
        if not relative_fullname:
            continue
        if 'spouse' in relationship.lower():
            spouse_map[person_id].append(relative_fullname)
        elif relationship == 'Child/Parent':
            children_map[person_id].append(relative_fullname)
        elif relationship == 'Parent/Child':
            parents_map[person_id].append(relative_fullname)
        else:
            relative_map[person_id].append(relative_fullname)
    # Update each person's record with comma-separated full names
    all_ids = set(list(spouse_map.keys()) + list(children_map.keys()) + list(parents_map.keys()) + list(relative_map.keys()))
    for person_id in all_ids:
        try:
            watchlist = rica_Watchlist.objects.get(ricaWatchlistId=person_id)
            watchlist.ricaSpouse = ', '.join(spouse_map[person_id]) if spouse_map[person_id] else None
            watchlist.ricaChildren = ', '.join(children_map[person_id]) if children_map[person_id] else None
            watchlist.ricaParents = ', '.join(parents_map[person_id]) if parents_map[person_id] else None
            watchlist.ricaRelative = ', '.join(relative_map[person_id]) if relative_map[person_id] else None
            watchlist.save()
        except rica_Watchlist.DoesNotExist:
            continue
    print("Family relationships (full names) updated.")


def check_and_recreate_temp_table():
    """Initialize the hybrid temp table (both in-memory and physical) - NO CLEARING"""
    global records_created, records_updated, watchlist_temp_table
    records_created = 0
    records_updated = 0
    
    try:
        # DO NOT clear the temp table here - it's cleared once at program start
        # watchlist_temp_table.clear()  # REMOVED - temp table cleared only once at start
        
        # Get the actual table name from Django model for logging
        rica_watchlist_table = rica_Watchlist._meta.db_table
        print(f"Main table name: {rica_watchlist_table}")
        print("Hybrid temp table initialized successfully (without clearing)!")
        
        # Verify by checking both counts
        memory_count = watchlist_temp_table.count()
        physical_count = watchlist_temp_table.count_physical()
        print(f"Table verification - Memory count: {memory_count}, Physical count: {physical_count}")
        
        # Verify sync status
        watchlist_temp_table.verify_sync()
        
    except Exception as e:
        print(f"Error initializing hybrid temp table: {e}")
        import traceback
        traceback.print_exc()

def insert_into_temp_table(data):
    """Insert data into ORM-based temp table"""
    global watchlist_temp_table
    try:
        watchlist_temp_table.insert(data)
    except Exception as e:
        print(f"Error in ORM temp table insert: {e}")
        import traceback
        traceback.print_exc()

def get_temp_table_count():
    """Get count of records in ORM temp table"""
    global watchlist_temp_table
    try:
        return watchlist_temp_table.count()
    except Exception as e:
        print(f"Error getting ORM temp table count: {e}")
        return 0

def print_processing_summary():
    """Print summary of created/updated records with hybrid temp table info"""
    memory_count = watchlist_temp_table.count()
    physical_count = watchlist_temp_table.count_physical()
    is_synced = watchlist_temp_table.verify_sync()
    
    print(f"\n=== PROCESSING SUMMARY ===")
    print(f"Records created: {records_created}")
    print(f"Records updated: {records_updated}")
    print(f"Total processed: {records_created + records_updated}")
    print(f"Records in temp table (memory): {memory_count}")
    print(f"Records in temp table (physical): {physical_count}")
    print(f"Temp tables synced: {is_synced}")
    print("==========================\n")

def debug_temp_table():
    """Debug function to check hybrid temp table structure and data"""
    try:
        # Check in-memory table
        memory_count = watchlist_temp_table.count()
        print(f"In-memory temp table records: {memory_count}")
        
        # Check physical table
        physical_count = watchlist_temp_table.count_physical()
        print(f"Physical temp table records: {physical_count}")
        
        # Verify sync
        is_synced = watchlist_temp_table.verify_sync()
        print(f"Tables in sync: {is_synced}")
        
        # Check physical table structure
        from django.db import connection
        with connection.cursor() as cursor:
            db_engine = connection.settings_dict['ENGINE']
            temp_table_name = watchlist_temp_table.temp_table_name;
            
            # Check if table exists
            if 'sqlite' in db_engine.lower():
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{temp_table_name}'")
            else:
                cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_name = '{temp_table_name}'")
            
            table_exists = cursor.fetchone()
            print(f"Physical temp table exists: {table_exists is not None}")
            
            if table_exists:
                # Get table structure
                if 'sqlite' in db_engine.lower():
                    cursor.execute(f"PRAGMA table_info({temp_table_name})")
                else:
                    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{temp_table_name}'")
                
                columns = cursor.fetchall()
                print(f"Physical temp table columns count: {len(columns)}")
                print(f"First 5 columns: {columns[:5] if columns else 'None'}")
                
    except Exception as e:
        print(f"Error debugging hybrid temp table: {e}")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# --- PDF Extraction Utility Integration ---



def remove_start_0(string=''):
    return  remove_start_0(str(string)[1:len(string)]) if (str(string).startswith('0') and len(string)>2 ) else str(string)


def transformDateField(date_str):
    date_string = str(date_str).replace('/','')
    print('date_string',date_string)
    try:
        try:
            year = remove_start_0(date_string[4:])
            month = remove_start_0(date_string[2:4])
            day = remove_start_0(date_string[0:2])
        except:
            year = remove_start_0(date_string[0:4])
            month = remove_start_0(date_string[6:8])
            day = remove_start_0(date_string[4:6])
        print('date',date_string,year,month,day)
        res =  dt(int(year),int(month),int(day))
        return res.date()
    except Exception as e:
        return None


def transformTimeField(date_str):
    date_string = str(date_str).replace(':','')
    print(date_string)
    try:
        hour = remove_start_0(date_string[0:2])
        minute = remove_start_0(date_string[2:4])
        seconds = '01' # remove_start_0(date_string[4:6])
        print('time', hour, minute, seconds)
        res = datetime.time(int(hour), int(minute), int(seconds))
        return res
    except Exception as e:
        return None


def modifyData(data):
    for key in list(data.keys()):
        if str(data[key]).lower() == 'nan':
            data[key] = None
        if key in DATE_FIELDS and data[key]:
            data[key] = transformDateField(data[key])
        if key in TIME_FIELDS and data[key]:
            data[key] = transformTimeField(data[key]) 
        if not data[key]:
            data[key] = None
    return data



def createWatchlistId(fullname):
    name = re.sub(r"[\d\s\W]+", '', str(fullname).lower())
    total = re.split(r"[\W]{0}",name)
    total = [ x for x in total if x]
    total = functools.reduce(lambda a,b:  int(a) + int(ord(b)),total,0)
    return str(total)

def get_regulator_desc(list_name):
    try:
        # Handle special cases for composite categories
        if list_name == 'INEC_CANDIDATES':
            return "INEC Electoral Candidates List"
        
        return ricasanctionRegulators.objects.get(ricaSanctionCode=list_name).ricaSanctionDesc
    except Exception as e:
        return ""



# @sync_to_async
def create_sanction(new_data):
    try:
        wd=rica_Watchlist.objects.get(ricaWatchlistId=new_data['ricaWatchlistId'])
        print("ITEM",new_data['ricaWatchlistId'],'already exists')
    except Exception as e:
        rica_Watchlist.objects.create(**new_data)


def map_data(data, map_=None, temp_table_records=None):
    global records_created, records_updated
    if not map_:
        new_data = data
    else:
        new_data = {}
        for key in map_:
            km = data.get(key) or data.get(map_[key])
            if km:
                new_data[map_[key]] = km

    new_data = modifyData(new_data)

    # ENHANCED NAME PROCESSING: Parse translated names into individual components
    if new_data.get('ricaFullName'):
        full_name = new_data['ricaFullName']
        
        # COMPREHENSIVE NAME CLEANUP AND PROCESSING
        # Check if we have multiple versions separated by semicolons
        if ';' in full_name:
            # print(f"Processing multi-version name: {full_name}")
            
            # Split by semicolon and analyze each part
            name_parts = [part.strip() for part in full_name.split(';')]
            
            # Find the best English/Latin version to use as primary
            best_name = None
            original_name = None
            
            for part in name_parts:
                if not part:
                    continue
                    
                # Check if this part is predominantly Latin script
                if not is_non_latin(part):
                    # This is likely the English/translated version
                    if not best_name or len(part) > len(best_name):
                        best_name = part
                else:
                    # This is likely the original foreign script
                    if not original_name:
                        original_name = part
            
            # Use the best Latin version as the primary name
            if best_name:
                clean_name = best_name.strip()
                # print(f"Selected best Latin name: '{clean_name}'")
            else:
                # If no Latin version found, use the first part and try to translate
                clean_name = name_parts[0].strip()
                # print(f"No Latin version found, using first part: '{clean_name}'")
                
                # Try to translate if it's non-Latin
                if is_non_latin(clean_name):
                    try:
                        translated_name = translate(clean_name)
                        if translated_name != clean_name:
                            clean_name = translated_name
                            # print(f"Translated to: '{clean_name}'")
                    except Exception as e:
                        print(f"Translation failed: {str(e)[:100]}")
            
            # Update the ricaFullName with the clean version
            new_data['ricaFullName'] = clean_name
            
            # Parse into individual components if it's a person's name
            first_name, middle_name, surname, title, _ = parse_translated_name(clean_name)
            
            # Only set individual name fields if parsing returned actual names (not entities)
            if first_name and surname:  # Must have both first and surname for individual
                new_data['ricaFirstName'] = first_name
                # print(f"Set ricaFirstName: '{first_name}'")
                if middle_name:
                    new_data['ricaMiddleName'] = middle_name
                    print(f"Set ricaMiddleName: '{middle_name}'")
                new_data['ricaSurname'] = surname
                # print(f"Set ricaSurname: '{surname}'")
                if title:
                    new_data['ricaTitle'] = title
                    # print(f"Set ricaTitle: '{title}'")
                # print(f"Parsed individual name: First='{first_name}', Middle='{middle_name}', Surname='{surname}'")
            else:
                
                print(f"Detected entity/organization name, not splitting: '{clean_name}'")
        
        # Check if ricaFullName contains only non-Latin text (needs translation)
        elif any(ord(char) > 127 for char in full_name) and not any(char.isalpha() and ord(char) < 128 for char in full_name):
            # This is pure non-Latin like "Քոչարյան Գագիկ Ռուբենի"
            print(f"Processing pure non-Latin name: {full_name}")
            try:
                translated_full_name = translate(full_name)
                
                if translated_full_name and translated_full_name != full_name:
                    # print(f"Translated full name: '{full_name}' → '{translated_full_name}'")
                    new_data['ricaFullName'] = translated_full_name
                    
                    # Also parse into individual components if it's a person's name
                    first_name, middle_name, surname, title, _ = parse_translated_name(translated_full_name)
                    if first_name and surname and not new_data.get('ricaFirstName'):
                        new_data['ricaFirstName'] = first_name
                        if middle_name and not new_data.get('ricaMiddleName'):
                            new_data['ricaMiddleName'] = middle_name
                        if not new_data.get('ricaSurname'):
                            new_data['ricaSurname'] = surname
                        # print(f"Set name components from translation: First='{first_name}', Middle='{middle_name}', Surname='{surname}'")
                else:
                    pass
                    # print(f"Translation returned same text, keeping original: '{full_name}'")

            except Exception as e:
                pass
                # print(f"Translation failed for '{full_name}': {str(e)[:100]}, keeping original")
                # Continue processing without crashing
        
        # For names that are already in Latin script, try to parse if individual fields are missing
        elif not new_data.get('ricaFirstName') and not new_data.get('ricaSurname'):
            first_name, middle_name, surname, title, _ = parse_translated_name(full_name)
            if first_name and surname:  # Only set if we have both first and surname
                new_data['ricaFirstName'] = first_name
                if middle_name:
                    new_data['ricaMiddleName'] = middle_name
                new_data['ricaSurname'] = surname
                if title:
                    new_data['ricaTitle'] = title
                # print(f"Parsed regular name: '{full_name}' → First='{first_name}', Middle='{middle_name}', Surname='{surname}'")
            else:
                pass
                # print(f"Could not parse as individual name: '{full_name}' - treating as entity")
        
        # Also ensure other name fields are translated if they contain non-Latin text
        for name_field in ['ricaFirstName', 'ricaMiddleName', 'ricaSurname', 'ricaTitle']:
            if new_data.get(name_field) and is_non_latin(str(new_data[name_field])):
                original_value = new_data[name_field]
                try:
                    translated_value = translate(original_value)
                    if translated_value != original_value:
                        print(f"Translated {name_field}: '{original_value}' → '{translated_value}'")
                        new_data[name_field] = translated_value
                    else:
                        print(f"Translation returned same text for {name_field}: '{original_value}'")
                except Exception as e:
                    print(f"Translation failed for {name_field} '{original_value}': {str(e)[:100]}, keeping original")
                    # Continue processing without crashing


    # Ensure ricaSource is set from source_key if present
    # ENHANCED SOURCE DEBUGGING WITH CALLER DETECTION
    original_source = new_data.get('ricaSource')
    original_source_key = new_data.get('source_key')
    original_category = new_data.get('ricaCategory')
    
    # NEW: Detect the calling crawler by inspecting the call stack
    import inspect
    calling_crawler = None
    calling_crawler_raw = None
    try:
        frame = inspect.currentframe()
        while frame:
            frame = frame.f_back
            if frame and frame.f_code.co_name.startswith('run_'):
                calling_crawler_raw = frame.f_code.co_name.upper()
                
                # Extract source name from crawler function name
                if calling_crawler_raw.startswith('RUN_') and calling_crawler_raw.endswith('_LIST'):
                    calling_crawler = calling_crawler_raw[4:-5]  # Remove 'RUN_' and '_LIST'
                elif calling_crawler_raw.startswith('RUN_'):
                    calling_crawler = calling_crawler_raw[4:]  # Remove 'RUN_'
                else:
                    calling_crawler = calling_crawler_raw
                break
    except Exception:
        calling_crawler = None
        calling_crawler_raw = None
    
    # print(f"🔍 Source Debug - Original: ricaSource={original_source}, source_key={original_source_key}, ricaCategory={original_category}")
    # print(f"🎯 Detected calling crawler: {calling_crawler_raw} → extracted: {calling_crawler}")
    
    # Dedicated debug logger for sources
    sources_debug_logger = logging.getLogger("SourcesDebug")
    if not sources_debug_logger.handlers:
        sources_debug_handler = logging.FileHandler(os.path.join(os.path.dirname(__file__), 'sources_debug.log'))
        sources_debug_formatter = logging.Formatter('%(asctime)s - %(message)s')
        sources_debug_handler.setFormatter(sources_debug_formatter)
        sources_debug_logger.addHandler(sources_debug_handler)
        sources_debug_logger.setLevel(logging.INFO)
    sources_debug_logger.info(f"After mapping: ricaSource={new_data.get('ricaSource')}, source_key={new_data.get('source_key')}, calling_crawler={calling_crawler}, ID={new_data.get('ricaWatchlistId')}")
    
    new_data["ricaReportedBy"] = new_data['ricaCategory']
    new_data['ricaLanguage'] = 'en'
    new_data['ricaOperator'] = 'BOT'
    new_data['ricaRecordDate'] = dt.now().strftime('%Y%m%d')
    new_data['ricaRecordTime'] = dt.now().strftime('%H%M%S')
    new_data['ricaOperation'] = 'en-201'
    new_data['ricaWorkstation'] = '10.40.14.228'
    new_data['ricaRecordCounter'] = 1

    # Filter to only model fields and truncate overlong fields
    model_fields = {f.name: f for f in rica_Watchlist._meta.get_fields() if hasattr(f, 'attname')}
    filtered_data = {k: v for k, v in new_data.items() if k in model_fields}
    # Truncate string fields that are too long
    for k, f in model_fields.items():
        if k in filtered_data and hasattr(f, 'max_length') and f.max_length and filtered_data[k] is not None:
            if isinstance(filtered_data[k], str) and len(filtered_data[k]) > f.max_length:
                filtered_data[k] = filtered_data[k][:f.max_length]


    # Only insert if not already in rica_watchlist using ricaWatchlistId for uniqueness
    
    try:
        exists = rica_Watchlist.objects.filter(
            ricaWatchlistId=filtered_data.get('ricaWatchlistId', '')
        ).exists()
        
        if not exists:
            # ENHANCED SOURCE ASSIGNMENT WITH CRAWLER DETECTION
            final_source = None
            
            # print(f"🔍 SOURCE ASSIGNMENT DEBUG:")
            # print(f"  - source_key in filtered_data: {'source_key' in filtered_data}")
            # print(f"  - source_key value: {filtered_data.get('source_key', 'NOT_FOUND')}")
            # print(f"  - ricaSource in filtered_data: {'ricaSource' in filtered_data}")
            # print(f"  - ricaSource value: {filtered_data.get('ricaSource', 'NOT_FOUND')}")
            # print(f"  - calling_crawler_raw: {calling_crawler_raw}")
            # print(f"  - calling_crawler extracted: {calling_crawler}")
            
            if 'source_key' in filtered_data and filtered_data['source_key']:
                final_source = filtered_data['source_key']
                filtered_data['ricaSource'] = final_source
                # print(f"✅ Set ricaSource from source_key: {final_source}")
            elif 'ricaSource' in filtered_data and filtered_data['ricaSource']:
                final_source = filtered_data['ricaSource']
                # print(f"✅ Using existing ricaSource: {final_source}")
            elif calling_crawler:
                # Use the detected crawler name as source
                final_source = calling_crawler
                filtered_data['ricaSource'] = final_source
                # print(f"🔧 Set ricaSource from calling crawler: {final_source}")
            else:
                final_source = filtered_data.get('ricaCategory', 'UNKNOWN')
                filtered_data['ricaSource'] = final_source
                # print(f"⚠️ Fallback ricaSource from ricaCategory: {final_source}")
            
            # print(f"🎯 FINAL ricaSource for record: {final_source}")
            # print(f"🎯 FINAL filtered_data ricaSource: {filtered_data.get('ricaSource')}")

            
            rica_Watchlist.objects.create(**filtered_data)

            records_created += 1
           
            # Insert into temp table (only new records)
         
            insert_into_temp_table(filtered_data)
            # Append to temp_table_records if provided
            if temp_table_records is not None:
                temp_table_records.append(dict(filtered_data))
                print(f"📝 Added to temp_table_records with ricaSource: '{filtered_data.get('ricaSource', 'MISSING')}' (record #{len(temp_table_records)})")
        else:
            print('Data with same ricaWatchlistId already exists in rica_Watchlist, skipping')
    except Exception as e:
        traceback.print_exc()
        return None
    return filtered_data


def get_total_list(list_name):
    try:
        return rica_Watchlist.objects.filter(ricaCategory=list_name).count()
    except Exception as e:
        return 0

def get_existing_records_by_source():
    """
    Get the old record counts that were captured at the beginning.
    Returns: dict like {'CANADIAN': 1500, 'NIGSAC': 800, ...}
    """
    global old_records_by_source
    print(f"📊 Using captured old records by source: {old_records_by_source}")
    return old_records_by_source

def get_new_records_by_source():
    """
    Get count of new records in rica_temp_watchlist grouped by ricaSource
    Returns: dict like {'CANADIAN': 150, 'NIGSAC': 75, ...}
    """
    from django.db.models import Count
    try:
        # Group by ricaSource and count from temp table
        source_counts = (rica_temp_watchlist.objects
                        .values('ricaSource')
                        .annotate(count=Count('ricaSource'))
                        .order_by('ricaSource'))
        
        # Convert to simple dict
        new_counts = {item['ricaSource']: item['count'] for item in source_counts if item['ricaSource']}
        
        print(f"📊 New records by source: {new_counts}")
        return new_counts
    except Exception as e:
        print(f"Error getting new records by source: {e}")
        return {}

def build_database_driven_analysis(full_analysis_keys):
    """
    Build analysis using database queries instead of runtime counting
    Uses captured old records (from program start) + new records (from temp table)
    """
    print(f"\n=== DATABASE-DRIVEN ANALYSIS ===")
    
    # Get existing and new records by source
    existing_by_source = get_existing_records_by_source()  # Uses captured old data
    new_by_source = get_new_records_by_source()           # Queries temp table
    
    # Build analysis for each crawler
    analysis = {}
    
    for list_name in full_analysis_keys:
        # Extract source name from crawler name
        extracted_source = None
        if list_name.startswith('RUN_') and list_name.endswith('_LIST'):
            extracted_source = list_name[4:-5]  # Remove 'RUN_' and '_LIST'
        elif list_name.startswith('RUN_'):
            extracted_source = list_name[4:]  # Remove 'RUN_'
        else:
            extracted_source = list_name
        
        # Get counts for this source
        old_count = existing_by_source.get(extracted_source, 0)
        new_count = new_by_source.get(extracted_source, 0)
        total_count = old_count + new_count
        
        # Build analysis structure
        analysis[list_name] = {
            'old_record': old_count,
            'new_record': new_count,
            'total': total_count,
            'list_name': list_name,
            'list_desc': get_regulator_desc(list_name)
        }
        
        print(f"🔍 {list_name} → {extracted_source}: old={old_count}, new={new_count}, total={total_count}")
    
    print("=================================\n")
    return analysis

# pip install deep-translator




# --- Translation Utilities ---
_translation_cache = {}
_translation_lock = threading.Lock()


def is_non_latin(text):
    allowed_chars = set(string.punctuation + " ’‘;")
    for char in text:
        if char in allowed_chars:
            continue
        try:
            name = unicodedata.name(char)
            if 'LATIN' not in name:
                return True
        except ValueError:
            return True
    return False

# Utility: Check if a name list contains both Latin and non-Latin forms
def is_already_translated_name_list(name_field):
    if isinstance(name_field, list) and len(name_field) >= 2:
        has_latin = any(not is_non_latin(n) for n in name_field)
        has_nonlatin = any(is_non_latin(n) for n in name_field)
        return has_latin and has_nonlatin
    return False

def setup_argos_translate():
    # Download and install English and all available language packs if not present
    import os
    import argostranslate.package
    import argostranslate.translate
    packages = argostranslate.package.get_available_packages()
    installed_languages = [lang.code for lang in argostranslate.translate.get_installed_languages()]
    # Always ensure English is present
    for pkg in packages:
        if pkg.to_code == "en" and pkg.from_code not in installed_languages:
            download_path = pkg.download()
            argostranslate.package.install_from_path(download_path)

def batch_translate_argos(names):
    import argostranslate.translate
    setup_argos_translate()
    installed_languages = argostranslate.translate.get_installed_languages()
    en_lang = next((l for l in installed_languages if l.code == "en"), None)
    translations = {}
    translation_cache = {}
    for name in names:
        # Find a language that can translate this name to English
        src_lang = None
        for lang in installed_languages:
            if lang.code != "en":
                try:
                    translation_obj = translation_cache.get(lang.code)
                    if not translation_obj:
                        translation_obj = lang.get_translation(en_lang)
                        translation_cache[lang.code] = translation_obj
                    # Try to translate a word; if it doesn't error, use this lang
                    _ = translation_obj.translate(name)
                    src_lang = lang
                    break
                except Exception:
                    continue
        if src_lang:
            try:
                translation_obj = translation_cache[src_lang.code]
                translations[name] = translation_obj.translate(name)
            except Exception:
                translations[name] = name
        else:
            translations[name] = name
    return translations




def parse_translated_name(full_name):
    """
    Enhanced parsing of translated full names into individual components.
    Handles complex cases like: "Kocharyan Alfred Gagik; Քոչարյան Ալֆրեդ Գագիկի"
    Also handles organization names and multiple versions separated by semicolons.
    Returns: (first_name, middle_name, surname, title, full_name_clean)
    """
    if not full_name:
        return None, None, None, None, None
    
    # Handle names with semicolon (multiple versions)
    working_name = full_name.strip()
    if ';' in working_name:
        # Split by semicolon and find the best Latin version
        name_parts = [part.strip() for part in working_name.split(';')]
        
        # Prefer the first non-empty Latin script version
        for part in name_parts:
            if part and not is_non_latin(part):
                working_name = part
                break
        else:
            # If no Latin version found, use the first non-empty part
            working_name = next((part for part in name_parts if part), working_name)
    
    # Clean the working name
    working_name = working_name.strip()
    
    # COMPREHENSIVE ENTITY DETECTION
    entity_indicators = [
        # Business entities - EXPANDED
        'INC.', 'INC', 'LLC', 'LTD', 'LIMITED', 'CORP', 'CORPORATION', 'CO.', 'COMPANY', 
        'GROUP', 'FOUNDATION', 'ASSOCIATION', 'INSTITUTE', 'UNIVERSITY',
        'BANK', 'TRUST', 'FUND', 'HOLDING', 'ENTERPRISE', 'ORGANIZATION',
        'SOCIETY', 'PARTNERSHIP', 'JOINT STOCK', 'LIMITED LIABILITY',
        'PUBLIC', 'PRIVATE', 'FEDERAL', 'STATE', 'GOVERNMENT', 'MINISTRY',
        'DEPARTMENT', 'ADMINISTRATION', 'MANAGEMENT', 'BUREAU', 'AGENCY',
        'SERVICE', 'INSTITUTION', 'ESTABLISHMENT', 'OFFICE', 'CENTER',
        'CENTRE', 'COUNCIL', 'COMMITTEE', 'COMMISSION', 'BOARD',
        # International variants - EXPANDED
        'S.A.', 'S.L.', 'SARL', 'GMBH', 'AG', 'KG', 'OHG', 'BERHAD',
        'PVT', 'PLC', 'JSC', 'OAO', 'ZAO', 'OOO', 'PAO', 'AO', 'PJSC',
        'SPAOLO', 'SOCIETA', 'AZIONI', 'D.O.O.', 'SRL', 'S.R.L.',
        'LTDA', 'LTDA.', 'S.A. DE C.V.', 'DE C.V.', 'PTY LTD', 'PTY',
        # Educational/Research/Medical - EXPANDED
        'SCHOOL', 'COLLEGE', 'ACADEMY', 'RESEARCH', 'LABORATORY', 'LAB',
        'CLINIC', 'HOSPITAL', 'MEDICAL', 'HEALTH', 'SANATORIUM', 'PHARMACY',
        'HEALTHCARE', 'CONVALESCENT', 'REHABILITATION', 'AMBULATORY',
        # Government/Military
        'FEDERAL', 'ADMINISTRATION', 'МИНИСТЕРСТВО', 'ГОСУДАРСТВЕННОЕ',
        'УЧРЕЖДЕНИЕ', 'УПРАВЛЕНИЕ', 'КОМИТЕТ', 'СЛУЖБА', 'PRISON',
        'CORRECTIONAL', 'DETENTION', 'SECURITY',
        # Financial
        'SECURITIES', 'INVESTMENT', 'ASSET', 'CAPITAL', 'FINANCIAL',
        'INSURANCE', 'MUTUAL', 'PENSION', 'CREDIT', 'SAVINGS'
    ]
    
    # Check if this looks like a vessel/ship name (common maritime patterns) - EXPANDED
    vessel_indicators = [
        'SHIP', 'VESSEL', 'BOAT', 'YACHT', 'FERRY', 'TANKER', 'CARGO', 'FREIGHTER',
        'TRADER', 'NAVIGATOR', 'EXPLORER', 'OCEAN', 'SEA', 'MARINE', 'MARITIME',
        'STAR', 'SKY', 'QUEEN', 'KING', 'LADY', 'LORD', 'PRINCESS', 'PRINCE',
        'FALCON', 'EAGLE', 'TIGER', 'LION', 'ISLAND', 'MARMARA', 'AMBASSADOR',
        'SHIPPING', 'FLEET', 'NAVIGATION', 'CARRIER', 'BULK', 'ENERGY',
        'GALAXY', 'UNIVERSE', 'COSMOS', 'TRIUMPH', 'VICTORY', 'GLORY',
        'SPIRIT', 'WIND', 'WAVE', 'TIDE', 'CURRENT', 'STREAM', 'FLOW'
    ]
    
    upper_name = working_name.upper()
    
    # Enhanced entity detection logic
    # 1. Check for business entities
    if any(indicator in upper_name for indicator in entity_indicators):
        print(f"Detected entity name: '{working_name}' - not splitting into individual names")
        return None, None, None, None, working_name
    
    # 2. Check for very long names (likely organizations)
    if len(working_name) > 100:  # Names longer than 100 chars are likely organizations
        print(f"Detected long organization name: '{working_name}' - not splitting into individual names")
        return None, None, None, None, working_name
    
    # 3. Check for names with quotes (often organization names)
    if '"' in working_name or '"' in working_name or '"' in working_name:
        print(f"Detected quoted organization name: '{working_name}' - not splitting into individual names")
        return None, None, None, None, working_name
    
    # 4. Check for vessel/ship names
    if any(indicator in upper_name for indicator in vessel_indicators):
        print(f"Detected vessel name: '{working_name}' - not splitting into individual names")
        return None, None, None, None, working_name
    
    # 5. Check for names with multiple entities (semicolon or comma separated long segments)
    import re
    if len(re.findall(r'[;,]', working_name)) >= 2 and len(working_name) > 50:
        print(f"Detected multi-entity name: '{working_name}' - not splitting into individual names")
        return None, None, None, None, working_name
    
    # Check for numeric patterns that suggest vessel/equipment names
    import re
    if re.search(r'\b[A-Z]+\s+\d+\b', working_name):  # Pattern like "PANDO 1", "ANGORA 3"
        print(f"Detected vessel/equipment name pattern: '{working_name}' - not splitting into individual names")
        return None, None, None, None, working_name
    
    # Check for single letters as surnames (likely vessel codes)
    name_parts_preview = working_name.split()
    if len(name_parts_preview) >= 2 and len(name_parts_preview[-1]) == 1 and name_parts_preview[-1].isalpha():
        print(f"Detected vessel code pattern: '{working_name}' - not splitting into individual names") 
        return None, None, None, None, working_name
    
    # Handle names with comma (could be "Surname, FirstName" format)
    if ',' in working_name:
        parts = [p.strip() for p in working_name.split(',')]
        if len(parts) == 2:
            # "Surname, FirstName" format
            surname = parts[0]
            first_name = parts[1]
            return first_name, None, surname, None, working_name
    
    # Split by spaces for normal "FirstName MiddleName Surname" format
    name_parts = working_name.split()
    
    if len(name_parts) == 1:
        # Only one name part
        return name_parts[0], None, None, None, working_name
    elif len(name_parts) == 2:
        # First and Last name
        return name_parts[0], None, name_parts[1], None, working_name
    elif len(name_parts) == 3:
        # First, Middle, Last name
        return name_parts[0], name_parts[1], name_parts[2], None, working_name
    elif len(name_parts) >= 4:
        # First, Middle(s), Last name - combine middle parts
        first_name = name_parts[0]
        middle_name = ' '.join(name_parts[1:-1])
        surname = name_parts[-1]
        return first_name, middle_name, surname, None, working_name
    
    return None, None, None, None, working_name

def translate(name):
    """
    Simple translation function: if text is not Latin script, translate it.
    All translations are normalized to uppercase for consistency.
    """
    if not name or not isinstance(name, str):
        return name
    
    name_clean = name.strip()
    
    # print(f"Testing name: '{name_clean}'")
    
    # Debug: Show character analysis
    for i, char in enumerate(name_clean[:10]):  # Check first 10 chars
        if char.isalpha():
            try:
                char_name = unicodedata.name(char)
                # print(f"  Char {i}: '{char}' → {char_name}")
            except ValueError:
                continue
                # print(f"  Char {i}: '{char}' → UNKNOWN CHARACTER")
    
    # Only check: Is it non-Latin? If yes, translate.
    if is_non_latin(name_clean):
        try:
            # print(f"✅ Non-Latin text detected, translating: '{name_clean}'")
            translated = translate_multi_language(name_clean)
            
            if translated and translated != name_clean:
                # Normalize to ALL CAPS for consistency
                translated_upper = translated.upper()
                # print(f"✅ Translated: '{name_clean}' → '{translated_upper}'")
                return translated_upper
            else:
                # print(f"⚠️ Translation failed or returned same text: '{name_clean}'")
                return name
                
        except Exception as e:
            # print(f"❌ Translation error for '{name_clean}': {str(e)[:100]}")
            return name
    else:
        # It's Latin script, don't translate
        # print(f"⏭️ Latin script detected, skipping translation: '{name_clean}'")
        return name


def get_emails_receivers(data):
    email_list=[]
    if data.ricaEmailReceiver:
        email_list = [email["ricaEmailReciever"] for email in json.loads(data.ricaEmailReceiver)]

    if data.ricaRespondent and str(data.ricaRespondentFlag).lower()=='yes':
        email_list.append(data.ricaRespondent.replace(" ",""))
    if data.ricaInvestigator and str(data.ricaInvestigatorFlag).lower()=='yes':
        email_list.append(data.ricaInvestigator.replace(" ",""))
    if data.ricaOwner and str(data.ricaOwnerFlag).lower()=='yes':
        email_list.append(data.ricaOwner.replace(" ",""))
    if data.ricaNextOwner and str(data.ricaNextOwnerFlag).lower()=='yes':
        email_list.append(data.ricaNextOwner.replace(" ",""))

    return email_list



def send_response_mail(send_to, options={}, attachment_paths=None):
    try:
        spf = ricaspf.objects.get(ricaSpfId__iexact=f'en-SYSTEM')
    except Exception as e:
        spf = None

    # Add current date to template variables
    from datetime import datetime
    template_vars = {
        'current_date': datetime.now().strftime('%B %d, %Y'),  # e.g., "July 07, 2025"
        'current_date_short': datetime.now().strftime('%Y-%m-%d'),  # e.g., "2025-07-07"
        'current_datetime': datetime.now().strftime('%B %d, %Y at %I:%M %p')  # e.g., "July 07, 2025 at 02:30 PM"
    }
    
    try:    
        html = ricaMessages.objects.get(ricaMsgId=f"{'en'}-{options.get('msg_code')}").ricaMessage,
    except Exception as e:
        html = None
    html_out = report.gen_template('sanction_update.html', {**template_vars, **options}, html=html)
    report.send_html_mail(
        'New Sanctions Records Alert - Excel Report Attached',
        html_out,
        send_to,
        [],
        spf,
        attachment=attachment_paths or [],
        options=options
    )
    

def generate_new_records_excel(new_records):
    if not new_records:
        return None
    # Do NOT add 'Source' column anymore, just export all discovered records as-is
    df = pd.DataFrame(new_records)
    
    # Generate filename with current date: sanction_YYYY-MM-DD.xlsx
    from datetime import datetime
    current_date = datetime.now().strftime('%Y-%m-%d')
    filename = f'sanction_{current_date}.xlsx'
    
    # Create file in temp directory with custom name
    import tempfile
    temp_dir = tempfile.gettempdir()
    file_path = os.path.join(temp_dir, filename)
    
    df.to_excel(file_path, index=False)
    print(f"Excel file generated: {filename}")
    return file_path


def scrape_accordion_content( accordion_element):
    try:
        cleaned_data = {}
        title_element = accordion_element.query_selector('h5')
        title = title_element.inner_text()

        if title in processed_titles:
            return None
        else:
            processed_titles.add(title)

        tab_buttons = accordion_element.query_selector_all('button.tablinks')
        for button in tab_buttons:
            if "Narrative Summary" in button.inner_text():
                tab_id = button.get_attribute("onclick").split(", ")[1][1:-2]
                narrative_summary = accordion_element.query_selector(f"div.tabcontent#{tab_id} textarea")
                if narrative_summary:
                    summary = narrative_summary.inner_text()

                    if "INDIVIDUAL/ENTITY ASSOCIATED" in title.upper():
                        for key,split_wrd in enumerate(re.split('\n',summary)):
                            if key == 0:
                                name = process_relation_name(str(split_wrd.split("-")[1]).strip())
                                cleaned_data['ricaFullName'] = translate(f"{name[0]}")
                                cleaned_data['ricaFirstName'] = translate(f"{name[1]}")
                                cleaned_data['ricaMiddleName'] = translate(name[2])
                                cleaned_data['ricaSurname'] = translate(f"{name[3]}")
                                cleaned_data['ricaTitle'] = translate(name[4])
                                # Store original names as backup
                                # cleaned_data['ricaFullNameOriginal'] = name[0]
                                # cleaned_data['ricaFirstNameOriginal'] = name[1]
                                # cleaned_data['ricaMiddleNameOriginal'] = name[2]
                                # cleaned_data['ricaSurnameOriginal'] = name[3]
                                # cleaned_data['ricaTitleOriginal'] = name[4]
                            else:
                                split_wrd_n = split_wrd.split(":")
                                if len(split_wrd_n)==2:
                                    cleaned_data[str(split_wrd_n[0]).strip()] = str(split_wrd_n[1]).strip()

                        if cleaned_data.get("Date of Registration"):
                            cleaned_data['ricaWatchType'] = 'Entity'
                        else:
                            cleaned_data['ricaWatchType'] = 'Individual'
                    else:
                        cleaned_data["ricaFullName"] = translate(title.replace("DESIGNATION OF","").replace("AS A TERRORIST GROUP","").strip())
                        cleaned_data["ricaFullNameOriginal"] = title.replace("DESIGNATION OF","").replace("AS A TERRORIST GROUP","").strip()
                        print(cleaned_data["ricaFullName"])
                        cleaned_data['ricaWatchType'] = 'Entity'
                        cleaned_data['ricaDescription'] = summary.strip()

                    cleaned_data['ricaCategory'] = 'NSL'
                    cleaned_data['ricaSubCategory'] = 'Sanctions'
        
        cleaned_data['ricaWatchlistId'] = str(cleaned_data.get('ricaFirstName') or cleaned_data.get('ricaFullName') ).upper().split(" ")[0] +'-'+ createWatchlistId(cleaned_data['ricaFullName'])
        # Do NOT call map_data here!
        return cleaned_data

    except Exception as e:
        print("An error occurred while scraping an accordion:", str(e))


NS = None  # will be resolved at runtime

def _txt(elem, tag, default=None):
    """
    Namespace-aware text extractor.
    `tag` is the tag name without prefix.
    """
    global NS
    if NS is None and elem.tag.startswith("{"):
        NS = elem.tag.split("}")[0] + "}"
    node = elem.find(f"{NS or ''}{tag}")
    if node is not None and node.text:
        return node.text.strip()
    # fallback: search any descendant ending with tag
    for n in elem.iter():
        if n.tag.endswith(tag) and n.text:
            return n.text.strip()
    return default

def ensure_log_table_exists():
    """No-op: Table is managed by Django ORM. No manual creation needed."""
    pass


def log_run_to_db(source, records_found, records_new, status, email_sent, error_message=None, previousRecords=0):
    """Insert a log entry for this run using the Django ORM."""
    import json
    from datetime import datetime
    try:
        # Try to get sourceBreakdown from caller if available
        source_breakdown = None
        try:
            import inspect
            frame = inspect.currentframe().f_back
            if 'dicts' in frame.f_locals and 'analysis' in frame.f_locals['dicts']:
                source_breakdown = json.dumps(frame.f_locals['dicts']['analysis'])
        except Exception:
            source_breakdown = None

        log_entry = rica_watchlist_log.objects.create(
            runStatus=status,
            recordsProcessed=records_found,
            recordsInserted=records_new,
            previousRecords=previousRecords,
            errorDetails=error_message,
            sources=source,
            processingNotes=f"email_sent={email_sent}",
            ricaOperator="BOT",  # Or use actual operator if available
            lastRunDate=datetime.now(),
            emailStatus="sent" if email_sent else "not_sent",
            sourceBreakdown=source_breakdown
        )
        print(f"Successfully logged run to rica_watchlist_log: {status} at {log_entry.lastRunDate}")
    except Exception as e:
        print("Failed to log run to rica_watchlist_log:", e)

# ==== PDF EXTRACTION FUNCTIONS ====# ==== PDF EXTRACTION FUNCTIONS ====



def start():
    global running_flag
    running_flag = True

    try:
        ensure_log_table_exists()
        
        # FIRST: Capture old record counts before ANY processing starts
        print("=== CAPTURING OLD RECORDS BY SOURCE BEFORE PROCESSING ===")
        capture_old_records_by_source()
        
        # Clear temp table ONLY ONCE at the start of scan cycle
        print("=== CLEARING TEMP TABLE AT PROGRAM START ===")
        initial_count = watchlist_temp_table.count_physical()
        print(f"Temp table had {initial_count} records before clearing")
        watchlist_temp_table.clear()
        print(f"✅ Temp table cleared. Starting fresh scan cycle.")
        
        while running_flag:
            print("=== STARTING NEW SCAN CYCLE ===")
            
            # Monitor temp table state at cycle start
            cycle_start_memory = watchlist_temp_table.count()
            cycle_start_physical = watchlist_temp_table.count_physical()
            print(f"📊 Cycle start - Temp table: {cycle_start_memory} memory, {cycle_start_physical} physical")
            
            # ENHANCED ERROR HANDLING: Wrap each major section in try-catch
            try:
                check_and_recreate_temp_table()
                debug_temp_table()
            except Exception as e:
                print(f"Error in table setup: {str(e)[:200]}")
                print("Continuing with degraded functionality...")

            temp_table_records = []

            try:
                previousRecords = rica_Watchlist.objects.count()
            except Exception as e:
                print(f"Error getting previous records count: {str(e)[:100]}")
                previousRecords = 0

            results = {}
            lock = threading.Lock()

            def run_and_store(key, func):
                try:
                    print(f"Starting crawler: {key}")
                    data = func()
                    
                    # Generic detection for any crawler that yields batches (generator pattern)
                    if hasattr(data, '__iter__') and not isinstance(data, (list, dict, str)):
                        print(f"Processing {key} in streaming mode (detected generator)...")
                        total_records = 0
                        batch_count = 0
                        
                        for batch in data:  # Process each batch as it yields
                            batch_count += 1
                            batch_size = len(batch) if isinstance(batch, list) else 1
                            total_records += batch_size
                            
                            print(f"[{key}] Processing batch {batch_count} with {batch_size} records...")
                            
                            # Process this batch immediately
                            temp_table_records_count = get_temp_table_count()
                            
                            # Use ThreadPoolExecutor to process batch records
                            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                                futures = [
                                    executor.submit(_process_record, rec, temp_table_records)
                                    for rec in batch
                                ]
                                
                                for future in as_completed(futures):
                                    try:
                                        future.result()
                                    except Exception as e:
                                        print(f"Error processing record in {key}: {e}")
                            
                            # Memory cleanup after each batch
                            del batch
                            import gc
                            gc.collect()
                            
                            print(f"[{key}] Completed batch {batch_count}. Total processed: {total_records}")
                        
                        # Store the count instead of data
                        with lock:
                            results[key] = total_records
                        print(f"Completed {key} in streaming mode: {total_records} total records")
                        return
                    
                    # Handle regular crawlers that return lists directly
                    else:
                        with lock:
                            results[key] = data
                        print(f"Completed crawler: {key} ({len(data) if isinstance(data, list) else 'unknown'} records)")
                        
                except Exception as e:
                    print(f"CRITICAL ERROR in crawler {key}: {str(e)[:200]}")
                    import traceback
                    traceback.print_exc()
                    # Set empty result to prevent KeyError later
                    with lock:
                        results[key] = []

            # PROTECTED THREAD EXECUTION
            try:
                # Get enabled crawler names from DB (case-insensitive match to discovered keys)
                enabled_crawlers = list(RicaCrawlerRegistry.objects.filter(enabled=True).values_list('name', flat=True))
                # Normalize to uppercase for matching
                enabled_crawlers = [name.upper() for name in enabled_crawlers]
                threads = [
                    threading.Thread(target=run_and_store, args=(key, crawler_map[key]))
                    for key in enabled_crawlers if key in crawler_map
                ]
                print(f"Starting {len(threads)} threads for data extraction...")
                
                for t in threads:
                    t.start()

                for t in threads:
                    t.join()
                
                print("All threads completed successfully.")
                
            except Exception as e:
                print(f"Error in thread execution: {str(e)[:200]}")
                import traceback
                traceback.print_exc()
                print("Continuing with available results...")

            # PROTECTED DATA AGGREGATION
            try:
                full_analysis = {}
                for key, data in results.items():
                    # Check if this was a streaming crawler (result is a count)
                    if isinstance(data, int):
                        # This was a streaming crawler - records already processed
                        full_analysis[key] = data
                        print(f"{key} processed {data} records in streaming mode")
                    else:
                        # This was a regular crawler - use the data as-is
                        full_analysis[key] = data
            except Exception as e:
                print(f"Error in data aggregation: {str(e)[:100]}")
                full_analysis = {}

            global len_all_records
            all_records = []
            streaming_records_count = 0
            
            for key, data in full_analysis.items():
                # Check if this was a streaming crawler (already processed)
                if isinstance(data, int):
                    streaming_records_count += data
                    continue
                    
                # Process regular crawler data
                if isinstance(data, dict):
                    # dict of lists: flatten all lists
                    for v in data.values():
                        if isinstance(v, list):
                            all_records.extend(v)
                elif isinstance(data, list):
                    # already a list of dicts
                    all_records.extend(data)
                elif hasattr(data, '__iter__') and not isinstance(data, str):
                    # generator of batches (list of dicts) - shouldn't happen with new logic
                    for batch in data:
                        if isinstance(batch, list):
                            all_records.extend(batch)
                else:
                    print(f"Unknown data type for {key}: {type(data)}")
            
            len_all_records = len(all_records) + streaming_records_count
            print(f"Total records across all sources: {len_all_records}")
            print(f"  - Streaming crawler records (already processed): {streaming_records_count}")
            print(f"  - Regular crawler records (to be processed): {len(all_records)}")
           

            # --- BATCHED PROCESSING FOR LARGE DATASETS ---
            new_records, skipped_records, skipped_reasons = [], [], []
            skipped_error_count = 0
            BATCH_SIZE = 10000
            total_records = len(all_records)
            print(f"Processing {total_records} records in batches of {BATCH_SIZE}...")
            for batch_start in range(0, total_records, BATCH_SIZE):
                batch = all_records[batch_start:batch_start+BATCH_SIZE]
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                    futures = []
                    for idx, rec in enumerate(batch):
                        futures.append(pool.submit(_process_record, rec, temp_table_records))
                    for fut_idx, fut in enumerate(as_completed(futures)):
                        try:
                            tag, *payload = fut.result()
                            if tag == "new":
                                new_records.append(payload[0])
                            else:
                                skipped_records.append(payload[0])
                                skipped_reasons.append((payload[0], payload[1]))
                        except Exception as e:
                            skipped_error_count += 1
                            
                            import traceback
                            traceback.print_exc()
                            # Log to a skip error file
                            with open('skipped_batch_error.log', 'a', encoding='utf-8') as ferr:
                                ferr.write(f"Batch start: {batch_start}, Record in batch: {fut_idx}\n")
                                ferr.write(f"Record data: {batch[fut_idx] if fut_idx < len(batch) else 'Unknown'}\n")
                                ferr.write(f"Exception: {str(e)}\n")
                                ferr.write(traceback.format_exc())
                                ferr.write("\n---\n")
                print(f"Processed batch {batch_start//BATCH_SIZE+1} ({min(batch_start+BATCH_SIZE, total_records)}/{total_records})")

            print_processing_summary()
            
            # Monitor temp table state at cycle end
            cycle_end_memory = watchlist_temp_table.count()
            cycle_end_physical = watchlist_temp_table.count_physical()
            print(f"📊 Cycle end - Temp table: {cycle_end_memory} memory, {cycle_end_physical} physical")
            print(f"📈 Temp table accumulation: Started with {cycle_start_memory}, ended with {cycle_end_memory} (net: +{cycle_end_memory - cycle_start_memory})")
            
            print(f"Total problematic (skipped due to error) records: {skipped_error_count}")
            # Update family relationships with full names after all records are processed
            update_family_relationships_with_fullnames()
            update_temp_family_relationships_with_fullnames()
            
            # === NEW DATABASE-DRIVEN ANALYSIS APPROACH ===
            print(f"\n=== SWITCHING TO DATABASE-DRIVEN ANALYSIS ===")
            
            # Get all crawler keys that ran
            crawler_keys = list(full_analysis.keys())
            
            # Build analysis using database queries
            database_analysis = build_database_driven_analysis(crawler_keys)
            
            # Calculate breakdown totals from database analysis FIRST
            breakdown_total = sum(analysis.get('new_record', 0) for analysis in database_analysis.values())
            old_total = sum(analysis.get('old_record', 0) for analysis in database_analysis.values())
            grand_total = sum(analysis.get('total', 0) for analysis in database_analysis.values())
            
            # Update dicts with database-driven analysis
            dicts = {
                'result_len': breakdown_total,  # Use database count instead of len(new_records)
                'analysis': database_analysis
            }
            
            print(f"=== DATABASE ANALYSIS VERIFICATION ===")
            print(f"New records breakdown by source:")
            for list_name, analysis in database_analysis.items():
                old_count = analysis.get('old_record', 0)
                new_count = analysis.get('new_record', 0) 
                total_count = analysis.get('total', 0)
                print(f"  {list_name}: old={old_count:,}, new={new_count:,}, total={total_count:,}")
            
            print(f"\nDatabase Analysis Summary:")
            print(f"  Sum of new records: {breakdown_total:,}")
            print(f"  Sum of old records: {old_total:,}")  # ADD THIS
            print(f"  Sum of total records: {grand_total:,}")
            print(f"  Temp table physical count: {watchlist_temp_table.count_physical():,}")
            print(f"  Analysis accuracy: {'✓' if breakdown_total == watchlist_temp_table.count_physical() else '✗'}")
            print("==========================================\n")
            
            # --- PATCH: Add old_record from TXT file, and always overwrite after run ---
            txt_file = 'previous_record_totals.txt'
            try:
                with open(txt_file, 'w') as f:
                    for list_name, analysis in database_analysis.items():
                        total_count = analysis.get('total', 0)
                        f.write(f"{list_name}:{total_count}\n")
            except Exception as e:
                print(f"Failed to write previous record totals TXT: {e}")
            
            # === ENHANCED BREAKDOWN VERIFICATION WITH DEBUGGING ===
            print(f"\n=== BREAKDOWN VERIFICATION ===")
            breakdown_total = 0
            print(f"New records breakdown by source:")
            for list_name, analysis in dicts['analysis'].items():
                if isinstance(analysis, dict) and 'new_record' in analysis:
                    new_count = analysis['new_record']
                    total_count = analysis.get('total', 0)
                    breakdown_total += new_count
                    print(f"  {list_name}: {new_count:,} new records (total: {total_count:,})")
                else:
                    print(f"  {list_name}: INVALID ANALYSIS STRUCTURE - {type(analysis)}")
            
            print(f"\nBreakdown Summary:")
            print(f"  Sum of breakdown: {breakdown_total:,}")
            print(f"  Total new records: {len(new_records):,}")
            print(f"  Temp table records: {len(temp_table_records):,}")
            print(f"  Breakdown matches: {'✓' if breakdown_total == len(new_records) == len(temp_table_records) else '✗'}")
            
            
            email_sent = False
            if breakdown_total > 0:  # Use database count instead of temp_table_records
                print("There's an update content")
                
                # Add totals to dicts for email template
                dicts['breakdown_total'] = breakdown_total
                dicts['old_total'] = old_total  # ADD THIS
                dicts['grand_total'] = grand_total
                
                # Generate Excel from temp table records
                temp_records_for_excel = watchlist_temp_table.get_all_records_physical()
                excel_path = generate_new_records_excel(temp_records_for_excel)
                
                # Add analysis summary to email body
                dicts['analysis_summary'] = json.dumps(dicts['analysis'], indent=2)
                for subscriber in scanSubscriber:
                    print("subscriber: ", subscriber)
                    dicts['msg_code'] = subscriber.ricaMsg
                    emails = get_emails_receivers(subscriber)
                    if len(emails):
                        send_response_mail(emails, dicts, attachment_paths=[excel_path])
                        email_sent = True
            # Log the run at the end
            log_run_to_db(
                source=','.join(crawler_keys),
                records_found=len_all_records,
                records_new=breakdown_total,  # Use database count
                status="success",
                email_sent=email_sent,
                error_message=None,
                previousRecords=previousRecords
            )
            print("Sleeping for a day")
            time.sleep(24 * 60 * 60)  # Adjust the interval as needed
    except KeyboardInterrupt:
        running_flag = False  # Gracefully stop the loop if Ctrl+C is pressed
    except Exception as e:
        # Log error if exception occurs
        try:
            previousRecords = rica_Watchlist.objects.count()
        except Exception:
            previousRecords = 0
        log_run_to_db(
            source='unknown',
            records_found=0,
            records_new=0,
            status="error",
            email_sent=False,
            error_message=str(e),
            previousRecords=previousRecords
        )
        raise
# def test():
#   print("Testing")
#   # scan1 = run_un_list()
#   # scan2 = run_nigsac()
#   scan3 = run_consolidated_list()
#   full_analysis = { **scan3}
#   new_records = sum([x for x in full_analysis.values() ])
#   dicts = { 'result_len':new_records,"analysis":{**full_analysis }}

#   for list_name in full_analysis:
#       dicts['analysis'][list_name] = {}
#       dicts['analysis'][list_name]['total'] = get_total_list(list_name)
#       dicts['analysis'][list_name]['new_record'] =full_analysis[list_name]
#       dicts['analysis'][list_name]['list_name'] =list_name
#       dicts['analysis'][list_name]['list_desc'] = get_regulator_desc(list_name)
#   print(dicts)
#   if new_records:
#       print("There's an update content")
#       for subscriber in scanSubscriber:
#           sys.exit("subscriber: "+str(subscriber))
#           dicts['msg_code'] = subscriber.ricaMsg
#           emails = get_emails_receivers(subscriber)
#           if len(emails):
#               send_response_mail(emails,dicts)


if __name__ == '__main__':
    # WriteToFile()
    start()
    # Pythonservice.parse_command_line()



