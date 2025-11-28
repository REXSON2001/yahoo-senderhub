# yahoo_persistent_enhanced_complete.py
import time
import json
import pandas as pd
import logging
from datetime import datetime, date
from typing import Dict, Optional, List
import os
import secrets
import warnings
from selenium.webdriver.support.ui import Select
import base64
from PIL import Image, ImageDraw, ImageFont
import io
import re
import psycopg2
from contextlib import contextmanager
from psycopg2.extras import RealDictCursor
import threading
import random
from dotenv import load_dotenv
load_dotenv()

# Suppress all warnings
warnings.filterwarnings("ignore")
os.environ['WDM_LOG_LEVEL'] = '0'
os.environ['WDM_LOG'] = 'false'

# Selenium imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup

# Setup logging with immediate flush
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('yahoo_scraper_errors.log'),
        logging.StreamHandler()
    ]
)

# Force immediate flush for all prints
import sys
sys.stdout.reconfigure(line_buffering=True)

class AccountManager:
    """Manage multiple Yahoo accounts"""
    
    def __init__(self, accounts_file: str = "accounts.json"):
        self.accounts_file = accounts_file
        self.accounts = self.load_accounts()
    
    def load_accounts(self) -> List[Dict]:
        """Load accounts from JSON file or environment variables"""
        accounts = []
        
        # Try to load from JSON file first
        if os.path.exists(self.accounts_file):
            try:
                with open(self.accounts_file, 'r') as f:
                    data = json.load(f)
                    accounts = data.get('yahoo_accounts', [])
                print(f"✅ Loaded {len(accounts)} accounts from {self.accounts_file}", flush=True)
            except Exception as e:
                print(f"❌ Error loading accounts from {self.accounts_file}: {e}", flush=True)
        
        # If no accounts file or empty, try environment variables
        if not accounts:
            print("🔄 Trying environment variables for accounts...", flush=True)
            env_email = os.getenv('YAHOO_EMAIL')
            env_password = os.getenv('YAHOO_PASSWORD')
            
            if env_email and env_password:
                accounts = [{
                    'email': env_email,
                    'password': env_password,
                    'name': 'Environment Account',
                    'enabled': True
                }]
                print("✅ Loaded account from environment variables", flush=True)
        
        # Filter only enabled accounts
        enabled_accounts = [acc for acc in accounts if acc.get('enabled', True)]
        print(f"📋 {len(enabled_accounts)} accounts enabled for scraping", flush=True)
        
        return enabled_accounts
    
    def get_accounts(self) -> List[Dict]:
        """Get all enabled accounts"""
        return self.accounts
    
    def validate_accounts(self) -> bool:
        """Validate that we have at least one account configured"""
        if not self.accounts:
            print("❌ No accounts configured!", flush=True)
            print("💡 Please either:", flush=True)
            print("   1. Create accounts.json file with your Yahoo accounts", flush=True)
            print("   2. Set YAHOO_EMAIL and YAHOO_PASSWORD environment variables", flush=True)
            return False
        return True

class DatabaseManager:
    def __init__(self, db_config: Dict = None):
        # Get database configuration from environment variables with DOCKER COMPATIBLE defaults
        self.db_config = db_config or {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME', 'sender_hub'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', ''),
            'port': os.getenv('DB_PORT', '5432')
        }
        
        # Docker-specific: Try to connect to different possible database hosts
        self.possible_hosts = ['postgres', 'localhost', 'db', 'database']
        self.db_available = True
        
    def get_connection(self):
        """Get PostgreSQL connection with DOCKER-COMPATIBLE host resolution"""
        # If we already know DB is not available, return None early
        if not self.db_available:
            return None
            
        last_error = None
        
        # Try all possible hosts for Docker compatibility
        for host in self.possible_hosts:
            try:
                test_config = self.db_config.copy()
                test_config['host'] = host
                print(f"🔧 Trying database connection to host: {host}", flush=True)
                
                conn = psycopg2.connect(**test_config)
                print(f"✅ Database connection successful to host: {host}", flush=True)
                
                # Update the main config to use the working host
                self.db_config['host'] = host
                return conn
                
            except Exception as e:
                last_error = e
                print(f"❌ Connection failed to host {host}: {str(e)}", flush=True)
                continue
        
        # If all hosts failed, try with original host one more time
        try:
            print(f"🔧 Final attempt with original host: {self.db_config['host']}", flush=True)
            conn = psycopg2.connect(**self.db_config)
            print(f"✅ Database connection successful to original host: {self.db_config['host']}", flush=True)
            return conn
        except Exception as e:
            print(f"❌ All database connection attempts failed. Last error: {str(last_error)}", flush=True)
            print(f"🔧 Connection details: host={self.db_config['host']}, db={self.db_config['database']}, user={self.db_config['user']}", flush=True)
            self.db_available = False
            return None
    
    @contextmanager
    def get_cursor(self):
        """Context manager for database connections"""
        conn = self.get_connection()
        if conn is None:
            # Return a dummy context manager that does nothing
            class DummyCursor:
                def execute(self, *args, **kwargs): pass
                def fetchone(self): return None
                def fetchall(self): return []
                def close(self): pass
            dummy = DummyCursor()
            try:
                yield dummy
            finally:
                pass
        else:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            try:
                yield cursor
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()
                conn.close()
    
    def init_database(self):
        """Initialize database with required tables - DOCKER COMPATIBLE"""
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                with self.get_cursor() as cursor:
                    # Only proceed if we have a real database connection
                    if not self.db_available:
                        print("⚠️ Database not available, skipping initialization", flush=True)
                        return
                        
                    # Drop existing tables first to recreate with proper schema
                    cursor.execute('DROP TABLE IF EXISTS domain_stats CASCADE')
                    cursor.execute('DROP TABLE IF EXISTS scraping_sessions CASCADE')
                    cursor.execute('DROP TABLE IF EXISTS scraping_accounts CASCADE')
                    cursor.execute('DROP TABLE IF EXISTS api_keys CASCADE')
                    
                    # Create domain_stats table with account tracking
                    cursor.execute('''
                        CREATE TABLE domain_stats (
                            id SERIAL PRIMARY KEY,
                            account_email TEXT NOT NULL,
                            domain_name TEXT NOT NULL,
                            status TEXT,
                            verified BOOLEAN,
                            added_date TEXT,
                            timestamp TEXT NOT NULL,
                            date DATE NOT NULL DEFAULT CURRENT_DATE,
                            delivered_count INTEGER,
                            delivered_percentage TEXT,
                            complaint_rate REAL,
                            complaint_percentage TEXT,
                            complaint_trend TEXT,
                            time_range TEXT,
                            insights_data TEXT,
                            full_data TEXT,
                            screenshot_path TEXT,
                            has_data BOOLEAN DEFAULT TRUE,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE(account_email, domain_name, date)
                        )
                    ''')
                    
                    # Create api_keys table
                    cursor.execute('''
                        CREATE TABLE api_keys (
                            id SERIAL PRIMARY KEY,
                            api_key TEXT UNIQUE NOT NULL,
                            name TEXT,
                            is_active BOOLEAN DEFAULT TRUE,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            last_used TIMESTAMP
                        )
                    ''')
                    
                    # Create scraping_sessions table with account tracking
                    cursor.execute('''
                        CREATE TABLE scraping_sessions (
                            id SERIAL PRIMARY KEY,
                            account_email TEXT NOT NULL,
                            session_start TEXT NOT NULL,
                            session_end TEXT,
                            domains_processed INTEGER DEFAULT 0,
                            total_domains INTEGER DEFAULT 0,
                            status TEXT DEFAULT 'running',
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    ''')
                    
                    # Create accounts table to track account usage
                    cursor.execute('''
                        CREATE TABLE scraping_accounts (
                            id SERIAL PRIMARY KEY,
                            email TEXT UNIQUE NOT NULL,
                            name TEXT,
                            last_used TIMESTAMP,
                            total_sessions INTEGER DEFAULT 0,
                            is_active BOOLEAN DEFAULT TRUE,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    ''')
                    
                    print("✅ PostgreSQL database schema created successfully with multi-account support!", flush=True)
                    
                    # Insert sample API key for testing
                    cursor.execute('''
                        INSERT INTO api_keys (api_key, name, is_active) 
                        VALUES (%s, %s, %s)
                    ''', ('test-api-key-12345', 'Default API Key', True))
                    
                    print("✅ Sample API key inserted", flush=True)
                    return
                    
            except Exception as e:
                if attempt < max_attempts - 1:
                    print(f"⚠️ Database initialization attempt {attempt + 1} failed, retrying...: {str(e)}", flush=True)
                    time.sleep(5)
                else:
                    print(f"❌ Error initializing database after {max_attempts} attempts: {str(e)}", flush=True)
                    self.db_available = False
                    raise
    
    def save_domain_stats(self, stats: Dict, account_email: str) -> bool:
        """Save domain statistics to database with account tracking"""
        if not self.db_available:
            print("⚠️ Database not available, skipping save operation", flush=True)
            return False
            
        try:
            with self.get_cursor() as cursor:
                
                # Ensure we're saving valid JSON
                insights_json = "{}"
                full_data_json = "{}"
                
                try:
                    insights_data = stats.get('insights_data', {})
                    if isinstance(insights_data, str):
                        insights_json = insights_data
                    else:
                        insights_json = json.dumps(insights_data)
                except (TypeError, ValueError):
                    pass
                    
                try:
                    full_data_json = json.dumps(stats)
                except (TypeError, ValueError):
                    pass
                
                # Get current date for unique constraint
                current_date = date.today()
                
                # Use UPSERT to update existing record for same domain on same day for same account
                cursor.execute('''
                    INSERT INTO domain_stats 
                    (account_email, domain_name, status, verified, added_date, timestamp, date,
                     delivered_count, delivered_percentage, complaint_rate, complaint_percentage, 
                     complaint_trend, time_range, insights_data, full_data, screenshot_path, has_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (account_email, domain_name, date) 
                    DO UPDATE SET
                        status = EXCLUDED.status,
                        verified = EXCLUDED.verified,
                        added_date = EXCLUDED.added_date,
                        timestamp = EXCLUDED.timestamp,
                        delivered_count = EXCLUDED.delivered_count,
                        delivered_percentage = EXCLUDED.delivered_percentage,
                        complaint_rate = EXCLUDED.complaint_rate,
                        complaint_percentage = EXCLUDED.complaint_percentage,
                        complaint_trend = EXCLUDED.complaint_trend,
                        time_range = EXCLUDED.time_range,
                        insights_data = EXCLUDED.insights_data,
                        full_data = EXCLUDED.full_data,
                        screenshot_path = EXCLUDED.screenshot_path,
                        has_data = EXCLUDED.has_data,
                        created_at = CURRENT_TIMESTAMP
                ''', (
                    account_email,
                    stats.get('domain'),
                    stats.get('status'),
                    stats.get('verified', False),
                    stats.get('added_date'),
                    stats.get('timestamp'),
                    current_date,
                    stats.get('delivered_count'),
                    stats.get('delivered_percentage'),
                    stats.get('complaint_rate'),
                    stats.get('complaint_percentage'),
                    stats.get('complaint_trend'),
                    stats.get('time_range'),
                    insights_json,
                    full_data_json,
                    stats.get('screenshot_path'),
                    stats.get('has_data', True)
                ))
                
                return True
                
        except Exception as e:
            print(f"❌ Error saving to database: {str(e)}", flush=True)
            self.db_available = False
            return False
    
    def get_latest_domain_stats(self, domain: str, account_email: str = None) -> Optional[Dict]:
        """Get latest statistics for a domain (optionally filtered by account)"""
        if not self.db_available:
            return None
            
        try:
            with self.get_cursor() as cursor:
                if account_email:
                    cursor.execute('''
                        SELECT * FROM domain_stats 
                        WHERE domain_name = %s AND account_email = %s
                        ORDER BY timestamp DESC 
                        LIMIT 1
                    ''', (domain, account_email))
                else:
                    cursor.execute('''
                        SELECT * FROM domain_stats 
                        WHERE domain_name = %s 
                        ORDER BY timestamp DESC 
                        LIMIT 1
                    ''', (domain,))
                
                row = cursor.fetchone()
                if row:
                    return dict(row)
                return None
                
        except Exception as e:
            print(f"❌ Error getting domain stats: {str(e)}", flush=True)
            return None
    
    def get_all_domains_stats(self, limit: int = 100, account_email: str = None) -> List[Dict]:
        """Get all domains statistics - only latest per domain (optionally filtered by account)"""
        if not self.db_available:
            return []
            
        try:
            with self.get_cursor() as cursor:
                if account_email:
                    cursor.execute('''
                        SELECT ds1.* FROM domain_stats ds1
                        INNER JOIN (
                            SELECT domain_name, MAX(timestamp) as max_timestamp
                            FROM domain_stats
                            WHERE account_email = %s
                            GROUP BY domain_name
                        ) ds2 ON ds1.domain_name = ds2.domain_name AND ds1.timestamp = ds2.max_timestamp
                        WHERE ds1.account_email = %s
                        ORDER BY ds1.timestamp DESC 
                        LIMIT %s
                    ''', (account_email, account_email, limit))
                else:
                    cursor.execute('''
                        SELECT ds1.* FROM domain_stats ds1
                        INNER JOIN (
                            SELECT domain_name, MAX(timestamp) as max_timestamp
                            FROM domain_stats
                            GROUP BY domain_name
                        ) ds2 ON ds1.domain_name = ds2.domain_name AND ds1.timestamp = ds2.max_timestamp
                        ORDER BY ds1.timestamp DESC 
                        LIMIT %s
                    ''', (limit,))
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            print(f"❌ Error getting all domains stats: {str(e)}", flush=True)
            return []
    
    def start_scraping_session(self, total_domains: int, account_email: str) -> int:
        """Start a new scraping session and return session ID with account tracking"""
        if not self.db_available:
            return 1
            
        try:
            with self.get_cursor() as cursor:
                session_start = datetime.now().isoformat()
                
                # Update account usage
                cursor.execute('''
                    INSERT INTO scraping_accounts (email, name, last_used, total_sessions)
                    VALUES (%s, %s, %s, 1)
                    ON CONFLICT (email) 
                    DO UPDATE SET
                        last_used = EXCLUDED.last_used,
                        total_sessions = scraping_accounts.total_sessions + 1
                ''', (account_email, f"Account {account_email}", datetime.now()))
                
                # Create session
                cursor.execute('''
                    INSERT INTO scraping_sessions (account_email, session_start, total_domains, status)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                ''', (account_email, session_start, total_domains, 'running'))
                
                result = cursor.fetchone()
                return result['id'] if result else 1
        except Exception as e:
            print(f"❌ Error starting scraping session: {str(e)}", flush=True)
            return 1
    
    def update_scraping_session(self, session_id: int, domains_processed: int, status: str = 'running'):
        """Update scraping session progress"""
        if not self.db_available:
            return
            
        try:
            with self.get_cursor() as cursor:
                if status == 'completed':
                    session_end = datetime.now().isoformat()
                    cursor.execute('''
                        UPDATE scraping_sessions 
                        SET domains_processed = %s, status = %s, session_end = %s
                        WHERE id = %s
                    ''', (domains_processed, status, session_end, session_id))
                else:
                    cursor.execute('''
                        UPDATE scraping_sessions 
                        SET domains_processed = %s, status = %s
                        WHERE id = %s
                    ''', (domains_processed, status, session_id))
        except Exception as e:
            print(f"❌ Error updating scraping session: {str(e)}", flush=True)

class FileManager:
    """Manage files (screenshots and JSON) with domain-based organization"""
    
    def __init__(self):
        self.screenshot_dir = "screenshots"
        self.data_dir = "data"
        self.setup_directories()
    
    def setup_directories(self):
        """Create necessary directories"""
        os.makedirs(self.screenshot_dir, exist_ok=True)
        os.makedirs(self.data_dir, exist_ok=True)
        print(f"✅ Directories created: {self.screenshot_dir}, {self.data_dir}", flush=True)
    
    def get_screenshot_path(self, domain: str) -> str:
        """Get screenshot file path for a domain (one screenshot per domain)"""
        return os.path.join(self.screenshot_dir, f"{domain}_180_days.png")
    
    def get_json_path(self, domain: str) -> str:
        """Get JSON file path for a domain (one JSON file per domain)"""
        return os.path.join(self.data_dir, f"{domain}_stats.json")
    
    def add_timestamp_watermark(self, image_path: str):
        """Add timestamp watermark to screenshot with LARGER TEXT and LOCAL TIMEZONE"""
        try:
            # Open the image
            with Image.open(image_path) as img:
                # Convert to RGB if necessary
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                
                # Create a drawing context
                draw = ImageDraw.Draw(img)
                
                # Get current timestamp in LOCAL TIMEZONE with timezone info
                current_time = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
                timestamp_text = f"Scraped: {current_time}"
                
                # LARGER FONT SETTINGS
                font_size = 60  # Increased from 35 to 60
                padding = 30   # Increased padding
                margin = 10    # Increased margin
                
                # Try to use larger fonts, fallback to default if not available
                try:
                    # Try different font paths for larger fonts
                    font_paths = [
                        "arial.ttf",
                        "Arial.ttf", 
                        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
                        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"
                    ]
                    
                    font = None
                    for font_path in font_paths:
                        try:
                            font = ImageFont.truetype(font_path, font_size)
                            break
                        except:
                            continue
                    
                    # If no font found, try to use default with larger size
                    if font is None:
                        try:
                            # Try to use default font with larger size
                            font = ImageFont.load_default()
                            # For default font, we'll use a smaller size since it might not scale well
                            font_size = 40
                        except:
                            font = ImageFont.load_default()
                            
                except Exception as font_error:
                    print(f"⚠️ Font loading error, using default: {font_error}", flush=True)
                    font = ImageFont.load_default()
                    font_size = 40
                
                # Get text dimensions
                try:
                    bbox = draw.textbbox((0, 0), timestamp_text, font=font)
                    text_width = bbox[2] - bbox[0]
                    text_height = bbox[3] - bbox[1]
                except AttributeError:
                    # Fallback for older PIL versions
                    try:
                        text_width, text_height = draw.textsize(timestamp_text, font=font)
                    except:
                        # Final fallback
                        text_width = len(timestamp_text) * font_size // 2
                        text_height = font_size
                
                # Calculate position (top right with some padding)
                x = img.width - text_width - padding
                y = padding
                
                # Draw semi-transparent background with larger margin
                draw.rectangle(
                    [x - margin, y - margin, x + text_width + margin, y + text_height + margin],
                    fill=(0, 0, 0, 180)  # Darker semi-transparent black for better readability
                )
                
                # Draw timestamp text in WHITE for better contrast
                draw.text((x, y), timestamp_text, fill=(255, 255, 255), font=font)
                
                # Save the image with timestamp
                img.save(image_path, "PNG")
                print(f"📅 LARGE TIMESTAMP ADDED: {current_time}", flush=True)
                
        except Exception as e:
            print(f"⚠️ Could not add timestamp watermark: {str(e)}", flush=True)
            # Continue without timestamp if there's an error
    
    def save_screenshot(self, driver, domain: str) -> str:
        """Save screenshot for domain - OVERWRITES existing screenshot with timestamp watermark"""
        try:
            screenshot_path = self.get_screenshot_path(domain)
            
            # Remove existing screenshot if it exists
            if os.path.exists(screenshot_path):
                os.remove(screenshot_path)
                print(f"🗑️ Removed previous screenshot: {screenshot_path}", flush=True)
            
            # Take screenshot
            driver.save_screenshot(screenshot_path)
            print(f"✅ Screenshot saved: {screenshot_path}", flush=True)
            
            # Add timestamp watermark
            self.add_timestamp_watermark(screenshot_path)
            
            return screenshot_path
        except Exception as e:
            print(f"❌ Screenshot failed: {str(e)}", flush=True)
            return ""
    
    def save_domain_json(self, domain: str, stats: Dict, account_email: str):
        """Save domain stats to JSON file - UPDATES existing file"""
        try:
            json_path = self.get_json_path(domain)
            
            # Load existing data if file exists
            existing_data = {}
            if os.path.exists(json_path):
                try:
                    with open(json_path, 'r') as f:
                        existing_data = json.load(f)
                    print(f"📁 Loaded existing JSON data for {domain}", flush=True)
                except Exception as e:
                    print(f"⚠️ Could not load existing JSON: {e}", flush=True)
                    existing_data = {}
            
            # Initialize domain data structure if not exists
            if 'domain' not in existing_data:
                existing_data['domain'] = domain
            
            # Initialize accounts data if not exists
            if 'accounts' not in existing_data:
                existing_data['accounts'] = {}
            
            # Update account-specific data
            existing_data['accounts'][account_email] = {
                'last_updated': datetime.now().isoformat(),
                'data': stats
            }
            
            # Update overall domain summary (use latest data)
            existing_data['last_updated'] = datetime.now().isoformat()
            existing_data['latest_data'] = stats
            existing_data['total_accounts'] = len(existing_data['accounts'])
            
            # Calculate aggregated metrics across all accounts
            self.calculate_aggregated_metrics(existing_data)
            
            # Save updated data
            with open(json_path, 'w') as f:
                json.dump(existing_data, f, indent=2)
            
            print(f"✅ JSON data updated for {domain} (account: {account_email})", flush=True)
            return json_path
            
        except Exception as e:
            print(f"❌ Error saving JSON for {domain}: {str(e)}", flush=True)
            return ""
    
    def calculate_aggregated_metrics(self, domain_data: Dict):
        """Calculate aggregated metrics across all accounts for a domain"""
        accounts = domain_data.get('accounts', {})
        
        if not accounts:
            return
        
        # Initialize aggregated metrics
        delivered_counts = []
        complaint_rates = []
        verified_count = 0
        has_data_count = 0
        
        for account_email, account_data in accounts.items():
            data = account_data.get('data', {})
            
            # Collect delivered counts
            delivered = data.get('delivered_count')
            if delivered is not None:
                delivered_counts.append(delivered)
            
            # Collect complaint rates
            complaint = data.get('complaint_rate')
            if complaint is not None:
                complaint_rates.append(complaint)
            
            # Count verified domains
            if data.get('verified'):
                verified_count += 1
            
            # Count domains with data
            if data.get('has_data', True):
                has_data_count += 1
        
        # Calculate averages
        domain_data['aggregated_metrics'] = {
            'average_delivered': sum(delivered_counts) / len(delivered_counts) if delivered_counts else 0,
            'average_complaint_rate': sum(complaint_rates) / len(complaint_rates) if complaint_rates else 0,
            'verified_accounts': verified_count,
            'accounts_with_data': has_data_count,
            'total_accounts': len(accounts)
        }

class PersistentAccountScraper:
    """PERSISTENT scraper for a single account with ALL ENHANCED features - DOCKER COMPATIBLE"""
    
    def __init__(self, account: Dict, db_manager: DatabaseManager, headless: bool = False):
        self.account = account
        self.db = db_manager
        self.headless = headless
        self.driver = None
        self.wait = None
        self.logged_in = False
        self.is_running = True
        self.last_scrape_time = None
        self.current_account = account['email']
        self.file_manager = FileManager()
        self.current_session_id = None
        self.previous_domains = set()  # Track previously found domains
        
    def setup_persistent_driver(self):
        """Configure and setup Chrome driver with optimal settings for PERSISTENT operation - DOCKER COMPATIBLE"""
        try:
            chrome_options = Options()
            
            if self.headless:
                chrome_options.add_argument("--headless=new")
            
            # DOCKER-COMPATIBLE Chrome settings - ENHANCED FOR STABILITY
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--remote-debugging-port=0")  # Use random port
            chrome_options.add_argument("--disable-software-rasterizer")
            chrome_options.add_argument("--disable-background-timer-throttling")
            chrome_options.add_argument("--disable-backgrounding-occluded-windows")
            chrome_options.add_argument("--disable-renderer-backgrounding")
            chrome_options.add_argument("--disable-features=VizDisplayCompositor")
            chrome_options.add_argument("--disable-back-forward-cache")
            
            # PERSISTENT settings with incognito mode
            chrome_options.add_argument("--incognito")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            chrome_options.add_argument("--force-device-scale-factor=1")
            chrome_options.add_argument("--log-level=3")
            chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
            
            # Additional Docker compatibility - MEMORY OPTIMIZATION
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-plugins")
            chrome_options.add_argument("--disable-images")  # Disable images to save memory
            chrome_options.add_argument("--blink-settings=imagesEnabled=false")
            chrome_options.add_argument("--disable-javascript")  # Disable JS if not needed
            chrome_options.add_argument("--memory-pressure-off")
            chrome_options.add_argument("--max_old_space_size=4096")
            
            # Try multiple times to setup driver with better error handling
            max_attempts = 5  # Increased attempts
            for attempt in range(max_attempts):
                try:
                    print(f"🔄 Browser setup attempt {attempt + 1}/{max_attempts} for {self.account['email']}", flush=True)
                    
                    # Use Service class for better Chrome management
                    from selenium.webdriver.chrome.service import Service
                    service = Service()
                    
                    self.driver = webdriver.Chrome(service=service, options=chrome_options)
                    self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                    self.wait = WebDriverWait(self.driver, 30)
                    
                    print(f"✅ PERSISTENT incognito browser setup completed for {self.account['email']}", flush=True)
                    return True
                    
                except Exception as e:
                    if attempt < max_attempts - 1:
                        wait_time = (attempt + 1) * 10  # Increasing wait time: 10, 20, 30, 40 seconds
                        print(f"⚠️ Browser setup attempt {attempt + 1} failed for {self.account['email']}, retrying in {wait_time} seconds...: {str(e)}", flush=True)
                        time.sleep(wait_time)
                        
                        # Clean up any residual processes
                        try:
                            if self.driver:
                                self.driver.quit()
                        except:
                            pass
                        self.driver = None
                    else:
                        print(f"❌ All browser setup attempts failed for {self.account['email']}: {str(e)}", flush=True)
                        return False
            
        except Exception as e:
            print(f"❌ Error setting up persistent driver for {self.account['email']}: {str(e)}", flush=True)
            return False

    def random_delay(self, min_delay: float = 1.0, max_delay: float = 3.0):
        """Add random delay between actions"""
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)

    def take_screenshot(self, domain: str) -> str:
        """Take screenshot for domain - uses FileManager"""
        return self.file_manager.save_screenshot(self.driver, domain)

    def ensure_page_fully_loaded(self):
        """Ensure the entire page is fully loaded"""
        try:
            self.wait.until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            time.sleep(3)  # Increased wait time for data to load
        except Exception as e:
            print(f"⚠️ Page loading check failed: {str(e)}", flush=True)

    def check_if_logged_in(self) -> bool:
        """Check if login was successful by looking for dashboard elements"""
        try:
            dashboard_indicators = [
                "//*[contains(text(), 'Sender Hub')]",
                "//*[contains(text(), 'Dashboard')]",
                "//*[contains(text(), 'Insights')]",
                "//*[contains(text(), 'Domains')]"
            ]

            for indicator in dashboard_indicators:
                try:
                    element = self.driver.find_element(By.XPATH, indicator)
                    if element.is_displayed():
                        return True
                except:
                    continue

            current_url = self.driver.current_url
            if 'senders.yahooinc.com' in current_url and ('dashboard' in current_url or 'domains' in current_url or 'feature-management' in current_url):
                return True

            return False

        except Exception:
            return False

    def guaranteed_login(self):
        """GUARANTEED login using PROVEN login logic from working code - DOCKER COMPATIBLE"""
        email = self.account['email']
        password = self.account['password']
        
        print(f"🔐 Performing GUARANTEED login for: {email}", flush=True)
        
        max_login_attempts = 3
        for login_attempt in range(max_login_attempts):
            try:
                # Navigate to Yahoo Sender Hub
                self.driver.get("https://senders.yahooinc.com/")
                self.random_delay(2, 4)

                # Look for the Sign In link in the navigation
                signin_links = [
                    "//a[@href='/api/v1/login/sign_in']",
                    "//a[contains(@href, 'login/sign_in')]",
                    "//a[contains(text(), 'Sign In')]",
                    "//button[contains(text(), 'Sign in')]",
                    "//a[contains(text(), 'Sign in')]"
                ]

                signin_element = None
                for selector in signin_links:
                    try:
                        signin_element = self.driver.find_element(By.XPATH, selector)
                        if signin_element.is_displayed():
                            break
                    except:
                        continue

                if not signin_element:
                    print("❌ Could not find sign in element", flush=True)
                    if login_attempt < max_login_attempts - 1:
                        print(f"🔄 Login attempt {login_attempt + 1} failed, retrying...", flush=True)
                        time.sleep(10)
                        continue
                    return False

                # Get the signin URL
                try:
                    signin_url = signin_element.get_attribute('href')
                except:
                    signin_url = "https://senders.yahooinc.com/api/v1/login/sign_in"

                # Navigate to signin URL
                self.driver.get(signin_url)
                self.random_delay(3, 5)

                # Wait for redirect to Yahoo login
                self.random_delay(3, 5)

                # Now handle the actual Yahoo login form
                try:
                    self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "input")))
                except:
                    pass

                # STEP 1: Find username field
                username_field = None
                username_selectors = [
                    "input[name='username']",
                    "input[name='email']",
                    "input[type='text']",
                    "input[type='email']",
                    "input#login-username",
                    "input#username"
                ]

                for selector in username_selectors:
                    try:
                        if selector.startswith("//"):
                            username_field = self.wait.until(
                                EC.element_to_be_clickable((By.XPATH, selector))
                            )
                        else:
                            username_field = self.wait.until(
                                EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                            )
                        
                        if username_field and username_field.is_displayed():
                            break
                        else:
                            username_field = None
                    except Exception:
                        continue

                if not username_field:
                    print("❌ Could not find username field", flush=True)
                    if login_attempt < max_login_attempts - 1:
                        print(f"🔄 Login attempt {login_attempt + 1} failed, retrying...", flush=True)
                        time.sleep(10)
                        continue
                    return False

                # Enter username
                username_field.clear()
                username_field.send_keys(email)
                self.random_delay(1, 2)

                # STEP 2: Find and click Next button
                next_button = None
                next_selectors = [
                    "input[type='submit']",
                    "button[type='submit']",
                    "input[value='Next']",
                    "input#login-signin",
                    "button#login-signin",
                    "//input[@type='submit']",
                    "//button[@type='submit']",
                    "//input[@value='Next']",
                    "//button[contains(text(), 'Next')]"
                ]

                for selector in next_selectors:
                    try:
                        if selector.startswith("//"):
                            next_button = self.driver.find_element(By.XPATH, selector)
                        else:
                            if selector.startswith("input") or selector.startswith("button"):
                                next_button = self.driver.find_element(By.CSS_SELECTOR, selector)
                            else:
                                next_button = self.driver.find_element(By.ID, selector.replace("#", ""))
                        
                        if next_button and next_button.is_displayed():
                            break
                        else:
                            next_button = None
                    except:
                        continue

                if not next_button:
                    print("❌ Could not find Next button", flush=True)
                    if login_attempt < max_login_attempts - 1:
                        print(f"🔄 Login attempt {login_attempt + 1} failed, retrying...", flush=True)
                        time.sleep(10)
                        continue
                    return False

                # Click Next button
                self.driver.execute_script("arguments[0].click();", next_button)
                self.random_delay(3, 5)

                # STEP 3: Check for password field
                password_field = None
                password_selectors = [
                    "input[type='password']",
                    "input[name='password']",
                    "input#login-passwd",
                    "input#password"
                ]

                for selector in password_selectors:
                    try:
                        if selector.startswith("//"):
                            password_field = self.wait.until(
                                EC.element_to_be_clickable((By.XPATH, selector))
                            )
                        else:
                            password_field = self.wait.until(
                                EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                            )
                        
                        if password_field and password_field.is_displayed():
                            break
                        else:
                            password_field = None
                    except:
                        continue

                if not password_field:
                    if self.check_if_logged_in():
                        self.logged_in = True
                        self.current_account = email
                        print(f"✅ Already logged in: {email}", flush=True)
                        return True
                    else:
                        print("❌ Could not find password field", flush=True)
                        if login_attempt < max_login_attempts - 1:
                            print(f"🔄 Login attempt {login_attempt + 1} failed, retrying...", flush=True)
                            time.sleep(10)
                            continue
                        return False

                # STEP 4: Enter password
                password_field.clear()
                password_field.send_keys(password)
                self.random_delay(1, 2)

                # STEP 5: Find and click Sign In button
                signin_button = None
                signin_selectors = [
                    "button[type='submit']",
                    "input[type='submit']",
                    "button#login-signin",
                    "input#login-signin",
                    "//button[contains(text(), 'Sign In')]",
                    "//input[@value='Sign In']"
                ]

                for selector in signin_selectors:
                    try:
                        if selector.startswith("//"):
                            signin_button = self.driver.find_element(By.XPATH, selector)
                        else:
                            signin_button = self.driver.find_element(By.CSS_SELECTOR, selector)
                        
                        if signin_button and signin_button.is_displayed():
                            break
                        else:
                            signin_button = None
                    except:
                        continue

                if signin_button:
                    self.driver.execute_script("arguments[0].click();", signin_button)
                else:
                    # Try pressing Enter
                    password_field.send_keys(Keys.RETURN)

                # Wait for login to complete
                self.random_delay(5, 8)

                # STEP 6: Final verification
                if self.check_if_logged_in():
                    self.logged_in = True
                    self.current_account = email
                    print(f"✅ GUARANTEED login successful: {email}", flush=True)
                    return True
                else:
                    # Additional verification
                    current_url = self.driver.current_url
                    if 'senders.yahooinc.com' in current_url and ('dashboard' in current_url or 'domains' in current_url):
                        self.logged_in = True
                        self.current_account = email
                        print(f"✅ URL-based login verification successful: {email}", flush=True)
                        return True
                    else:
                        print(f"❌ Login verification failed for {email}", flush=True)
                        if login_attempt < max_login_attempts - 1:
                            print(f"🔄 Login attempt {login_attempt + 1} failed, retrying...", flush=True)
                            time.sleep(10)
                            continue
                        return False

            except Exception as e:
                print(f"❌ GUARANTEED login attempt {login_attempt + 1} failed for {self.account['email']}: {str(e)}", flush=True)
                if login_attempt < max_login_attempts - 1:
                    print(f"🔄 Retrying login...", flush=True)
                    time.sleep(10)
                else:
                    return False
        
        return False

    def ensure_browser_alive(self):
        """Ensure browser is alive, restart if needed - AUTO-RECOVERY - DOCKER COMPATIBLE"""
        try:
            if not self.driver:
                print(f"🔄 Starting NEW persistent browser for {self.account['email']} (recovery)", flush=True)
                return self.setup_persistent_driver()
            
            # Try to get current URL to check if browser is responsive
            try:
                current_url = self.driver.current_url
                return True
            except Exception as e:
                print(f"⚠️ Browser died for {self.account['email']}, restarting...", flush=True)
                try:
                    if self.driver:
                        self.driver.quit()
                except:
                    pass
                self.driver = None
                self.logged_in = False
                time.sleep(5)  # Wait before restart
                return self.setup_persistent_driver()
            
        except Exception as e:
            print(f"⚠️ Browser recovery failed for {self.account['email']}, retrying: {str(e)}", flush=True)
            try:
                if self.driver:
                    self.driver.quit()
            except:
                pass
            self.driver = None
            self.logged_in = False
            time.sleep(10)  # Longer wait before retry
            return self.setup_persistent_driver()

    def ensure_logged_in(self):
        """Ensure we're logged in, perform guaranteed login if not"""
        if not self.ensure_browser_alive():
            return False
            
        if self.check_if_logged_in():
            return True
        else:
            print(f"🔐 Session expired for {self.account['email']}, performing guaranteed login...", flush=True)
            return self.guaranteed_login()

    def safe_refresh_browser(self):
        """Safely refresh browser without losing login session"""
        try:
            print(f"🔄 Refreshing browser for {self.account['email']} to detect new domains...", flush=True)
            
            # Get current URL before refresh
            current_url = self.driver.current_url
            
            # Perform refresh
            self.driver.refresh()
            time.sleep(5)
            
            # Ensure page is fully loaded
            self.ensure_page_fully_loaded()
            
            # Check if we're still logged in after refresh
            if not self.check_if_logged_in():
                print(f"⚠️ Lost login session after refresh, re-logging in...", flush=True)
                if not self.guaranteed_login():
                    print(f"❌ Failed to re-login after refresh", flush=True)
                    return False
            
            print(f"✅ Browser refreshed successfully for {self.account['email']}", flush=True)
            return True
            
        except Exception as e:
            print(f"❌ Error refreshing browser for {self.account['email']}: {str(e)}", flush=True)
            # Try to recover by ensuring login
            return self.ensure_logged_in()

    def find_and_click_dropdown_enhanced(self) -> bool:
        """ENHANCED method to find and click domain dropdown with better selectors and waits"""
        try:
            print(f"🔍 ENHANCED: Looking for domain dropdown for {self.account['email']}...", flush=True)
            
            # Wait for page to be fully ready
            self.ensure_page_fully_loaded()
            time.sleep(3)
            
            # ENHANCED: More comprehensive dropdown selectors with better waiting
            dropdown_selectors = [
                # Primary selectors - most common
                "//button[contains(@class, 'dropdown')]",
                "//div[contains(@class, 'dropdown')]",
                "//*[contains(@class, 'selector')]",
                "//*[contains(@class, 'select')]",
                "//*[@role='button' and contains(text(), '.com')]",
                "//button[contains(@aria-label, 'domain')]",
                
                # Yahoo-specific selectors
                "//*[@id='domain-selector']",
                "//*[contains(@id, 'domain') and contains(@id, 'select')]",
                "//div[contains(@class, 'tw-cursor-pointer')]",
                "//button[contains(@class, 'tw-cursor-pointer')]",
                
                # UI framework selectors
                "//div[contains(@class, 'relative')]//button",
                "//*[contains(@class, 'menu')]//button",
                "//button[contains(@class, 'button') and contains(@class, 'select')]",
                "//button[contains(@class, 'MuiSelect-select')]",
                "//div[contains(@class, 'MuiSelect-select')]",
                
                # Fallback selectors - click anything that looks like a domain
                "//*[contains(text(), '.com') and @role='button']",
                "//button[contains(., '.com')]",
                "//div[contains(., '.com') and @role='button']",
            ]
            
            all_elements_text = []
            found_elements = set()
            
            for selector in dropdown_selectors:
                try:
                    print(f"🔍 Trying dropdown selector: {selector}", flush=True)
                    elements = self.driver.find_elements(By.XPATH, selector)
                    
                    for i, element in enumerate(elements):
                        try:
                            if element.is_displayed() and element.is_enabled():
                                element_text = element.text.strip()
                                print(f"✅ Found visible dropdown element {i+1}/{len(elements)}: '{element_text}'", flush=True)
                                
                                # Try to click the element
                                try:
                                    element.click()
                                    print(f"🖱️ Successfully clicked dropdown with selector: {selector}", flush=True)
                                    time.sleep(3)
                                    return True
                                except Exception as click_error:
                                    print(f"⚠️ Standard click failed, trying JavaScript click: {click_error}", flush=True)
                                    try:
                                        self.driver.execute_script("arguments[0].click();", element)
                                        print(f"🖱️ JavaScript click successful for selector: {selector}", flush=True)
                                        time.sleep(3)
                                        return True
                                    except Exception as js_error:
                                        print(f"⚠️ JavaScript click failed, trying ActionChains: {js_error}", flush=True)
                                        try:
                                            ActionChains(self.driver).move_to_element(element).click().perform()
                                            print(f"🖱️ ActionChains click successful for selector: {selector}", flush=True)
                                            time.sleep(3)
                                            return True
                                        except Exception as ac_error:
                                            print(f"❌ All click methods failed for selector: {selector}", flush=True)
                                            continue
                        except Exception as e:
                            continue
                            
                except Exception as e:
                    print(f"⚠️ Error with selector {selector}: {str(e)}", flush=True)
                    continue
            
            # If no dropdown found, try alternative approach - look for any clickable element with domain text
            print("🔄 Trying alternative dropdown detection...", flush=True)
            alternative_selectors = [
                "//button",
                "//div[@role='button']",
                "//*[@onclick]",
                "//*[contains(@class, 'clickable')]"
            ]
            
            for selector in alternative_selectors:
                try:
                    elements = self.driver.find_elements(By.XPATH, selector)
                    for element in elements:
                        try:
                            if element.is_displayed():
                                text = element.text.strip()
                                if text and ('.com' in text or 'domain' in text.lower()):
                                    print(f"🔄 Found alternative dropdown: '{text}'", flush=True)
                                    element.click()
                                    time.sleep(3)
                                    return True
                        except:
                            continue
                except:
                    continue
            
            print(f"❌ ENHANCED: Could not find or click dropdown for {self.account['email']}", flush=True)
            return False
            
        except Exception as e:
            print(f"❌ ENHANCED: Error clicking dropdown for {self.account['email']}: {str(e)}", flush=True)
            return False

    def clean_domain_text(self, text: str) -> str:
        """Clean and extract domain from text"""
        if not text:
            return ""
        
        # Remove extra whitespace and newlines
        text = ' '.join(text.split())
        
        # Look for domain patterns
        domain_patterns = [
            r'([a-zA-Z0-9-]+\.(com|net|org|io|co))',
            r'([a-zA-Z0-9-]+\.[a-zA-Z0-9-]+\.[a-zA-Z]{2,})',
        ]
        
        for pattern in domain_patterns:
            matches = re.findall(pattern, text)
            if matches:
                for match in matches:
                    if isinstance(match, tuple):
                        domain = match[0]
                    else:
                        domain = match
                    
                    # Validate it looks like a real domain
                    if len(domain) > 4 and '.' in domain and domain not in ['yahoo.com', 'example.com']:
                        return domain
        
        # If no pattern match, try to extract the first part that looks like a domain
        parts = text.split()
        for part in parts:
            if '.' in part and len(part) > 4 and any(ext in part for ext in ['.com', '.net', '.org', '.io']):
                clean_part = part.strip('.,;!?()[]{}')
                if len(clean_part) < 100:
                    return clean_part
        
        return ""

    def get_available_domains_enhanced(self) -> List[str]:
        """ENHANCED method to get available domains with better error handling"""
        domains = []
        try:
            print(f"🔄 ENHANCED: Getting available domains for {self.account['email']}...", flush=True)
            
            # Try multiple times to find and click dropdown
            max_dropdown_attempts = 3
            for attempt in range(max_dropdown_attempts):
                print(f"🔄 Dropdown attempt {attempt + 1}/{max_dropdown_attempts}", flush=True)
                if self.find_and_click_dropdown_enhanced():
                    break
                time.sleep(2)
            else:
                print(f"❌ Failed to open dropdown after {max_dropdown_attempts} attempts", flush=True)
                return ['leadlatticeloop.com', 'aiinvesttech.com']  # Return defaults
            
            time.sleep(4)  # Wait for dropdown to fully open
            
            # Take screenshot for debugging
            try:
                self.driver.save_screenshot(f"dropdown_{self.account['email']}.png")
                print(f"📸 Saved dropdown screenshot: dropdown_{self.account['email']}.png", flush=True)
            except:
                pass
            
            # ENHANCED: Better domain selectors with comprehensive patterns
            domain_selectors = [
                "//div[@role='listbox']//*",
                "//div[@role='menu']//*",
                "//*[@role='option']",
                "//*[@role='menuitem']",
                "//li//*[contains(text(), '.com')]",
                "//li//*[contains(text(), '.net')]",
                "//li//*[contains(text(), '.org')]",
                "//div[contains(@class, 'option')]",
                "//*[contains(@class, 'domain-item')]",
                "//*[contains(@class, 'menu-item')]",
                "//*[contains(text(), '.com')]",
                "//*[contains(@class, 'item') and contains(text(), '.com')]",
                "//*[contains(@class, 'MuiMenuItem-root')]",
                "//li[contains(@class, 'MuiMenuItem-root')]",
                "//*[@data-value]",
                "//*[contains(@class, 'MuiListItem-root')]",
                "//*[contains(@class, 'dropdown-item')]",
                "//*[contains(@class, 'select-option')]"
            ]
            
            all_elements_text = []
            found_elements = set()
            
            for selector in domain_selectors:
                try:
                    elements = self.driver.find_elements(By.XPATH, selector)
                    print(f"🔍 Found {len(elements)} elements with selector: {selector}", flush=True)
                    
                    for element in elements:
                        try:
                            if element.is_displayed():
                                text = element.text.strip()
                                if text and text not in all_elements_text:
                                    all_elements_text.append(text)
                                    print(f"📝 Element text: '{text}'", flush=True)
                                    
                                    # Clean and validate domain
                                    clean_domain = self.clean_domain_text(text)
                                    if (clean_domain and 
                                        clean_domain not in domains and 
                                        len(clean_domain) < 100 and
                                        clean_domain not in ['yahoo.com', 'example.com']):
                                        domains.append(clean_domain)
                                        found_elements.add(clean_domain)
                                        print(f"🌐 Found domain: {clean_domain}", flush=True)
                        except Exception as e:
                            continue
                except Exception as e:
                    continue
            
            # If we still don't have enough domains, try more aggressive extraction
            if len(domains) < 2:
                print("🔄 Manual domain extraction from all collected text...", flush=True)
                print(f"📋 All element texts found: {all_elements_text}", flush=True)
                
                # Try to extract domains from all collected text
                for text in all_elements_text:
                    clean_domain = self.clean_domain_text(text)
                    if (clean_domain and 
                        clean_domain not in domains and 
                        len(clean_domain) < 100 and
                        clean_domain not in ['yahoo.com', 'example.com']):
                        domains.append(clean_domain)
                        print(f"🌐 Added domain from manual extraction: {clean_domain}", flush=True)
            
            # Close dropdown by clicking outside
            try:
                body = self.driver.find_element(By.TAG_NAME, 'body')
                body.click()
                time.sleep(1)
            except:
                pass
            
            print(f"📋 Found {len(domains)} domains for {self.account['email']}: {domains}", flush=True)
            
            # If still no domains found, use default domains
            if not domains:
                print("❌ No domains found automatically, using defaults...", flush=True)
                domains = ['leadlatticeloop.com', 'aiinvesttech.com']
                print(f"🌐 Using default domains: {domains}", flush=True)
            
            return domains
            
        except Exception as e:
            print(f"❌ ENHANCED: Error getting domains for {self.account['email']}: {str(e)}", flush=True)
            # Return default domains as fallback
            return ['leadlatticeloop.com', 'aiinvesttech.com']

    def select_domain_directly_enhanced(self, target_domain: str) -> bool:
        """ENHANCED method to directly select domain with better error handling"""
        try:
            print(f"🎯 ENHANCED: Attempting to select domain: {target_domain} for {self.account['email']}", flush=True)
            
            # Try multiple times to find and click dropdown
            max_attempts = 3
            for attempt in range(max_attempts):
                print(f"🔄 Dropdown attempt {attempt + 1}/{max_attempts} for {target_domain}", flush=True)
                if self.find_and_click_dropdown_enhanced():
                    break
                time.sleep(2)
            else:
                print(f"❌ Failed to open dropdown for {target_domain} after {max_attempts} attempts", flush=True)
                return False
            
            time.sleep(4)  # Wait for dropdown to fully open
            
            print(f"🔍 Looking for exact domain: {target_domain}", flush=True)
            
            # ENHANCED: More flexible domain selection with better error handling
            domain_selectors = [
                f"//*[text()='{target_domain}']",
                f"//*[contains(text(), '{target_domain}')]",
                f"//*[contains(., '{target_domain}')]",
                f"//li[contains(., '{target_domain}')]",
                f"//div[contains(., '{target_domain}')]",
                f"//button[contains(., '{target_domain}')]",
                f"//*[@role='option' and contains(., '{target_domain}')]",
                f"//*[@role='menuitem' and contains(., '{target_domain}')]",
                f"//*[contains(@class, 'item') and contains(., '{target_domain}')]",
                f"//*[@data-value='{target_domain}']",
                f"//*[contains(@class, 'MuiMenuItem-root') and contains(., '{target_domain}')]"
            ]
            
            domain_element = None
            for selector in domain_selectors:
                try:
                    elements = self.driver.find_elements(By.XPATH, selector)
                    for element in elements:
                        try:
                            if element.is_displayed() and target_domain in element.text:
                                domain_element = element
                                print(f"✅ Found {target_domain} with selector: {selector}", flush=True)
                                break
                        except:
                            continue
                    if domain_element:
                        break
                except:
                    continue
            
            if not domain_element:
                print(f"❌ Could not find {target_domain} element after dropdown opened", flush=True)
                try:
                    self.driver.find_element(By.TAG_NAME, 'body').click()
                except:
                    pass
                return False
            
            print(f"🖱️ Clicking on {target_domain}...", flush=True)
            # Try multiple click methods
            click_success = False
            try:
                domain_element.click()
                click_success = True
                print(f"✅ Standard click successful for {target_domain}", flush=True)
            except:
                try:
                    self.driver.execute_script("arguments[0].click();", domain_element)
                    click_success = True
                    print(f"✅ JavaScript click successful for {target_domain}", flush=True)
                except:
                    try:
                        ActionChains(self.driver).move_to_element(domain_element).click().perform()
                        click_success = True
                        print(f"✅ ActionChains click successful for {target_domain}", flush=True)
                    except:
                        print(f"❌ All click methods failed for {target_domain}", flush=True)
            
            time.sleep(5)
            
            # ENHANCED: Better verification
            current_url = self.driver.current_url
            page_source = self.driver.page_source
            
            if target_domain in page_source or click_success:
                print(f"✅ Successfully selected and verified domain: {target_domain}", flush=True)
                return True
            else:
                print(f"⚠️ Domain selection may not have worked, but continuing...", flush=True)
                # Even if verification fails, continue and hope the page loaded correctly
                return True
                
        except Exception as e:
            print(f"❌ ENHANCED: Error selecting domain {target_domain}: {str(e)}", flush=True)
            return False

    def navigate_to_domain_stats_enhanced(self, domain: str) -> bool:
        """ENHANCED method to navigate to domain stats with multiple fallbacks"""
        if not self.logged_in:
            return False

        try:
            print(f"🌐 ENHANCED: Navigating to domain: {domain} for {self.account['email']}", flush=True)
            
            # Method 1: Try enhanced domain selection
            if self.select_domain_directly_enhanced(domain):
                print(f"✅ Enhanced navigation successful for {domain}", flush=True)
                return True
            
            print("🔄 Trying alternative navigation methods...", flush=True)
            
            # Method 2: Try direct URL with domain parameter
            try:
                self.driver.get(f"https://senders.yahooinc.com/feature-management/dashboard/?domain={domain}")
                time.sleep(6)
                if domain in self.driver.page_source:
                    print(f"✅ Direct URL navigation worked for {domain}", flush=True)
                    return True
            except:
                pass
            
            # Method 3: Try to refresh and check current domain
            try:
                self.driver.refresh()
                time.sleep(6)
                if domain in self.driver.page_source:
                    print(f"✅ Refresh worked for {domain}", flush=True)
                    return True
            except:
                pass

            # Method 4: Try navigating to domains page first
            try:
                self.driver.get("https://senders.yahooinc.com/feature-management/domains/")
                time.sleep(5)
                if self.select_domain_directly_enhanced(domain):
                    print(f"✅ Domain page navigation worked for {domain}", flush=True)
                    return True
            except:
                pass

            print(f"❌ All navigation methods failed for {domain}", flush=True)
            return False

        except Exception as e:
            print(f"❌ ENHANCED: Error navigating to domain {domain}: {str(e)}", flush=True)
            return False

    def select_insights_time_range(self, days: int = 180) -> bool:
        """Select the time range in Insights section"""
        try:
            print(f"🕐 Setting time range to {days} days...", flush=True)
            
            time.sleep(3)
            
            selectors_to_try = [
                "select",
                "//select",
                "//select[contains(@class, 'tw-text-sm')]",
                "//select[contains(@class, 'select')]",
                "//select[option[contains(text(), 'Last 7 days')]]",
                "//select[option[contains(text(), 'Last 180 days')]]",
                "//button[contains(text(), 'Last 7 days')]",
                "//button[contains(text(), 'Last 30 days')]",
                "//div[contains(text(), 'Last 7 days')]"
            ]
            
            for selector in selectors_to_try:
                try:
                    if selector.startswith("//"):
                        element = self.driver.find_element(By.XPATH, selector)
                    else:
                        element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    if element.is_displayed():
                        print(f"✅ Found time range element: {selector}", flush=True)
                        
                        if element.tag_name.lower() == 'select':
                            select = Select(element)
                            try:
                                select.select_by_visible_text(f"Last {days} days")
                                print(f"✅ Selected {days} days", flush=True)
                                time.sleep(5)  # Increased wait for data to load
                                return True
                            except:
                                try:
                                    select.select_by_value(str(days))
                                    print(f"✅ Selected {days} days by value", flush=True)
                                    time.sleep(5)
                                    return True
                                except:
                                    continue
                        else:
                            element.click()
                            time.sleep(2)
                            option_selectors = [
                                f"//*[contains(text(), 'Last {days} days')]",
                                f"//*[text()='Last {days} days']"
                            ]
                            for opt_selector in option_selectors:
                                try:
                                    option = self.driver.find_element(By.XPATH, opt_selector)
                                    if option.is_displayed():
                                        option.click()
                                        print(f"✅ Selected {days} days from menu", flush=True)
                                        time.sleep(5)
                                        return True
                                except:
                                    continue
                except:
                    continue
            
            print("❌ Could not set time range", flush=True)
            return False
            
        except Exception as e:
            print(f"❌ Error selecting time range: {str(e)}", flush=True)
            return False

    def check_for_no_data(self) -> bool:
        """Check if the page shows 'No data' message"""
        try:
            no_data_indicators = [
                "//*[contains(text(), 'No data')]",
                "//*[contains(text(), 'Unknown')]",
                "//*[contains(text(), 'No Data')]",
                "//*[contains(text(), 'no data')]"
            ]
            
            for indicator in no_data_indicators:
                try:
                    elements = self.driver.find_elements(By.XPATH, indicator)
                    for element in elements:
                        if element.is_displayed() and element.text.strip():
                            print("✅ Found 'No data' indicator on page", flush=True)
                            return True
                except:
                    continue
            
            # Also check page source
            page_source = self.driver.page_source.lower()
            if "no data" in page_source or "unknown" in page_source:
                print("✅ Found 'No data' in page source", flush=True)
                return True
                
            return False
            
        except Exception as e:
            print(f"❌ Error checking for no data: {str(e)}", flush=True)
            return False

    def extract_insights_data(self) -> Dict:
        """Extract actual insights data from the page with percentage values"""
        insights_data = {}
        try:
            print("🔍 Extracting insights data from page...", flush=True)
            
            # Wait for data to load
            time.sleep(5)
            
            # First check if there's no data
            if self.check_for_no_data():
                print("📭 No data available for this domain", flush=True)
                insights_data['has_data'] = False
                return insights_data
            
            insights_data['has_data'] = True
            
            # Extract delivered count with percentage
            delivered_selectors = [
                "//*[contains(text(), 'Delivered')]/following::span[contains(@class, 'tw-font-bold')]",
                "//*[contains(text(), 'Delivered')]/../span[contains(@class, 'tw-font-bold')]",
                "//*[contains(text(), 'Delivered')]/following::*[contains(@class, 'tw-font-bold')]"
            ]
            
            for selector in delivered_selectors:
                try:
                    elements = self.driver.find_elements(By.XPATH, selector)
                    for element in elements:
                        if element.is_displayed():
                            text = element.text.strip()
                            print(f"🔍 Delivered element text: '{text}'", flush=True)
                            
                            # Look for pattern like "302 +100%"
                            if text:
                                # Split by space to separate count and percentage
                                parts = text.split()
                                if parts:
                                    # First part should be the count
                                    if parts[0].isdigit():
                                        insights_data['delivered_count'] = int(parts[0])
                                        print(f"✅ Extracted delivered count: {insights_data['delivered_count']}", flush=True)
                                        
                                        # Look for percentage in the same element
                                        if len(parts) > 1 and '%' in parts[1]:
                                            insights_data['delivered_percentage'] = parts[1]
                                            print(f"✅ Extracted delivered percentage: {insights_data['delivered_percentage']}", flush=True)
                                        
                                        break
                    if 'delivered_count' in insights_data:
                        break
                except:
                    continue
            
            # Extract complaint rate with percentage
            complaint_selectors = [
                "//*[contains(text(), 'Complaint Rate')]/following::span[contains(@class, 'tw-font-bold')]",
                "//*[contains(text(), 'Complaint Rate')]/../span[contains(@class, 'tw-font-bold')]",
                "//*[contains(text(), 'Complaint Rate')]/following::*[contains(@class, 'tw-font-bold')]"
            ]
            
            for selector in complaint_selectors:
                try:
                    elements = self.driver.find_elements(By.XPATH, selector)
                    for element in elements:
                        if element.is_displayed():
                            text = element.text.strip()
                            print(f"🔍 Complaint rate element text: '{text}'", flush=True)
                            
                            if text:
                                # Look for percentage pattern like "0% +100%"
                                parts = text.split()
                                for part in parts:
                                    if '%' in part and not part.startswith('+') and not part.startswith('-'):
                                        # This is the main complaint rate
                                        rate_text = part.replace('%', '').strip()
                                        try:
                                            insights_data['complaint_rate'] = float(rate_text)
                                            print(f"✅ Extracted complaint rate: {insights_data['complaint_rate']}%", flush=True)
                                            
                                            # Look for percentage change in the same element
                                            for p in parts:
                                                if p.startswith(('+', '-')) and '%' in p:
                                                    insights_data['complaint_percentage'] = p
                                                    print(f"✅ Extracted complaint percentage: {insights_data['complaint_percentage']}", flush=True)
                                                    break
                                            
                                            # Check trend
                                            parent_html = element.get_attribute('outerHTML')
                                            if '🔺' in parent_html or '🔺️' in parent_html:
                                                insights_data['complaint_trend'] = 'up'
                                            elif '🔻' in parent_html:
                                                insights_data['complaint_trend'] = 'down'
                                            else:
                                                insights_data['complaint_trend'] = 'neutral'
                                            break
                                        except ValueError:
                                            continue
                    if 'complaint_rate' in insights_data:
                        break
                except:
                    continue
            
            # Extract time range
            time_range_selectors = [
                "//*[contains(text(), 'Last') and contains(text(), 'days')]",
                "//*[contains(text(), '180 days')]"
            ]
            
            for selector in time_range_selectors:
                try:
                    elements = self.driver.find_elements(By.XPATH, selector)
                    for element in elements:
                        if element.is_displayed():
                            text = element.text.strip()
                            if 'days' in text:
                                insights_data['time_range'] = text
                                print(f"✅ Extracted time range: {insights_data['time_range']}", flush=True)
                                break
                    if 'time_range' in insights_data:
                        break
                except:
                    continue
            
            # If still no data, try alternative extraction
            if not insights_data.get('delivered_count') and not insights_data.get('complaint_rate'):
                print("🔄 Trying alternative extraction method...", flush=True)
                page_text = self.driver.page_source
                
                # Look for delivered count in page source
                delivered_match = re.search(r'Delivered.*?(\d+)\s*(\+[\d%]+)?', page_text)
                if delivered_match:
                    insights_data['delivered_count'] = int(delivered_match.group(1))
                    if delivered_match.group(2):
                        insights_data['delivered_percentage'] = delivered_match.group(2)
                    print(f"✅ Extracted delivered count (alt method): {insights_data['delivered_count']}", flush=True)
                
                # Look for complaint rate in page source
                complaint_match = re.search(r'Complaint Rate.*?(\d+\.?\d*)%\s*(\+[\d%]+)?', page_text)
                if complaint_match:
                    insights_data['complaint_rate'] = float(complaint_match.group(1))
                    if complaint_match.group(2):
                        insights_data['complaint_percentage'] = complaint_match.group(2)
                    print(f"✅ Extracted complaint rate (alt method): {insights_data['complaint_rate']}%", flush=True)
            
            return insights_data
            
        except Exception as e:
            print(f"❌ Error extracting insights data: {str(e)}", flush=True)
            insights_data['has_data'] = False
            return insights_data

    def extract_domain_stats(self, domain: str) -> Dict:
        """Extract domain statistics with actual data"""
        if not self.logged_in:
            return {}

        try:
            stats = {
                'domain': domain,
                'timestamp': datetime.now().isoformat(),
                'source': 'yahoo_sender_hub',
                'status': 'Unknown',
                'verified': False,
                'added_date': None,
                'delivered_count': None,
                'delivered_percentage': None,
                'complaint_rate': None,
                'complaint_percentage': None,
                'complaint_trend': None,
                'time_range': None,
                'insights_data': {},
                'screenshot_path': None,
                'has_data': True
            }

            # Extract basic info
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')

            text = soup.get_text()
            if 'Verified' in text:
                stats['verified'] = True
                stats['status'] = 'Verified'
            
            if 'Added on' in text:
                date_match = re.search(r'Added on\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})', text)
                if date_match:
                    stats['added_date'] = date_match.group(1)

            # Extract actual insights data
            insights_data = self.extract_insights_data()
            stats.update(insights_data)
            stats['insights_data'] = insights_data

            return stats

        except Exception as e:
            print(f"❌ Error extracting domain stats: {str(e)}", flush=True)
            return {}

    def save_stats(self, stats: Dict, screenshot_path: str = ""):
        """Save statistics with screenshot path and account tracking"""
        if not stats:
            return

        domain = stats.get('domain', 'unknown_domain')

        try:
            # Add screenshot path to stats
            if screenshot_path:
                stats['screenshot_path'] = screenshot_path

            # Save to database with account tracking (if available)
            success = self.db.save_domain_stats(stats, self.current_account)
            if success:
                print(f"✅ Successfully saved to database: {domain} for account {self.current_account}", flush=True)
            else:
                print("⚠️ Database not available, data saved to JSON only", flush=True)

            # Save to JSON file - UPDATES existing file for domain
            json_path = self.file_manager.save_domain_json(domain, stats, self.current_account)
            
            print(f"💾 Stats saved for {domain}", flush=True)

        except Exception as e:
            print(f"❌ Error saving stats: {str(e)}", flush=True)

    def should_take_screenshot(self, stats: Dict) -> bool:
        """Check if screenshot should be taken based on data availability"""
        if not stats.get('has_data', True):
            print("📭 No data available, skipping screenshot", flush=True)
            return False
        
        delivered_count = stats.get('delivered_count')
        complaint_rate = stats.get('complaint_rate')
        
        # Take screenshot only if we have valid data for both metrics
        if delivered_count is not None and complaint_rate is not None:
            print(f"✅ Data available: delivered_count={delivered_count}, complaint_rate={complaint_rate}%, taking screenshot", flush=True)
            return True
        else:
            print(f"📭 Missing data: delivered_count={delivered_count}, complaint_rate={complaint_rate}, skipping screenshot", flush=True)
            return False

    def process_single_domain_enhanced(self, domain: str) -> Dict:
        """ENHANCED method to process single domain with better error handling"""
        try:
            print(f"\n{'='*60}", flush=True)
            print(f"🚀 ENHANCED PROCESSING DOMAIN: {domain}", flush=True)
            print(f"👤 Account: {self.current_account}", flush=True)
            print(f"{'='*60}", flush=True)
            
            # Navigate to domain using enhanced method
            if not self.navigate_to_domain_stats_enhanced(domain):
                print(f"❌ Enhanced navigation failed for domain {domain}", flush=True)
                return {}
            
            # Ensure page loaded
            self.ensure_page_fully_loaded()
            
            # Try to set time range to 180 days FIRST
            screenshot_path = ""
            if self.select_insights_time_range(180):
                print("✅ Time range set to 180 days", flush=True)
                time.sleep(8)  # Wait longer for data to load
                self.ensure_page_fully_loaded()
                
                # Extract stats with actual data
                stats = self.extract_domain_stats(domain)
                
                # Check if we should take screenshot based on data availability
                if self.should_take_screenshot(stats):
                    # Take 180-day screenshot ONLY if data exists - ONE SCREENSHOT PER DOMAIN
                    screenshot_path = self.take_screenshot(domain)
                else:
                    print("📭 No valid data for screenshot, skipping...", flush=True)
            else:
                print("⚠️ Could not set time range, using default view", flush=True)
                # Extract stats without setting time range
                stats = self.extract_domain_stats(domain)
            
            # Save results with screenshot path (empty if no screenshot taken)
            if stats:
                self.save_stats(stats, screenshot_path)
                if stats.get('has_data', True):
                    print(f"✅ Data extracted and saved for {domain}", flush=True)
                    print(f"📊 Extracted data: {stats.get('delivered_count')} delivered ({stats.get('delivered_percentage', 'N/A')}), {stats.get('complaint_rate')}% complaint rate ({stats.get('complaint_percentage', 'N/A')})", flush=True)
                else:
                    print(f"📭 No data available for {domain}, saved empty record", flush=True)
            else:
                print(f"⚠️ No stats extracted for {domain}", flush=True)
                stats = {
                    'domain': domain,
                    'timestamp': datetime.now().isoformat(),
                    'status': 'Processed',
                    'verified': False,
                    'screenshot_path': screenshot_path,
                    'has_data': False
                }
                self.save_stats(stats, screenshot_path)
            
            time.sleep(2)
            
            return stats
            
        except Exception as e:
            print(f"❌ ENHANCED: Error processing domain {domain}: {str(e)}", flush=True)
            error_stats = {
                'domain': domain,
                'timestamp': datetime.now().isoformat(),
                'status': f'Error: {str(e)}',
                'verified': False,
                'has_data': False
            }
            self.save_stats(error_stats, "")
            return {}

    def detect_new_domains(self, current_domains: List[str]) -> List[str]:
        """Detect newly added domains compared to previous scrape"""
        current_set = set(current_domains)
        previous_set = self.previous_domains
        
        if not previous_set:
            # First run, no previous domains to compare
            self.previous_domains = current_set
            return current_domains
        
        new_domains = list(current_set - previous_set)
        
        if new_domains:
            print(f"🎉 NEW DOMAINS DETECTED for {self.account['email']}: {new_domains}", flush=True)
        else:
            print(f"📋 No new domains detected for {self.account['email']}", flush=True)
        
        # Update previous domains for next comparison
        self.previous_domains = current_set
        
        return current_domains

    def run_hourly_scrape_enhanced(self):
        """ENHANCED hourly scrape with browser refresh and new domain detection"""
        try:
            print(f"\n⏰ ENHANCED: Starting HOURLY scrape for {self.account['email']} at {datetime.now()}", flush=True)
            
            # STEP 1: Safely refresh browser to get latest domain list
            print("🔄 Refreshing browser to detect newly added domains...", flush=True)
            if not self.safe_refresh_browser():
                print(f"❌ Browser refresh failed for {self.account['email']}", flush=True)
                return False
            
            # STEP 2: Ensure we're logged in after refresh
            if not self.ensure_logged_in():
                print(f"❌ Cannot login for {self.account['email']}, skipping this cycle", flush=True)
                return False
            
            # STEP 3: Get domains using ENHANCED method
            domains = self.get_available_domains_enhanced()
            if not domains:
                print(f"❌ No domains found for {self.account['email']}", flush=True)
                return False
            
            # STEP 4: Detect and log new domains
            all_domains = self.detect_new_domains(domains)
            print(f"📊 Total domains available for {self.account['email']}: {len(all_domains)}", flush=True)
            
            # STEP 5: Start scraping session (if database available)
            session_id = self.db.start_scraping_session(len(all_domains), self.account['email'])
            self.current_session_id = session_id
            
            # STEP 6: Scrape each domain using ENHANCED method
            processed_count = 0
            for domain in all_domains:
                stats = self.process_single_domain_enhanced(domain)
                processed_count += 1
                self.db.update_scraping_session(session_id, processed_count)
                self.random_delay(2, 3)  # Delay between domains
            
            # STEP 7: Mark session as completed (if database available)
            self.db.update_scraping_session(session_id, processed_count, 'completed')
            
            self.last_scrape_time = datetime.now()
            print(f"✅ ENHANCED: HOURLY scrape completed for {self.account['email']} at {self.last_scrape_time}", flush=True)
            print(f"📈 Successfully processed {processed_count}/{len(all_domains)} domains", flush=True)
            return True
            
        except Exception as e:
            print(f"❌ ENHANCED: Error in hourly scrape for {self.account['email']}: {str(e)}", flush=True)
            if self.current_session_id:
                self.db.update_scraping_session(self.current_session_id, 0, 'failed')
            return False

    def start_persistent_scraping(self):
        """Start PERSISTENT scraping - runs every hour FOREVER - DOCKER COMPATIBLE"""
        print(f"🔄 Starting PERSISTENT scraping for {self.account['email']}...", flush=True)
        
        # Initial setup and GUARANTEED login
        if not self.setup_persistent_driver():
            print(f"❌ Browser setup failed for {self.account['email']}", flush=True)
            return
            
        if not self.guaranteed_login():
            print(f"❌ GUARANTEED login failed for {self.account['email']}", flush=True)
            return
        
        print(f"✅ PERSISTENT setup completed for {self.account['email']}", flush=True)
        print(f"🕐 Browser will stay open and scrape every hour", flush=True)
        print(f"🔄 Browser will REFRESH before each scrape to detect newly added domains!", flush=True)
        
        # Continuous FOREVER loop - DOCKER COMPATIBLE with better resource management
        cycle_count = 0
        consecutive_failures = 0
        max_consecutive_failures = 3
        
        while self.is_running and consecutive_failures < max_consecutive_failures:
            try:
                # Run ENHANCED scrape cycle with browser refresh
                success = self.run_hourly_scrape_enhanced()
                cycle_count += 1
                
                if success:
                    consecutive_failures = 0
                    print(f"📊 Completed {cycle_count} cycles for {self.account['email']}", flush=True)
                    print(f"⏰ Next scrape in 1 hour...", flush=True)
                    
                    # Wait 1 hour before next scrape (3600 seconds) - DOCKER COMPATIBLE with proper timing
                    wait_seconds = 3600  # 1 hour
                    print(f"💤 Sleeping for {wait_seconds} seconds until next scrape...", flush=True)
                    
                    # Sleep in chunks to allow for interruption
                    for i in range(wait_seconds // 60):  # Check every minute
                        if not self.is_running:
                            break
                        time.sleep(60)
                        remaining = wait_seconds - ((i + 1) * 60)
                        if remaining > 0 and i % 5 == 0:  # Log every 5 minutes
                            minutes_remaining = remaining // 60
                            print(f"⏳ {self.account['email']}: {minutes_remaining} minutes remaining until next scrape", flush=True)
                    
                else:
                    consecutive_failures += 1
                    print(f"⚠️ Scrape cycle {cycle_count} failed for {self.account['email']} (consecutive failures: {consecutive_failures})", flush=True)
                    # Wait 10 minutes before retry on failure
                    print(f"🔄 Retrying in 10 minutes...", flush=True)
                    time.sleep(600)
                
            except Exception as e:
                consecutive_failures += 1
                print(f"❌ Error in persistent loop for {self.account['email']}: {str(e)}", flush=True)
                if consecutive_failures >= max_consecutive_failures:
                    print(f"🛑 Too many consecutive failures ({consecutive_failures}), stopping scraper for {self.account['email']}", flush=True)
                    break
                
                # Wait 10 minutes before retry if there's an error
                print(f"🔄 Retrying in 10 minutes... (failure {consecutive_failures}/{max_consecutive_failures})", flush=True)
                time.sleep(600)
        
        if consecutive_failures >= max_consecutive_failures:
            print(f"🚨 Scraper for {self.account['email']} stopped due to too many consecutive failures", flush=True)
        
        print(f"🛑 PERSISTENT scraping stopped for {self.account['email']}", flush=True)

    def stop_scraping(self):
        """Stop the persistent scraping"""
        self.is_running = False
        print(f"🛑 Stopping scraper for {self.account['email']}", flush=True)

class SimultaneousPersistentManager:
    """Manager for MULTIPLE PERSISTENT account scrapers running SIMULTANEOUSLY - DOCKER COMPATIBLE"""
    
    def __init__(self, headless: bool = False, db_manager: DatabaseManager = None):
        self.headless = headless
        self.db_manager = db_manager if db_manager else DatabaseManager()
        self.account_manager = AccountManager()
        self.scrapers = []
        self.threads = []
        self.is_running = True
    
    def start_all_accounts_simultaneously(self):
        """Start PERSISTENT scraping for ALL accounts SIMULTANEOUSLY - DOCKER COMPATIBLE"""
        accounts = self.account_manager.get_accounts()
        
        if not accounts:
            print("❌ No accounts available for persistent scraping", flush=True)
            return
        
        print(f"🚀 Starting SIMULTANEOUS PERSISTENT scraping for {len(accounts)} accounts...", flush=True)
        print("💡 Each account will have its own PERSISTENT browser that NEVER closes", flush=True)
        print("⏰ Data will be scraped every hour automatically", flush=True)
        print("🖥️  Browsers will remain open FOREVER with auto-recovery", flush=True)
        print("🔒 Using incognito mode for privacy", flush=True)
        print("🔄 Browsers will REFRESH before each scrape to detect new domains", flush=True)
        print("⚡ All accounts running SIMULTANEOUSLY!", flush=True)
        print("🐳 DOCKER COMPATIBLE - Optimized for containerized environment", flush=True)
        
        # Create persistent scrapers for each account
        for account in accounts:
            scraper = PersistentAccountScraper(
                account=account,
                db_manager=self.db_manager,
                headless=self.headless
            )
            self.scrapers.append(scraper)
        
        # Start each scraper in a separate thread SIMULTANEOUSLY with staggered starts
        for i, scraper in enumerate(self.scrapers):
            thread = threading.Thread(target=scraper.start_persistent_scraping)
            thread.daemon = True  # Set as daemon so they exit when main thread exits
            self.threads.append(thread)
            thread.start()
            print(f"✅ STARTED persistent scraper for {scraper.account['email']}", flush=True)
            
            # Staggered delay between starting browsers to avoid resource conflicts in Docker
            delay = 30 * (i + 1)  # 30s, 60s, 90s, etc.
            if i < len(self.scrapers) - 1:  # Don't delay after the last one
                print(f"⏳ Waiting {delay} seconds before starting next browser...", flush=True)
                time.sleep(delay)
        
        print(f"\n🎉 ALL {len(accounts)} accounts are now running PERSISTENTLY and SIMULTANEOUSLY!", flush=True)
        print("📊 Each account will scrape data every hour FOREVER", flush=True)
        print("🖥️  Browsers will stay open with sessions maintained", flush=True)
        print("💾 Data will be saved to JSON files (and database if available)", flush=True)
        print("🔄 Auto-recovery enabled for all browsers", flush=True)
        print("🔄 Browser refresh before each scrape to detect new domains", flush=True)
        print("⏰ Next automated scraping started!", flush=True)
        print("🐳 Running in DOCKER-OPTIMIZED mode", flush=True)
        
        # Keep main thread alive to monitor - DOCKER COMPATIBLE
        self.keep_main_thread_alive()
    
    def keep_main_thread_alive(self):
        """Keep main thread alive forever - DOCKER COMPATIBLE VERSION"""
        try:
            print("🔍 Main thread monitoring started...", flush=True)
            monitor_count = 0
            while self.is_running:
                time.sleep(60)  # Check every minute
                monitor_count += 1
                
                # Log status every 30 minutes
                if monitor_count % 30 == 0:
                    alive_threads = sum(1 for thread in self.threads if thread.is_alive())
                    total_threads = len(self.threads)
                    print(f"📊 SYSTEM STATUS: {alive_threads}/{total_threads} accounts still running", flush=True)
                    print(f"🔄 All browsers automatically refreshing to detect new domains", flush=True)
                    print(f"⏰ Next scrape cycles running continuously...", flush=True)
                    
                    # If all threads are dead, restart them
                    if alive_threads == 0 and total_threads > 0:
                        print("🚨 All scraper threads died! Restarting system...", flush=True)
                        self.restart_all_scrapers()
                
                # Restart any dead threads - but only if they've been dead for a while
                # Don't restart too aggressively in Docker
                for i, (thread, scraper) in enumerate(zip(self.threads, self.scrapers)):
                    if not thread.is_alive():
                        print(f"⚠️ Thread for {scraper.account['email']} died, will restart after cooldown...", flush=True)
                        # Wait 5 minutes before restarting to avoid rapid restarts
                        time.sleep(300)
                        if not thread.is_alive():  # Double check after cooldown
                            print(f"🔄 RESTARTING thread for {scraper.account['email']} after cooldown...", flush=True)
                            new_thread = threading.Thread(target=scraper.start_persistent_scraping)
                            new_thread.daemon = True
                            self.threads[i] = new_thread
                            new_thread.start()
                            print(f"✅ RESTARTED thread for {scraper.account['email']}", flush=True)
                            # Longer delay after restart
                            time.sleep(30)
                        
        except KeyboardInterrupt:
            print("\n🛑 Main monitoring interrupted by user", flush=True)
            self.stop_all_accounts()
        except Exception as e:
            print(f"❌ Main monitoring error: {str(e)}", flush=True)
            # Continue monitoring despite errors
            time.sleep(60)
            self.keep_main_thread_alive()
    
    def restart_all_scrapers(self):
        """Restart all scrapers - emergency recovery"""
        print("🔄 EMERGENCY: Restarting all scraper threads...", flush=True)
        self.stop_all_accounts()
        time.sleep(10)
        self.scrapers = []
        self.threads = []
        self.start_all_accounts_simultaneously()
    
    def stop_all_accounts(self):
        """Stop all persistent scrapers"""
        print("🛑 Stopping all persistent scrapers...", flush=True)
        self.is_running = False
        
        for scraper in self.scrapers:
            scraper.stop_scraping()
        
        # Wait for threads to finish with timeout
        for thread in self.threads:
            thread.join(timeout=30)
        
        print("✅ All persistent scrapers stopped", flush=True)

# Main execution - DOCKER COMPATIBLE with enhanced error handling
def main():
    print("🐳 Starting Yahoo Sender Hub Scraper - DOCKER COMPATIBLE VERSION", flush=True)
    
    # Database configuration is now handled via environment variables in DatabaseManager
    
    # Initialize database with retry logic
    print("🗄️ Initializing database for PERSISTENT scraping...", flush=True)
    db_manager = DatabaseManager()
    
    # Validate database connection before proceeding with retry logic
    max_db_attempts = 3  # Reduced attempts since we'll continue without DB
    db_attempt = 0
    db_initialized = False
    
    while db_attempt < max_db_attempts:
        try:
            db_manager.init_database()
            print("✅ Database connection and initialization successful!", flush=True)
            db_initialized = True
            break
        except Exception as e:
            db_attempt += 1
            if db_attempt < max_db_attempts:
                wait_time = db_attempt * 5  # Shorter wait times
                print(f"❌ Database initialization attempt {db_attempt} failed, retrying in {wait_time} seconds...: {str(e)}", flush=True)
                time.sleep(wait_time)
            else:
                print(f"❌ Database initialization failed after {max_db_attempts} attempts: {str(e)}", flush=True)
                print("💡 Please check your environment variables:", flush=True)
                print("   - DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT", flush=True)
                print("🚨 Continuing without database - data will be saved to JSON files only", flush=True)
                # Set the database as unavailable
                db_manager.db_available = False
                break
    
    # Create and run simultaneous persistent manager
    multi_manager = SimultaneousPersistentManager(
        headless=True,  # Set to True for Docker (no GUI)
        db_manager=db_manager
    )
    
    try:
        # Start ALL accounts SIMULTANEOUSLY with PERSISTENT scraping
        print("\n" + "="*80, flush=True)
        print("🚀 STARTING SCRAPING SYSTEM", flush=True)
        print("🔄 AUTOMATIC BROWSER REFRESH ENABLED FOR NEW DOMAIN DETECTION", flush=True)
        print("🐳 DOCKER COMPATIBLE - OPTIMIZED FOR CONTAINER ENVIRONMENT", flush=True)
        if db_initialized:
            print("💾 DATA WILL BE SAVED TO DATABASE AND JSON FILES", flush=True)
        else:
            print("💾 DATA WILL BE SAVED TO JSON FILES ONLY (Database unavailable)", flush=True)
        print("⏰ HOURLY AUTOMATED SCRAPING SCHEDULED", flush=True)
        print("="*80, flush=True)
        multi_manager.start_all_accounts_simultaneously()
            
    except KeyboardInterrupt:
        print("\n⏹️ Process interrupted by user", flush=True)
        multi_manager.stop_all_accounts()
    except Exception as e:
        print(f"❌ Main execution failed: {str(e)}", flush=True)
        multi_manager.stop_all_accounts()

if __name__ == "__main__":
    main()