import argparse
import logging
import sqlite3
import time
import calendar
from datetime import datetime
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import pandas as pd
from retrying import retry
import concurrent.futures
from threading import Lock, local

# Configuration
SITE_BASE_URL = 'https://13f.info/'
DATABASE_NAME = '13f_database.db'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('scraper.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Thread-local storage and database setup
thread_local = local()

class RateLimiter:
    def __init__(self, delay):
        self.delay = delay
        self.last_request = 0
        self.lock = Lock()

    def wait(self):
        with self.lock:
            elapsed = time.time() - self.last_request
            if elapsed < self.delay:
                time.sleep(self.delay - elapsed)
            self.last_request = time.time()

def init_db():
    conn = sqlite3.connect(DATABASE_NAME, check_same_thread=False)
    conn.execute('PRAGMA journal_mode=WAL;')
    c = conn.cursor()
    
    c.execute('''CREATE TABLE IF NOT EXISTS managers
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  name TEXT,
                  holding_url TEXT,
                  url TEXT UNIQUE)''')
                 
    c.execute('''CREATE TABLE IF NOT EXISTS filing
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  manager_id INTEGER,
                  value INTEGER,
                  quarter_date TEXT,
                  holdings_count INTEGER,
                  form_type TEXT,
                  date_filled TEXT,
                  filing_id TEXT UNIQUE,
                  filing_url TEXT UNIQUE,
                  FOREIGN KEY(manager_id) REFERENCES managers(id))''')
                  
    c.execute('''CREATE TABLE IF NOT EXISTS holdings
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  sym TEXT,
                  filing_id INTEGER,
                  issuer_name TEXT,
                  cl TEXT,
                  cusip TEXT,
                  shares INTEGER,
                  value INTEGER,
                  transaction_type TEXT,
                  change INTEGER,
                  pct_change REAL,
                  FOREIGN KEY(filing_id) REFERENCES filing(id))''')
    conn.commit()
    conn.close()

def get_db_connection():
    if not hasattr(thread_local, 'conn'):
        thread_local.conn = sqlite3.connect(DATABASE_NAME, check_same_thread=False)
        thread_local.conn.execute('PRAGMA journal_mode=WAL;')
    return thread_local.conn

def db_execute(query, params=(), commit=False, fetch=False, retries=3):
    conn = get_db_connection()
    for attempt in range(retries):
        try:
            c = conn.cursor()
            c.execute(query, params)
            if commit:
                conn.commit()
            return c.fetchall() if fetch else None
        except sqlite3.OperationalError as e:
            if 'locked' in str(e) and attempt < retries - 1:
                time.sleep(0.1 * (attempt + 1))
                continue
            raise
        finally:
            if commit:
                conn.close()
                del thread_local.conn

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def fetch_url(url, delay=1):
    time.sleep(delay)
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response
    except Exception as e:
        logger.error(f"Request failed: {url} - {e}")
        raise

def scrape_managers(delay):
    rate_limiter = RateLimiter(delay)
    
    def process_letter(letter):
        try:
            rate_limiter.wait()
            url = urljoin(SITE_BASE_URL, f"managers/{letter}")
            logger.info(f"Scraping managers: {url}")
            response = fetch_url(url, 0)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            for row in soup.select('table tr'):
                cols = row.find_all('td')
                if len(cols) < 2:
                    continue
                
                link = cols[0].find('a')
                if not link:
                    continue
                
                manager_url = urljoin(SITE_BASE_URL, link['href'])
                manager_name = link["href"].split("/")[-1].split("-", 1)[1]
                
                db_execute(
                    "INSERT OR IGNORE INTO managers (name, holding_url, url) VALUES (?, ?, ?)",
                    (link.text.strip(), manager_name, manager_url),
                    commit=True
                )
        except Exception as e:
            logger.error(f"Manager scrape error: {e}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(process_letter, 'abcdefghijklmnopqrstuvwxyz')

def scrape_filings(delay):
    rate_limiter = RateLimiter(delay)
    managers = db_execute("SELECT id, holding_url, url FROM managers", fetch=True)

    def process_manager(manager):
        manager_id, holding_url, manager_url = manager
        try:
            rate_limiter.wait()
            logger.info(f"Scraping filings for manager {manager_id}")
            response = fetch_url(manager_url, 0)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            table = soup.find('table')
            if not table:
                return
                
            for row in table.find_all('tr')[1:]:
                cols = row.find_all('td')
                if len(cols) < 5:
                    continue
                
                try:
                    quarter_str = cols[0].text.strip()
                    holdings_count = int(cols[1].text.strip().replace(',', ''))
                    value = int(cols[2].text.strip().replace(',', ''))
                    form_type = cols[4].text.strip()
                    date_filled = cols[5].text.strip()
                    filing_id = cols[6].text.strip()
                    
                    if form_type != '13F-HR':
                        continue
                    
                    formatted_str = quarter_str.lower().replace(" ", "-")
                    filing_url = urljoin(SITE_BASE_URL, f"13f/{filing_id}-{holding_url}-{formatted_str}")
                    
                    db_execute('''INSERT OR IGNORE INTO filing
                        (manager_id, value, quarter_date, holdings_count, form_type, date_filled, filing_id, filing_url)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                        (manager_id, value, quarter_str, holdings_count, form_type, date_filled, filing_id, filing_url),
                        commit=True
                    )
                except Exception as e:
                    logger.error(f"Filing insert error: {e}")
        except Exception as e:
            logger.error(f"Filing scrape error: {e}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(process_manager, managers)
import re
from urllib.parse import urljoin
from bs4 import BeautifulSoup

def parse_quarter_date(quarter_date):
    """Convert quarter date string (e.g., 'Q1 2024') to a tuple (year, quarter) for comparison."""
    match = re.match(r'Q([1-4]) (\d{4})', quarter_date)
    if match:
        quarter, year = int(match.group(1)), int(match.group(2))
        return (year, quarter)
    return None
def scrape_holdings(delay):
    rate_limiter = RateLimiter(delay)
    filings = db_execute(
        "SELECT id, filing_url, quarter_date FROM filing WHERE form_type = '13F-HR'",
        fetch=True
    )
    min_date = (2014, 1)
    def process_filing(filing):
        filing_id, filing_url , quarter_date= filing
        parsed_date = parse_quarter_date(quarter_date)

        # Skip filings before or equal to Q1 2024
        
        try:
            rate_limiter.wait()
            logger.info(f"Scraping holdings for filing {filing_id}")
            response = fetch_url(filing_url, 0)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            table = soup.find('table', {'id': 'filingAggregated'})
            if not table:
                return
            
            data_url = table.get('data-url')
            if not data_url:
                return
                
            api_url = urljoin(SITE_BASE_URL, data_url)
            api_response = fetch_url(api_url, 0)
            response_data = api_response.json()
            
            for entry in response_data.get('data', []):
                try:
                    if not isinstance(entry, list) or len(entry) < 9:
                        continue
                    if parsed_date and parsed_date <= min_date:
                        continue
                    
                    entry = [item if item != "null" else None for item in entry]
                    if 'COM' not in str(entry[2]):
                        continue
                    
                    value = int(entry[4]) if entry[4] else 0
                    shares = int(entry[6]) if entry[6] else 0
                    
                    db_execute(
                        '''INSERT OR IGNORE INTO holdings
                        (filing_id, sym, issuer_name, cl, cusip, shares, value)
                        VALUES (?, ?, ?, ?, ?, ?, ?)''',
                        (filing_id, entry[0], entry[1], entry[2], entry[3], shares, value),
                        commit=True
                    )
                except Exception as e:
                    logger.error(f"Entry processing error: {e}")
        except Exception as e:
            logger.error(f"Holdings scrape error: {e}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        executor.map(process_filing, filings)

def get_previous_quarter(current_quarter: str) -> str:
    quarter, year = current_quarter.split()
    year = int(year)
    quarter_number = int(quarter[1])
    return f"Q{4 if quarter_number == 1 else quarter_number - 1} {year - 1 if quarter_number == 1 else year}"

def infer_transaction_types():
    holdings = db_execute(
        """SELECT h.id, h.filing_id, h.issuer_name 
        FROM holdings h WHERE h.cl LIKE '%COM%'""",
        fetch=True
    )

    def process_holding(holding):
        holding_id, filing_id, issuer_name = holding
        try:
            filing = db_execute(
                """SELECT f.manager_id, f.quarter_date 
                FROM filing f WHERE f.id = ?""",
                (filing_id,),
                fetch=True
            )[0]
            
            manager_id, current_date = filing
            prev_filing = db_execute(
                """SELECT id FROM filing
                WHERE manager_id = ? AND quarter_date = ?""",
                (manager_id, get_previous_quarter(current_date)),
                fetch=True
            )
            
            shares_prev = 0
            if prev_filing:
                prev_shares = db_execute(
                    """SELECT shares FROM holdings 
                    WHERE filing_id = ? AND issuer_name = ?""",
                    (prev_filing[0][0], issuer_name),
                    fetch=True
                )
                shares_prev = prev_shares[0][0] if prev_shares else 0
                
            current_shares = db_execute(
                "SELECT shares FROM holdings WHERE id = ?",
                (holding_id,),
                fetch=True
            )[0][0]
            
            change = current_shares - shares_prev
            pct_change = (change / shares_prev * 100) if shares_prev != 0 else 0
            
            transaction_type = 'Buy' if change > 0 else 'Sell' if change < 0 else 'Other'
            
            db_execute(
                """UPDATE holdings
                SET transaction_type = ?, change = ?, pct_change = ?
                WHERE id = ?""",
                (transaction_type, change, pct_change, holding_id),
                commit=True
            )
        except Exception as e:
            logger.error(f"Transaction inference error: {e}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        executor.map(process_holding, holdings)

def export_to_csv(output_path):
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        df = pd.read_sql('''
            SELECT h.issuer_name, f.date_filled, f.quarter_date, h.sym, 
                   h.value, h.shares, h.change, h.pct_change, h.transaction_type
            FROM holdings h
            JOIN filing f ON h.filing_id = f.id
            JOIN managers m ON f.manager_id = m.id
        ''', conn)
        df.to_csv(output_path, index=False)
        logger.info(f"Exported {len(df)} records to {output_path}")
    except Exception as e:
        logger.error(f"Export error: {e}")
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description='13F Scraper')
    parser.add_argument('--output', default='transactions.csv', help='Output CSV path')
    parser.add_argument('--delay', type=float, default=2.0, help='Request delay in seconds')
    args = parser.parse_args()

    try:
        #init_db()
        #logger.info("Scraping managers...")
        #scrape_managers(args.delay)
        #logger.info("Scraping filings...")
        #scrape_filings(args.delay)
        logger.info("Scraping holdings...")
        scrape_holdings(args.delay)
        logger.info("Inferring transactions...")
        infer_transaction_types()
        logger.info("Exporting data...")
        export_to_csv(args.output)
        logger.info("Process completed successfully")
    finally:
        if hasattr(thread_local, 'conn'):
            thread_local.conn.close()

if __name__ == "__main__":
    main()