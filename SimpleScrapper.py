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

# Configuration
SITE_BASE_URL = 'https://13f.info/'
DATABASE_NAME = '13f_database.db'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('scraper.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Database setup
def init_db():
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    
    c.execute('''CREATE TABLE IF NOT EXISTS managers
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  name TEXT,
                holding_url, TEXT,
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
                pct_change INTEGER,
                  FOREIGN KEY(filing_id) REFERENCES filing(id))''')
    conn.commit()
    conn.close()

def db_execute(query, params=(), commit=False, fetch=False):
    conn = sqlite3.connect(DATABASE_NAME)
    try:
        c = conn.cursor()
        c.execute(query, params)
        if commit:
            conn.commit()
        return c.fetchall() if fetch else None
    finally:
        conn.close()

def parse_quarter(date_str):
    try:
        year, q = date_str.split(' Q')
        quarter_month = (int(q) * 3 - 2)
        last_day = calendar.monthrange(int(year), quarter_month + 2)[1]
        return datetime(int(year), quarter_month + 2, last_day).date()
    except Exception as e:
        logger.error(f"Date parse error: {date_str} - {e}")
        raise

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
    """Scrape managers with improved URL handling"""
    for letter in 'abcdefghijklmnopqrstuvwxyz':
        try:
            url = urljoin(SITE_BASE_URL, f"managers/{letter}")
            logger.info(f"Scraping managers: {url}")
            response = fetch_url(url, delay)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find manager links in table rows
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
                    "INSERT OR IGNORE INTO managers (name,holding_url ,url) VALUES (?, ?,?)",
                    (link.text.strip(), manager_name,manager_url),
                    commit=True
                )
        except Exception as e:
            logger.error(f"Manager scrape error: {e}")

def scrape_filings(delay):
    """Scrape filings with improved table parsing"""
    managers = db_execute("SELECT id, holding_url ,url FROM managers", fetch=True)
    for manager_id,holding_url, manager_url in managers:
        try:
            logger.info(f"Scraping filings for manager {manager_id}")
            response = fetch_url(manager_url, delay)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find filings table
            table = soup.find('table')
            if not table:
                logger.warning(f"No table found for {manager_url}")
                continue
                
            # Process table rows
            for row in table.find_all('tr')[1:]:  # Skip header
                cols = row.find_all('td')
                if len(cols) < 5:
                    continue
                
                try:
                    # Extract critical fields
                    quarter_str = cols[0].text.strip()
                    holdings_count = int(cols[1].text.strip().replace(',', ''))
                    value = int(cols[2].text.strip().replace(',', ''))
                    top_holdings = cols[3].text.strip()
                    form_type = cols[4].text.strip()
                    date_filled = cols[5].text.strip()
                    filing_id = cols[6].text.strip()
                    if form_type != '13F-HR':
                        continue
                    
                    formatted_str = quarter_str.lower().replace(" ", "-")
                    filing_url = urljoin(SITE_BASE_URL, f"13f/{filing_id}-{holding_url}-{formatted_str}")
                    

                    
                    try:
                        db_execute('''INSERT OR IGNORE INTO filing
                        (manager_id, value,quarter_date, holdings_count,form_type, date_filled, filing_id, filing_url)
                        VALUES (?, ?, ?,?, ?, ?, ?, ?)''',
                        (manager_id,
                        value,
                        quarter_str,
                        holdings_count,  # holdings_count (set to NULL)
                        form_type,
                        date_filled,
                        filing_id,
                        filing_url),commit=True)
                        

                    except Exception as e:
                        print(f"Error inserting data: {e}")
                except Exception as e:
                    logger.error(f"Filing insert error: {e}")
        except Exception as e:
            logger.error(f"Filing scrape error: {e}")

import re

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
    """Scrape holdings from dynamic API endpoint, filtering filings after Q1 2024."""
    
    # Fetch filings with quarter_date
    filings = db_execute(
        "SELECT id, filing_url, quarter_date FROM filing WHERE form_type = '13F-HR'",
        fetch=True
    )

    # Define the minimum quarter date for filtering (Q1 2024)
    min_date = (2014, 1)

    COLUMN_MAP = [
        'sym',          # Index 0
        'issuer_name',  # Index 1
        'cl',           # Index 2
        'cusip',        # Index 3
        'value',        # Index 4
        'percent',      # Index 5
        'shares',       # Index 6
        'principal',    # Index 7 (usually null)
        'option_type'   # Index 8
    ]

    for filing_id, filing_url, quarter_date in filings:
        parsed_date = parse_quarter_date(quarter_date)

        # Skip filings before or equal to Q1 2024
        if parsed_date and parsed_date <= min_date:
            continue

        try:
            logger.info(f"Scraping holdings for filing {filing_id} (Date: {quarter_date})")
            response = fetch_url(filing_url, delay)
            soup = BeautifulSoup(response.text, 'html.parser')

            # Find the table with API data reference
            table = soup.find('table', {'id': 'filingAggregated'})
            if not table:
                logger.warning(f"No holdings table found in {filing_url}")
                continue

            data_url = table.get('data-url')
            if not data_url:
                logger.warning("No data-url attribute found in table")
                continue

            # Build full API URL
            api_url = urljoin(SITE_BASE_URL, data_url)
            logger.info(f"Fetching API data from {api_url}")

            # Fetch JSON data
            api_response = fetch_url(api_url, delay)
            response_data = api_response.json()
            holdings_data = response_data.get('data', [])

            # Process JSON entries
            for entry in holdings_data:
                try:
                    # Skip non-COM entries
                    if not isinstance(entry, list) or len(entry) < 9:
                        continue
                    entry = [item if item != "null" else None for item in entry]
                    cl_value = str(entry[2]) if entry[2] else ''
                    if 'COM' not in cl_value:
                        continue
                    try:
                        value = int(entry[4]) if entry[4] else 0
                        shares = int(entry[6]) if entry[6] else 0
                    except (ValueError, TypeError) as e:
                        logger.error(f"Number conversion error: {e}")
                        continue
                    db_execute(
                        '''INSERT OR IGNORE INTO holdings
                        (filing_id, sym, issuer_name, cl, cusip, shares, value)
                        VALUES (?, ?, ?, ?, ?, ?, ?)''',
                        (filing_id,
                         entry[0],
                         entry[1],  # Issuer Name
                         cl_value,
                         entry[3],
                         shares,  # Shares
                         value),  # Value
                         commit=True
                    )
                    logger.info(f"Inserted holding: {entry[3]}")

                except Exception as e:
                    logger.error(f"Error processing entry: {e}")

        except Exception as e:
            logger.error(f"Holdings scrape error: {e}")



def get_previous_quarter(current_quarter: str) -> str:
    quarter, year = current_quarter.split()
    year = int(year)
    quarter_number = int(quarter[1])  # Extract the numeric part of the quarter

    if quarter_number == 1:
        previous_quarter = 4
        year -= 1
    else:
        previous_quarter = quarter_number - 1

    return f"Q{previous_quarter} {year}"
def infer_transaction_types():
    """Infer transaction types comparing consecutive quarters only"""
    holdings = db_execute(
        """SELECT h.id, h.filing_id, h.cusip, h.issuer_name
        FROM holdings h
        WHERE h.cl LIKE '%COM%'""",
        fetch=True
    )
    
    for holding_id, filing_id, cusip,issuer_name in holdings:
        try:
            # Get current filing info
            filing = db_execute(
                """SELECT f.manager_id, f.quarter_date 
                FROM filing f
                WHERE f.id = ?""",
                (filing_id,),
                fetch=True
            )[0]
            
            manager_id, current_date = filing
            
            # Find immediate previous quarter (same year if possible)
            prev_filing = db_execute(
                """SELECT id FROM filing
                WHERE manager_id = ? 
                AND quarter_date = (
                    SELECT MAX(quarter_date) 
                    FROM filing
                    WHERE manager_id = ? 
                    AND quarter_date = ?
                )""",
                (manager_id, manager_id, get_previous_quarter(current_date)),
                fetch=True
            )
            
            shares_prev = 0
            if prev_filing:
                prev_shares = db_execute(
                    """SELECT shares FROM holdings 
                    WHERE filing_id = ? 
                    AND issuer_name = ? 
                    AND cl LIKE '%COM%'""",
                    (prev_filing[0][0], issuer_name),
                    fetch=True
                )
                shares_prev = prev_shares[0][0] if prev_shares else 0
            logger.info(f"{manager_id} {current_date}")   
            current_shares = db_execute(
                "SELECT shares FROM holdings WHERE id = ?",
                (holding_id,),
                fetch=True
            )[0][0]
            logger.info(f"Current shares: {current_shares}, Previous shares: {shares_prev}")
            # Transaction logic
            if current_shares > shares_prev:
                transaction_type = 'Buy'
            elif current_shares < shares_prev:
                transaction_type = 'Sell'
            else:
                transaction_type = 'Other'
            change = current_shares - shares_prev
            pct_change =  ((current_shares - shares_prev) / shares_prev) * 100
            # Update database
            db_execute(
                """UPDATE holdings
                SET transaction_type = ?,change = ?, pct_change = ?
                WHERE id = ?""",
                (transaction_type,change,pct_change,holding_id),
                commit=True
            )
        except Exception as e:
            logger.error(f"Transaction inference error: {e}")

def export_to_csv(output_path):
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        query = '''
            SELECT h.issuer_name AS fund_name,
                   f.date_filled,
                   f.quarter_date,
                   h.sym,
                   h.cl,
                   h.value,
                   h.shares,
                   h.change,
                   h.pct_change,
                   h.transaction_type
            FROM holdings h
            JOIN filing f ON h.filing_id = f.id
            JOIN managers m ON f.manager_id = m.id
        '''
        df = pd.read_sql_query(query, conn)
        df.to_csv(output_path, index=False)
        logger.info(f"Exported {len(df)} records to {output_path}")
    except Exception as e:
        logger.error(f"Export error: {e}")
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description='13F Scraper')
    parser.add_argument('--output', default='transactions.csv', help='Output CSV path')
    parser.add_argument('--delay', type=float, default=2.0, help='Request delay')
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
        logger.info("Exporting CSV...")
        export_to_csv("transactions.csv")
        logger.info("Process completed successfully")
    except Exception as e:
        logger.error(f"Fatal error: {e}")

if __name__ == "__main__":
    main()