import pandas as pd
import requests
from bs4 import BeautifulSoup
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter
import json
from requests.packages.urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Step 1: Read the CSV file
file_path = 'KurumListe.csv'  # Make sure to save the .xls file as .csv
try:
    df = pd.read_csv(file_path, header=1)  # Use the second row as header
    logging.info('Successfully read the CSV file.')
except Exception as e:
    logging.error(f'Error reading the CSV file: {e}')
    exit()

# Ensure the column WEB_ADRES exists and matches exactly
if 'WEB_ADRES' not in df.columns:
    logging.error('The column WEB_ADRES does not exist in the CSV file.')
    exit()

# Extract URLs that end with '.meb.k12.tr'
urls = df['WEB_ADRES'].dropna().tolist()
urls = [url for url in urls if url.endswith('.meb.k12.tr')]
logging.info(f'Extracted {len(urls)} valid .meb.k12.tr URLs from the CSV file.')

# Function to create a session with retry logic
def create_session():
    session = requests.Session()
    retry = Retry(
        total=3,  # Total number of retries
        backoff_factor=0.3,  # Wait time between retries
        status_forcelist=[500, 502, 503, 504]  # Retry on these HTTP status codes
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

# Function to scrape a page for data
def scrape_page_for_data(url, session):
    try:
        response = session.get(f'http://{url}', timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Check if the site is down
        if "adresi sunucularımız üzerinde barındırılmamaktadır" in soup.text:
            logging.info(f'The site {url} is down.')
            return {"WEB_ADRES": url, "Öğrenci Sayısı": None, "Öğretim Şekli": None}
        
        öğrenci = None
        öğretim_şekli = None

        # Check specific HTML tags for "Öğrenci Sayısı"
        specific_tags = soup.find_all(class_="okulumuz-sayi")
        for tag in specific_tags:
            try:
                number = int(tag.text.strip())
                öğrenci = number
                logging.info(f'Found öğrenci sayısı: {öğrenci} on landing page {url}')
            except ValueError:
                continue

        # Check table structure for "Öğrenci Sayısı"
        rows = soup.find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) == 3 and ("öğrenci sayısı" in cells[0].text.lower() or "öğrenci" in cells[0].text.lower()):
                try:
                    öğrenci = int(cells[2].text.strip())
                    logging.info(f'Found öğrenci sayısı: {öğrenci} in table on landing page {url}')
                except ValueError:
                    continue

        # Check specific HTML structure for "Öğretim Şekli"
        specific_divs = soup.find_all("div", class_="col-sm-4")
        for div in specific_divs:
            if "öğretim şekli" in div.text.lower():
                öğretim_şekli = div.text.split(":")[-1].strip()
                logging.info(f'Found öğretim şekli: {öğretim_şekli} on landing page {url}')

        return {"WEB_ADRES": url, "Öğrenci Sayısı": öğrenci, "Öğretim Şekli": öğretim_şekli}
    except requests.RequestException as e:
        logging.error(f'Error scraping data from {url}: {e}')
        return {"WEB_ADRES": url, "Öğrenci Sayısı": None, "Öğretim Şekli": None}

# Function to gather relevant links from a site
def gather_relevant_links(base_url, session):
    urls_to_visit = [f'http://{base_url}']
    visited_urls = set()
    relevant_links = set()
    keywords = ["okulumuz_hakkinda", "hakkimizda", "hakkinda", "istatistikler"]

    while urls_to_visit:
        current_url = urls_to_visit.pop(0)
        if current_url in visited_urls:
            continue
        visited_urls.add(current_url)

        try:
            response = session.get(current_url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            links = soup.find_all('a', href=True)
            for link in links:
                href = link['href']
                full_url = urljoin(f'http://{base_url}', href)
                if any(keyword in full_url.lower() for keyword in keywords):
                    relevant_links.add(full_url)

        except requests.RequestException as e:
            logging.error(f'Error gathering links from {current_url}: {e}')

    return base_url, list(relevant_links)

# Function to scrape data from relevant links
def scrape_from_links(base_url, links, session):
    öğrenci = None
    öğretim_şekli = None

    for link in links:
        if öğretim_şekli is not None:
            break
        try:
            response = session.get(link, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Check specific HTML structure for "Öğretim Şekli"
            specific_divs = soup.find_all("div", class_="col-sm-4")
            for div in specific_divs:
                if "öğretim şekli" in div.text.lower():
                    öğretim_şekli = div.text.split(":")[-1].strip()
                    logging.info(f'Found öğretim şekli: {öğretim_şekli} on page {link}')
                    break

        except requests.RequestException as e:
            logging.error(f'Error scraping data from {link}: {e}')

    return {"WEB_ADRES": base_url, "Öğrenci Sayısı": öğrenci, "Öğretim Şekli": öğretim_şekli}

# Step 2: Scrape landing pages
landing_page_data = []
session = create_session()
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(scrape_page_for_data, url, session): url for url in urls}
    for future in as_completed(futures):
        data = future.result()
        if data and (data["Öğrenci Sayısı"] is not None or data["Öğretim Şekli"] is not None):
            landing_page_data.append(data)
            df.loc[df['WEB_ADRES'] == data['WEB_ADRES'], ['Öğrenci Sayısı', 'Öğretim Şekli']] = data['Öğrenci Sayısı'], data['Öğretim Şekli']
            df.to_csv('KurumListe_updated.csv', index=False)
            logging.info(f'Saved data to KurumListe_updated.csv')
        # Save intermediate results to avoid data loss
        if len(landing_page_data) % 50 == 0:
            with open('landing_page_data_backup.txt', 'w') as file:
                file.write(json.dumps(landing_page_data, ensure_ascii=False, indent=4))
            logging.info(f'Saved backup data for landing pages to landing_page_data_backup.txt after {len(landing_page_data)} records.')

# Step 3: Gather links from URLs that still need scraping
urls_to_fully_scrape = [data['WEB_ADRES'] for data in landing_page_data if data['Öğrenci Sayısı'] is None or data['Öğretim Şekli'] is None]
all_links = {}
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(gather_relevant_links, url, session): url for url in urls_to_fully_scrape}
    for future in as_completed(futures):
        url, links = future.result()
        if links:
            all_links[url] = links

# Step 4: Scrape data from relevant links
scraped_data = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(scrape_from_links, url, links, session): url for url, links in all_links.items()}
    for future in as_completed(futures):
        data = future.result()
        url = data['WEB_ADRES']
        if data and (data["Öğrenci Sayısı"] is not None or data["Öğretim Şekli"] is not None):
            scraped_data.append(data)
            df.loc[df['WEB_ADRES']  == url, ['Öğrenci Sayısı', 'Öğretim Şekli']] = data['Öğrenci Sayısı'], data['Öğretim Şekli']
            df.to_csv('KurumListe_updated.csv', index=False)
            logging.info(f'Saved data to KurumListe_updated.csv')

        # Save intermediate results to avoid data loss
        if len(scraped_data) % 50 == 0:
            temp_df = pd.DataFrame(scraped_data)
            df_temp = df.merge(temp_df, how='left', left_on='WEB_ADRES', right_on='WEB_ADRES')
            df_temp.to_csv('KurumListe_temp.csv', index=False)
            logging.info(f'Saved intermediate results to KurumListe_temp.csv after {len(scraped_data)} records.')

            # Save backup data to a text file
            with open('scraped_data_backup.txt', 'w') as file:
                file.write(json.dumps(scraped_data, ensure_ascii=False, indent=4))
            logging.info(f'Saved backup data to scraped_data_backup.txt')

logging.info(f'Successfully scraped data from {len(scraped_data)} URLs.')

# Save final results
df.to_csv('KurumListe_final.csv', index=False)
logging.info('Data scraping complete. Final results saved to KurumListe_final.csv')

