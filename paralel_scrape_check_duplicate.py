# nama_file: paralel_scrape.py

import concurrent.futures
import time
import os
import logging
import psycopg2

# Impor kelas Scraper dan daftar URL dari file scrape.py
from scrape import TrstdlyScraper, urls

# Konfigurasi logging dasar untuk file paralel
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- VARIABEL KONFIGURASI ---
MAX_WORKERS = 15

def get_existing_urls(db_config: dict) -> set:
    """
    Menghubungi database sekali untuk mengambil semua URL yang sudah disimpan.
    Menggunakan TRIM() untuk membersihkan spasi yang mungkin ada di data.
    """
    existing_urls = set()
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        with conn.cursor() as cur:
            # Menggunakan nama tabel yang benar: 'scraped_articles'
            cur.execute("SELECT TRIM(url) FROM scraped_articles;")
            
            results = cur.fetchall()
            existing_urls = {row[0] for row in results if row[0]} # Ubah ke set
            logger.info(f"Found {len(existing_urls)} existing URLs in the database.")
            
    except psycopg2.Error as e:
        logger.error(f"Could not fetch existing URLs from database: {e}")
        logger.error("Pastikan detail koneksi dan nama tabel 'scraped_articles' sudah benar.")
    finally:
        if conn:
            conn.close()
    
    return existing_urls

def process_single_url(url: str) -> tuple[str, str]:
    """
    Fungsi pekerja untuk setiap thread.
    Fungsi ini hanya fokus pada scraping karena URL sudah di-filter sebelumnya.
    """
    logger.info(f"Processing NEW URL: {url}")
    scraper = TrstdlyScraper()
    try:
        scrape_result = scraper.scrape_article(url)

        if scrape_result:
            article_data, full_html, content_for_db = scrape_result
            scraper._save_html_to_file(url, full_html)
            db_status = scraper._save_to_db(article_data, content_for_db)
            return url, db_status
        else:
            return url, "Failed to scrape content"

    except Exception as e:
        logger.critical(f"An unexpected error occurred in worker for {url}: {e}", exc_info=True)
        return url, f"Critical Error: {e}"

def main():
    """
    Fungsi utama untuk mengatur dan menjalankan proses scraping paralel.
    """
    # Atur password di sini. Ini akan digunakan oleh Scraper dan get_existing_urls.
    db_password = 'admin' # <<< GANTI DENGAN PASSWORD DATABASE ANDA
    os.environ['DB_PASSWORD'] = db_password

    # Konfigurasi koneksi untuk fungsi get_existing_urls
    db_config = {
        'dbname': 'scraper_db',
        'user': 'postgres',
        'password': db_password,
        'host': 'localhost'
    }

    # 1. Inisialisasi Database
    print("=== Step 1: Initializing Database (if necessary) ===")
    init_scraper = TrstdlyScraper()
    init_scraper.init_db()
    print("=" * 50)

    # 2. Ambil URL yang sudah ada dari DB (Metode Efisien)
    print("\n=== Step 2: Fetching existing URLs from Database ===")
    existing_urls_set = get_existing_urls(db_config)
    print("=" * 50)
    
    # 3. Saring Daftar URL
    all_urls = urls
    urls_to_scrape = [url for url in all_urls if url not in existing_urls_set]
    skipped_count = len(all_urls) - len(urls_to_scrape)

    if not urls_to_scrape:
        print("\nAll URLs already exist in the database. Nothing to do. ✨")
        print(f"Total URLs in list: {len(all_urls)}. All were skipped.")
        return

    # 4. Mulai Proses Paralel
    print(f"\n=== Step 3: Starting Parallel Scraping ===")
    print(f"Total URLs in source list: {len(all_urls)}")
    print(f"URLs already in DB (skipped): {skipped_count}")
    print(f"New URLs to scrape: {len(urls_to_scrape)}")
    print(f"Using a maximum of {MAX_WORKERS} parallel workers.")
    print("=" * 50)

    start_time = time.time()
    results = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {executor.submit(process_single_url, url): url for url in urls_to_scrape}

        for i, future in enumerate(concurrent.futures.as_completed(future_to_url), 1):
            url = future_to_url[future]
            try:
                completed_url, status = future.result()
                results[completed_url] = status
                print(f"[{i}/{len(urls_to_scrape)}] FINISHED: {completed_url.split('/')[-2][:40]}... -> {status}")
            except Exception as exc:
                error_status = f"Execution generated an exception: {exc}"
                results[url] = error_status
                print(f"[{i}/{len(urls_to_scrape)}] FAILED: {url} -> {error_status}")

    end_time = time.time()
    duration = end_time - start_time

    # 5. Tampilkan Ringkasan
    print("\n" + "=" * 50)
    print("=== PARALLEL SCRAPING COMPLETE ===")
    print(f"Time taken for new articles: {duration:.2f} seconds")
    
    success_count = sum(1 for status in results.values() if "Success" in status)
    failed_count = len(results) - success_count
    
    print(f"\n--- Final Summary ---")
    print(f"Total URLs in source list: {len(all_urls)}")
    print(f"Skipped (already in DB): {skipped_count}")
    print(f"New URLs attempted: {len(urls_to_scrape)}")
    print(f"  - Successful saves: {success_count}")
    print(f"  - Failed attempts: {failed_count}")
    print("=" * 50)


if __name__ == "__main__":
    main()