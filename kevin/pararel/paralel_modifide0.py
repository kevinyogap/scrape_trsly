import concurrent.futures
import time
import os
import logging
import psycopg2

# Impor kelas Scraper dan daftar URL dari file asli Anda
from scrape import TrstdlyScraper, urls

# Konfigurasi logging dasar untuk file paralel
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- VARIABEL KONFIGURASI ---
MAX_WORKERS = 15

# Konfigurasi koneksi DB manual (karena kita tidak bisa modifikasi TrstdlyScraper)
DB_CONFIG = {
    'dbname': 'scraper_db',
    'user': 'postgres',
    'password': 'admin',  # Ganti sesuai kebutuhan
    'host': 'localhost',
    'port': 5432
}

def url_exists_direct(url: str) -> bool:
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM scraped_articles WHERE url = %s LIMIT 1", (url,))
                return cur.fetchone() is not None
    except Exception as e:
        logger.error(f"Error during DB check for URL: {url} -> {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()

def process_single_url(url: str) -> tuple[str, str]:
    logger.info(f"Worker starting for URL: {url}")

    # 🛑 CEK apakah URL sudah ada di database
    try:
        if url_exists_direct(url):
            logger.info(f"Skipping URL (already exists): {url}")
            return url, "Skipped (already in DB)"
    except Exception as e:
        logger.error(f"Failed checking existence for URL: {url} with error: {e}")
        return url, f"Failed existence check: {e}"

    scraper = TrstdlyScraper()
    try:
        scrape_result = scraper.scrape_article(url)

        if scrape_result:
            article_data, full_html, content_for_db = scrape_result
            scraper._save_html_to_file(url, full_html)
            db_status = scraper._save_to_db(article_data, content_for_db)

            logger.info(f"Worker finished successfully for URL: {url} -> {db_status}")
            return url, db_status
        else:
            logger.error(f"Worker failed to scrape content for: {url}")
            return url, "Failed to scrape content"

    except Exception as e:
        logger.critical(f"An unexpected error occurred in worker for {url}: {e}", exc_info=True)
        return url, f"Critical Error: {e}"

def main():
    os.environ['DB_PASSWORD'] = 'admin'

    print("=== Initializing Database (if necessary) ===")
    init_scraper = TrstdlyScraper()
    init_scraper.init_db()
    print("=" * 40)

    all_urls = urls
    print(f"\n=== Starting Parallel Scraping for {len(all_urls)} Articles ===")
    print(f"Using a maximum of {MAX_WORKERS} parallel workers.")
    print("=" * 40)

    start_time = time.time()
    results = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {executor.submit(process_single_url, url): url for url in all_urls}

        for i, future in enumerate(concurrent.futures.as_completed(future_to_url), 1):
            url = future_to_url[future]
            try:
                completed_url, status = future.result()
                results[completed_url] = status
                print(f"[{i}/{len(all_urls)}] FINISHED: {completed_url} -> {status}")
            except Exception as exc:
                error_status = f"Execution generated an exception: {exc}"
                results[url] = error_status
                print(f"[{i}/{len(all_urls)}] FAILED: {url} -> {error_status}")

    end_time = time.time()
    duration = end_time - start_time

    print("\n" + "=" * 40)
    print("=== PARALLEL SCRAPING COMPLETE ===")
    print(f"Total time taken: {duration:.2f} seconds")
    print(f"Processed {len(results)} URLs.")

    success_count = sum(1 for status in results.values() if "Success" in status)
    skipped_count = sum(1 for status in results.values() if "Skipped" in status)
    failed_count = len(results) - success_count - skipped_count

    print(f"Successful saves: {success_count}")
    print(f"Skipped (already in DB): {skipped_count}")
    print(f"Failed attempts: {failed_count}")
    print("=" * 40)

if __name__ == "__main__":
    main()
