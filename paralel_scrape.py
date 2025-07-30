import concurrent.futures
import time
import os
import logging

# Impor kelas Scraper dan daftar URL dari file asli Anda
from scrape import TrstdlyScraper, urls

# Konfigurasi logging dasar untuk file paralel
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- VARIABEL KONFIGURASI ---
# Sesuaikan jumlah 'worker' atau thread paralel sesuai kebutuhan.
# Nilai yang baik untuk tugas I/O-bound adalah antara 5 dan 20.
# Terlalu banyak worker dapat membebani server target atau jaringan Anda.
MAX_WORKERS = 15

def process_single_url(url: str) -> tuple[str, str]:
    """
    Fungsi ini adalah 'pekerja' untuk setiap thread.
    Fungsi ini akan:
    1. Membuat instance Scraper baru untuk memastikan thread-safety (setiap thread punya session dan koneksi db sendiri).
    2. Menjalankan proses scraping lengkap untuk satu URL.
    3. Menyimpan file HTML dan data ke database.
    4. Mengembalikan status hasilnya.
    """
    logger.info(f"Worker starting for URL: {url}")
    # Membuat instance scraper di dalam worker memastikan tidak ada resource yang di-share antar thread (seperti koneksi db atau session)
    scraper = TrstdlyScraper()
    try:
        # 1. Scrape artikel
        scrape_result = scraper.scrape_article(url)

        if scrape_result:
            article_data, full_html, content_for_db = scrape_result

            # 2. Simpan file HTML (operasi ini aman untuk dijalankan secara paralel)
            scraper._save_html_to_file(url, full_html)

            # 3. Simpan ke DB (metode _save_to_db sudah menangani koneksi/diskoneksi sendiri, jadi aman untuk thread)
            db_status = scraper._save_to_db(article_data, content_for_db)

            logger.info(f"Worker finished successfully for URL: {url} -> {db_status}")
            return url, db_status
        else:
            logger.error(f"Worker failed to scrape content for: {url}")
            return url, "Failed to scrape content"

    except Exception as e:
        # Menangkap error tak terduga yang mungkin terjadi selama proses satu URL
        logger.critical(f"An unexpected error occurred in worker for {url}: {e}", exc_info=True)
        return url, f"Critical Error: {e}"

def main():
    """
    Fungsi utama untuk mengatur dan menjalankan proses scraping paralel.
    """
    # Atur password DB seperti pada skrip asli
    os.environ['DB_PASSWORD'] = 'admin' # Ganti dengan password Anda jika berbeda

    # --- 1. Inisialisasi Database ---
    # Lakukan ini sekali saja di awal, sebelum memulai proses paralel.
    print("=== Initializing Database (if necessary) ===")
    init_scraper = TrstdlyScraper()
    init_scraper.init_db()
    print("=" * 40)

    # --- 2. Siapkan Daftar URL ---
    # Gabungkan semua URL yang ingin Anda proses
    all_urls = urls
    # Untuk pengujian cepat, Anda bisa menggunakan slice: all_urls = urls1[:20]
    
    print(f"\n=== Starting Parallel Scraping for {len(all_urls)} Articles ===")
    print(f"Using a maximum of {MAX_WORKERS} parallel workers.")
    print("=" * 40)

    # Catat waktu mulai
    start_time = time.time()
    
    # Dictionary untuk menyimpan hasil akhir
    results = {}

    # --- 3. Jalankan Proses Paralel ---
    # Menggunakan ThreadPoolExecutor untuk mengelola thread worker
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # `executor.submit` akan memulai setiap tugas di thread yang berbeda
        # Kita menyimpan 'future' object untuk mendapatkan hasilnya nanti
        future_to_url = {executor.submit(process_single_url, url): url for url in all_urls}

        # Menggunakan `as_completed` untuk memproses hasil segera setelah sebuah thread selesai
        # Ini lebih efisien dan memberikan feedback real-time
        for i, future in enumerate(concurrent.futures.as_completed(future_to_url), 1):
            url = future_to_url[future]
            try:
                # Dapatkan hasil (return value) dari fungsi worker
                completed_url, status = future.result()
                results[completed_url] = status
                print(f"[{i}/{len(all_urls)}] FINISHED: {completed_url} -> {status}")
            except Exception as exc:
                # Menangkap error yang mungkin dilempar oleh future itu sendiri
                error_status = f"Execution generated an exception: {exc}"
                results[url] = error_status
                print(f"[{i}/{len(all_urls)}] FAILED: {url} -> {error_status}")

    # Catat waktu selesai
    end_time = time.time()
    duration = end_time - start_time

    print("\n" + "=" * 40)
    print("=== PARALLEL SCRAPING COMPLETE ===")
    print(f"Total time taken: {duration:.2f} seconds")
    print(f"Processed {len(results)} URLs.")
    
    # Hitung ringkasan hasil
    success_count = sum(1 for status in results.values() if "Success" in status)
    failed_count = len(results) - success_count
    print(f"Successful saves: {success_count}")
    print(f"Failed attempts: {failed_count}")
    print("=" * 40)


if __name__ == "__main__":
    main()