"""
Trstdly.com Article Scraper with Image Compression, LD+JSON parsing, and PostgreSQL Integration
Scrapes articles, extracts structured data, compresses images, and saves clean HTML content to a PostgreSQL database.
"""

import requests
from bs4 import BeautifulSoup
import time
import re
from urllib.parse import urljoin, urlparse
from pathlib import Path
import json
from typing import List, Dict, Optional, Tuple
import logging
from collections import OrderedDict
import lxml.etree as ET
import psycopg2
import os

from urls_part_1 import all_urls as urls1
from urls_part_2 import all_urls as urls2
from urls_part_3 import all_urls as urls3
from urls_part_4 import all_urls as urls4
from urls_part_5 import all_urls as urls5
from all_urls_combined import all_urls as urls

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- FUNGSI KOMPRESI GAMBAR (Tidak Berubah) ---
def compress_image_url(image_url: str) -> str:
    api_url = f"https://gateway.galang.eu.org/api/image/compress?url={image_url}"
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*", "Referer": "https://gateway.galang.eu.org/", "Origin": "https://gateway.galang.eu.org"}
    try:
        logger.info(f"Attempting to compress image: {image_url}")
        response = requests.get(api_url, headers=headers, timeout=20)
        response.raise_for_status()
        compressed_url = response.text.strip()
        if compressed_url.startswith("http"):
            logger.info(f"  -> Success. New URL: {compressed_url}")
            return compressed_url
        else:
            logger.warning(f"  -> API returned invalid text, falling back to original URL.")
            return image_url
    except requests.RequestException as e:
        logger.error(f"  -> Image compression failed: {e}. Falling back to original URL.")
        return image_url

class TrstdlyScraper:
    def __init__(self, delay: float = 1.5):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        })
        self.db_params = {
            'dbname': os.getenv('DB_NAME', 'scraper_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'your_password'),
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432')
        }
        self.conn = None

    # --- Bagian Database ---
    def _connect_db(self):
        if self.conn is None or self.conn.closed:
            try:
                logger.info("Connecting to the PostgreSQL database...")
                self.conn = psycopg2.connect(**self.db_params)
            except psycopg2.OperationalError as e:
                logger.error(f"Could not connect to database: {e}")
                self.conn = None

    def _close_db(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Database connection closed.")

    def init_db(self):
        self._connect_db()
        if not self.conn: return

        create_table_query = """
        CREATE TABLE IF NOT EXISTS scraped_articles (
            id SERIAL PRIMARY KEY,
            url TEXT NOT NULL UNIQUE,
            title TEXT,
            meta_description TEXT,
            meta_keywords TEXT,
            meta_tag TEXT,
            author_name TEXT,
            type TEXT,
            publisher_name TEXT,
            publisher_url TEXT,
            date_published TIMESTAMPTZ,
            date_created TIMESTAMPTZ,
            date_modified TIMESTAMPTZ,
            content TEXT
        );
        """
        try:
            with self.conn.cursor() as cur:
                logger.info("Initializing/updating database table 'scraped_articles'...")
                cur.execute(create_table_query)
                self.conn.commit()
                logger.info("Database initialized successfully.")
        except psycopg2.Error as e:
            logger.error(f"Error initializing database: {e}")
            if self.conn: self.conn.rollback()
        finally:
            self._close_db()

    def _save_to_db(self, article_data: Dict[str, any], content_for_db: str) -> str:
        self._connect_db()
        if not self.conn:
            logger.error("Cannot save to DB, no connection available.")
            return "Failed: No database connection"

        upsert_query = """
        INSERT INTO scraped_articles (
            url, title, meta_description, meta_keywords, meta_tag,
            author_name, type, publisher_name, publisher_url,
            date_published, date_created, date_modified, content
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (url) DO UPDATE SET
            title = EXCLUDED.title,
            meta_description = EXCLUDED.meta_description,
            meta_keywords = EXCLUDED.meta_keywords,
            meta_tag = EXCLUDED.meta_tag,
            author_name = EXCLUDED.author_name,
            type = EXCLUDED.type,
            publisher_name = EXCLUDED.publisher_name,
            publisher_url = EXCLUDED.publisher_url,
            date_published = EXCLUDED.date_published,
            date_created = EXCLUDED.date_created,
            date_modified = EXCLUDED.date_modified,
            content = EXCLUDED.content;
        """
        try:
            with self.conn.cursor() as cur:
                data_tuple = (
                    article_data['url'], article_data['title'],
                    article_data['meta_description'], article_data['meta_keywords'], article_data['meta_tag'],
                    article_data.get('ld_author_name'),
                    article_data.get('ld_type'),
                    article_data.get('ld_publisher_name'),
                    article_data.get('ld_publisher_url'),
                    article_data.get('ld_date_published'),
                    article_data.get('ld_date_created'),
                    article_data.get('ld_date_modified'),
                    content_for_db
                )
                cur.execute(upsert_query, data_tuple)
                self.conn.commit()
            return "Successfully saved to database."
        except psycopg2.Error as e:
            logger.error(f"Error saving to database: {e}")
            if self.conn: self.conn.rollback()
            return f"Failed: {e}"

    def _save_html_to_file(self, url: str, html_content: str, folder: str = 'output_html'):
        try:
            output_dir = Path(folder)
            output_dir.mkdir(parents=True, exist_ok=True)
            parsed_url = urlparse(url)
            filename = (parsed_url.path.replace('/', '_').replace('.', '-').strip('_') + ".html")
            if not filename.endswith(".html"):
                filename += ".html"
            filepath = output_dir / filename
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logger.info(f"Successfully saved full HTML to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save HTML file for {url}: {e}")

    def fetch_page(self, url: str, max_retries: int = 3) -> Optional[BeautifulSoup]:
        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching: {url} (attempt {attempt + 1})")
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'lxml')
                return soup
            except requests.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(self.delay * (attempt + 1))
                else:
                    logger.error(f"Failed to fetch {url} after {max_retries} attempts")
        return None

    def extract_article_content(self, soup: BeautifulSoup, url: str) -> Dict[str, str]:
        article_data = {
            'url': url, 'title': '', 'description': '', 'content': '',
            'meta_description': '', 'meta_keywords': '', 'meta_tag': '', 'images': [],
            'ld_type': None, 'ld_date_created': None, 'ld_date_published': None,
            'ld_date_modified': None, 'ld_author_name': None, 'ld_publisher_name': None,
            'ld_publisher_url': None
        }
        title_elem = soup.find('h1', class_='article-title') or soup.find('title')
        if title_elem: article_data['title'] = title_elem.get_text(strip=True)
        desc_elem = soup.find('p', class_='article-sinopsis')
        if desc_elem: article_data['description'] = desc_elem.get_text(strip=True)
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc: article_data['meta_description'] = meta_desc.get('content', '')
        meta_keywords = soup.find('meta', attrs={'name': 'keywords'}) or soup.find('meta', attrs={'name': 'tags'})
        if meta_keywords: article_data['meta_keywords'] = meta_keywords.get('content', '')
        tag_list = soup.find('ul', class_='box-list--related')
        if tag_list:
            tags = [a.get('href').split('/tag/')[-1] for a in tag_list.find_all('a') if '/tag/' in a.get('href', '')]
            article_data['meta_tag'] = ','.join(tags)
        ld_json_script = soup.find('script', type='application/ld+json')
        if ld_json_script:
            try:
                ld_data = json.loads(ld_json_script.string)
                data_list = ld_data if isinstance(ld_data, list) else [ld_data]
                for item in data_list:
                    if item.get('@type') == 'NewsArticle':
                        logger.info("Found 'NewsArticle' in LD+JSON. Extracting data.")
                        article_data['ld_type'] = item.get('@type')
                        article_data['ld_date_created'] = item.get('dateCreated')
                        article_data['ld_date_published'] = item.get('datePublished')
                        article_data['ld_date_modified'] = item.get('dateModified')
                        article_data['ld_author_name'] = item.get('author', {}).get('name')
                        article_data['ld_publisher_name'] = item.get('publisher', {}).get('name')
                        article_data['ld_publisher_url'] = item.get('publisher', {}).get('url')
                        break
            except (json.JSONDecodeError, AttributeError):
                logger.warning("Could not parse LD+JSON content.")
            except Exception as e:
                logger.error(f"An unexpected error occurred during LD+JSON parsing: {e}")
        else:
            logger.info("LD+JSON script tag not found on page.")
        content_html = self._extract_raw_content(soup)
        article_data['content'] = content_html
        return article_data

    def _extract_raw_content(self, soup: BeautifulSoup) -> str:
        article_container = soup.find('div', class_=['article', 'article--version2'])
        if not article_container: return ""

        content_copy = BeautifulSoup(str(article_container), 'lxml')

        # Remove unwanted elements
        unwanted_selectors = ['nav[aria-label="breadcrumb"]', '.article-share', '.gpt-ads',
                             '.banner', 'script', 'style', 'h1.article-title',
                             'p.article-sinopsis', 'time', '.article-tag',
                             '.flex.flex-wrap.justify-between']
        for selector in unwanted_selectors:
            for elem in content_copy.select(selector):
                elem.decompose()

        # Process images and figures
        figures = content_copy.find_all('figure')
        for i, figure in enumerate(figures):
            img = figure.find('img')
            if not img:
                figure.decompose()
                continue

            # For the first image (cover), we'll keep it simple without figcaption
            if i == 0:
                img_str = f'<img src="{img.get("src", "")}" alt="{img.get("alt", "")}"/>'
                figure.replace_with(BeautifulSoup(img_str, 'html.parser'))
            else:
                # For other images, keep both img and figcaption
                elements_to_insert = []
                attrs = OrderedDict([('src', img.get('src', '')), ('alt', img.get('alt', ''))])
                new_img = soup.new_tag('img', attrs=attrs)
                elements_to_insert.append(new_img)

                for fc in figure.find_all('figcaption'):
                    new_fc = soup.new_tag('figcaption')
                    new_fc.extend(fc.contents)
                    elements_to_insert.append(new_fc)

                for element in reversed(elements_to_insert):
                    figure.insert_after(element)
                figure.decompose()

        # Simplify other tags
        for iframe in content_copy.find_all('iframe'):
            src = iframe.get('src')
            iframe.attrs = {}
            if src: iframe['src'] = src

        for a_tag in content_copy.find_all('a'):
            href = a_tag.get('href')
            a_tag.attrs = {}
            if href: a_tag['href'] = href

        for tag in content_copy.find_all(['li', 'span', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
                                        'p', 'ol', 'ul', 'blockquote']):
            tag.attrs = {}

        content_body = content_copy.find('div')
        if content_body:
            # Unwrap div tags
            for div_tag in content_body.find_all('div'):
                div_tag.unwrap()

            # Compress images
            for img_tag in content_body.find_all('img'):
                original_src = img_tag.get('src')
                if original_src:
                    img_tag['src'] = compress_image_url(original_src)

            # Build content HTML
            content_html = ""
            for element in content_body.children:
                if not hasattr(element, 'name') or not element.name:
                    if isinstance(element, str) and element.strip():
                        content_html += element + '\n'
                    continue

                if element.name == 'img':
                    src = element.get('src', '')
                    alt = element.get('alt', '')
                    img_str = f'<img src="{src}" alt="{alt}"/>'
                    content_html += img_str + '\n'
                else:
                    content_html += str(element) + '\n'
        else:
            content_html = str(content_copy)

        return content_html.strip()

    def _post_process_content(self, content: str) -> str:
        content = re.sub(r'</?body[^>]*>', '', content)
        content = re.sub(r'</?html[^>]*>', '', content)
        content = re.sub(r'<br\s*/?>', '<br>', content)
        content = re.sub(r'<br>\s*<br>', '<br><br>', content)
        lines = content.split('\n')
        processed_lines = []
        for i, line in enumerate(lines):
            line = line.strip()
            if not line: continue
            processed_lines.append(line)
            if line.startswith(('<img ', '<figcaption>', '<h2>', '<span></span>', '<iframe')) or (line.startswith('<p>') and line.endswith('</p>')):
                if not line.startswith('<img '): processed_lines.append('')
            elif not line.startswith('<') and not line.endswith('>'):
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if next_line.startswith(('<img ', '<span></span>', '<h2>', '<iframe')):
                        processed_lines.append('')
        result = '\n'.join(processed_lines)
        result = re.sub(r'\n\s*\n\s*\n+', '\n\n', result)
        return result.strip()

    def generate_meta_tags(self, article_data: Dict[str, any]) -> str:
        meta_tags = []
        tag_map = {'ld_date_published': 'article:published_time', 'ld_date_modified': 'article:modified_time', 'ld_author_name': 'article:author', 'ld_publisher_name': 'article:publisher'}
        for key, prop in tag_map.items():
            if article_data.get(key):
                meta_tags.append(f'<meta property="{prop}" content="{article_data[key]}">')
        return '\n'.join(meta_tags)

    def create_clean_html_for_file(self, article_data: Dict[str, str]) -> str:
        clean_content = self._post_process_content(article_data['content'])

        html_parts = []

        # Add meta tags
        if article_data['title']:
            html_parts.append(f'<title>{article_data["title"]}</title>')
        if article_data['meta_description']:
            html_parts.append(f'<meta name="description" content="{article_data["meta_description"]}">')
        if article_data['meta_keywords']:
            html_parts.append(f'<meta name="keywords" content="{article_data["meta_keywords"]}">')
        if article_data['meta_tag']:
            html_parts.append(f'<meta name="tags" content="{article_data["meta_tag"]}">')

        # Add LD+JSON meta tags
        ld_meta_tags = self.generate_meta_tags(article_data)
        if ld_meta_tags:
            html_parts.append(ld_meta_tags)

        html_parts.append('')  # Empty line

        # Add the first image (cover) if exists
        first_img_match = re.search(r'<img [^>]*/>', clean_content)
        if first_img_match:
            html_parts.append(first_img_match.group(0))
            html_parts.append('')  # Empty line

        # Add title (h1)
        if article_data['title']:
            html_parts.extend([f'<h1>{article_data["title"]}</h1>', ''])

        # Add description if exists
        if article_data['description']:
            html_parts.extend([f'<p>{article_data["description"]}</p>', ''])

        # Add the rest of the content (excluding the first image which we already added)
        if clean_content:
            # Remove the first image from content since we already added it
            if first_img_match:
                clean_content = clean_content.replace(first_img_match.group(0), '', 1).strip()

            clean_lines = [line.strip() for line in clean_content.split('\n') if line.strip()]
            for line in clean_lines:
                html_parts.append(line)
                if not line.endswith('</li>'):
                    html_parts.append('')

        result = '\n'.join(html_parts)
        result = re.sub(r'\n{3,}', '\n\n', result)
        return result.strip()

    def scrape_article(self, url: str) -> Optional[Tuple[Dict, str, str]]:
        soup = self.fetch_page(url)
        if not soup: return None

        article_data = self.extract_article_content(soup, url)

        full_html_for_file = self.create_clean_html_for_file(article_data)

        # --- START OF MODIFIED SECTION TO CONSTRUCT DB CONTENT ---
        # Tujuan: Memformat konten DB dengan baris baru agar rapi, mirip dengan file HTML.
        clean_body_content = self._post_process_content(article_data['content'])
        db_parts = []

        # Cari dan ekstrak gambar pertama (sampul)
        first_img_match = re.search(r'<img [^>]*/>', clean_body_content)
        cover_image = ""
        if first_img_match:
            cover_image = first_img_match.group(0)
            # Hapus gambar sampul dari konten utama untuk mencegah duplikasi
            clean_body_content = clean_body_content.replace(cover_image, '', 1).strip()

        # Tambahkan gambar sampul jika ada, diikuti dengan baris kosong
        if cover_image:
            db_parts.append(cover_image)
            db_parts.append('')

        # Tambahkan judul (h1), diikuti dengan baris kosong
        if article_data['title']:
            db_parts.extend([f"<h1>{article_data['title']}</h1>", ''])

        # Tambahkan deskripsi (p), diikuti dengan baris kosong
        if article_data['description']:
            db_parts.extend([f"<p>{article_data['description']}</p>", ''])

        # Tambahkan sisa konten, dengan memberikan spasi setelah setiap elemen
        if clean_body_content:
            clean_lines = [line.strip() for line in clean_body_content.split('\n') if line.strip()]
            for line in clean_lines:
                db_parts.append(line)
                # Tambahkan baris kosong untuk spasi, kecuali setelah item list.
                if not line.endswith('</li>'):
                    db_parts.append('')

        # Gabungkan semua bagian, lalu bersihkan baris kosong yang berlebihan.
        content_for_db = '\n'.join(db_parts).strip()
        content_for_db = re.sub(r'\n{3,}', '\n\n', content_for_db)
        # --- END OF MODIFIED SECTION ---

        time.sleep(self.delay)

        return article_data, full_html_for_file, content_for_db

    def scrape_multiple_articles(self, urls: List[str]):
        results = {}
        self._connect_db()
        for i, url in enumerate(urls, 1):
            logger.info(f"Processing article {i}/{len(urls)}: {url}")
            try:
                scrape_result = self.scrape_article(url)
                if scrape_result:
                    article_data, full_html, content_for_db = scrape_result

                    self._save_html_to_file(url, full_html)
                    db_status = self._save_to_db(article_data, content_for_db)

                    results[url] = db_status
                    logger.info(f"URL: {url} -> {db_status}")
                else:
                    results[url] = "Failed to scrape content"
                    logger.error(f"Failed to scrape: {url}")
            except Exception as e:
                results[url] = f"Error: {str(e)}"
                logger.error(f"Error processing {url}: {e}")
        self._close_db()
        return results

# --- CONTOH PENGGUNAAN (Telah diperbaiki) ---
if __name__ == "__main__":
    os.environ['DB_PASSWORD'] = 'admin'

    scraper = TrstdlyScraper(delay=1.5)

    print("=== Initializing Database (if necessary) ===")
    scraper.init_db()

    print("\n=== Scraping Multiple Articles and Saving to PostgreSQL ===")
    all_urls = urls  # Ganti dengan urls1, urls2, urls3, urls4, atau urls5 sesuai kebutuhan

    # all_urls = [
    #     # # "https://www.trstdly.com/article/top-5-scariest-japanese-horror-movies-of-all-time-42168-mvk.html",
    #     # # "https://www.trstdly.com/news/6-portraits-of-violinist-mayuko-suenobu-wife-of-yuzuru-hanyu-who-is-8-years-older-42775-mvk.html",
    #     # # "https://www.trstdly.com/yours-truly/the-ultimate-guide-to-the-top-10-best-rizz-lines-and-more-288032-mvk.html",
    # ]
    results = scraper.scrape_multiple_articles(all_urls)

    print("\n--- Scraping Results ---")
    for url, result in results.items():
        print(f"{url}: {result}")