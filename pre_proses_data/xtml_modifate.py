#!/usr/bin/env python3
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path


def truncate_text_intelligently(text):
    """
    Memotong teks hingga di bawah 250 karakter dengan mencoba menghapus kalimat utuh.
    Jika kalimat pertama sudah lebih dari 250 karakter, maka akan dipotong paksa.
    """
    if len(text) <= 250:
        return text

    truncated_text = text
    # Terus-menerus hapus kalimat terakhir sampai panjangnya sesuai
    while len(truncated_text) > 250:
        # Cari posisi tanda baca akhir kalimat terakhir
        last_period = truncated_text.rfind('.')
        last_exclamation = truncated_text.rfind('!')
        last_question = truncated_text.rfind('?')
        
        last_terminator_pos = max(last_period, last_exclamation, last_question)

        # Jika tidak ada tanda baca yang ditemukan, keluar dari loop
        if last_terminator_pos == -1:
            break
            
        # Potong string hingga tanda baca terakhir
        truncated_text = truncated_text[:last_terminator_pos + 1].strip()

    # Jika setelah menghapus semua kalimat masih > 250 (kasus 1 kalimat panjang),
    # potong paksa berdasarkan karakter.
    if len(truncated_text) > 250:
        return text[:250]
    
    return truncated_text


def truncate_alt_attributes(html_content):
    """
    Menemukan semua atribut alt dalam string HTML dan memotong nilainya 
    secara cerdas menjadi maksimal 250 karakter.
    """
    if not html_content:
        return ""

    pattern = re.compile(r'alt=(["\'])(.*?)\1', re.IGNORECASE | re.DOTALL)

    def replacer(match):
        quote_char = match.group(1)
        alt_text = match.group(2)
        
        # Panggil fungsi pemotongan cerdas yang baru
        truncated_text = truncate_text_intelligently(alt_text)
        
        return f'alt={quote_char}{truncated_text}{quote_char}'

    return pattern.sub(replacer, html_content)


def generate_slug(title):
    """
    Generate slug from title
    Args:
        title (str): Article title
    Returns:
        str: URL-friendly slug
    """
    if not title:
        return ""
    
    # Convert to lowercase and remove special characters
    slug = title.lower()
    slug = re.sub(r'[^a-z0-9\s\-]', '', slug)  # Remove special characters
    slug = re.sub(r'\s+', '-', slug)  # Replace spaces with hyphens
    slug = re.sub(r'-+', '-', slug)  # Replace multiple hyphens with single
    slug = slug.strip('-')  # Remove leading/trailing hyphens
    
    return slug


def format_wordpress_date(iso_date):
    """
    Convert ISO date to WordPress format (YYYY-MM-DD HH:MM:SS)
    Args:
        iso_date (str): ISO date string
    Returns:
        str: WordPress formatted date
    """
    try:
        date_obj = datetime.fromisoformat(iso_date.replace('Z', '+00:00'))
        return date_obj.strftime('%Y-%m-%d %H:%M:%S')
    except:
        return iso_date


def format_pub_date(iso_date):
    """
    Convert ISO date to RFC 2822 format for pubDate
    Args:
        iso_date (str): ISO date string
    Returns:
        str: RFC 2822 formatted date
    """
    try:
        date_obj = datetime.fromisoformat(iso_date.replace('Z', '+00:00'))
        return date_obj.strftime('%a, %d %b %Y %H:%M:%S GMT')
    except:
        return iso_date


def extract_first_image(content):
    """
    Extract first image from content
    Args:
        content (str): HTML content
    Returns:
        str: First image tag or empty string
    """
    if not content:
        return ""
    
    img_match = re.search(r'<img[^>]*>', content, re.IGNORECASE)
    return img_match.group(0) if img_match else ""


def extract_h1_title(content):
    """
    Extract H1 title from content
    Args:
        content (str): HTML content
    Returns:
        str: H1 tag or empty string
    """
    if not content:
        return ""
    
    h1_match = re.search(r'<h1[^>]*>.*?</h1>', content, re.IGNORECASE | re.DOTALL)
    return h1_match.group(0) if h1_match else ""


def get_remaining_content(content):
    """
    Get remaining content after first image and h1
    Args:
        content (str): Original HTML content
    Returns:
        str: Content without first image and h1
    """
    if not content:
        return ""
    
    remaining = content
    
    # Remove first image
    img_match = re.search(r'<img[^>]*>', remaining, re.IGNORECASE)
    if img_match:
        remaining = remaining.replace(img_match.group(0), "").strip()
    
    # Remove h1
    h1_match = re.search(r'<h1[^>]*>.*?</h1>', remaining, re.IGNORECASE | re.DOTALL)
    if h1_match:
        remaining = remaining.replace(h1_match.group(0), "").strip()
    
    return remaining


def format_comma_separated(comma_separated):
    """
    Format comma-separated values with spaces after commas
    Args:
        comma_separated (str): Comma-separated string
    Returns:
        str: Formatted string with spaces after commas
    """
    if not comma_separated:
        return ""
    
    items = [item.strip() for item in comma_separated.split(',')]
    return ', '.join(items)


def remove_trstdly_references(content):
    """
    Remove "trstdly.com" strings from content
    Args:
        content (str): HTML content
    Returns:
        str: Content with trstdly.com references removed
    """
    if not content:
        return ""
    
    # Remove various patterns of trstdly.com references
    content = re.sub(r'\s*@\s*\d{4}\s*trstdly\.com', '', content, flags=re.IGNORECASE)
    content = re.sub(r'\s*©\s*\d{4}\s*trstdly\.com', '', content, flags=re.IGNORECASE)
    content = re.sub(r'trstdly\.com', '', content, flags=re.IGNORECASE)
    content = re.sub(r'\s+', ' ', content)  # Clean up multiple spaces
    content = content.strip()
    
    return content


def get_first_keyword(meta_keywords):
    """
    Get first keyword from meta_keywords (before first comma)
    Args:
        meta_keywords (str): Meta keywords string
    Returns:
        str: First keyword
    """
    if not meta_keywords:
        return ""
    
    return meta_keywords.split(',')[0].strip()


def process_content(original_content, meta_description, meta_tags, meta_keywords):
    """
    Process content to insert meta tags after h1
    Args:
        original_content (str): Original HTML content
        meta_description (str): Meta description
        meta_tags (str): Meta tags
        meta_keywords (str): Meta keywords
    Returns:
        str: Processed content with meta tags
    """
    # Pastikan semua atribut alt di konten asli tidak lebih dari 250 karakter
    original_content = truncate_alt_attributes(original_content)
    
    first_image = extract_first_image(original_content)
    h1_title = extract_h1_title(original_content)
    remaining_content = get_remaining_content(original_content)
    
    # Build processed content following exact format from sample
    processed_content = ""
    
    if first_image:
        # Add class="aligncenter" to first image like in sample
        img_with_class = first_image.replace('<img ', '<img class="aligncenter" ')
        processed_content += img_with_class + "\n\n"
    
    if h1_title:
        processed_content += h1_title + "\n\n"
    
    # Add meta tags exactly like in sample
    if meta_description:
        processed_content += f'<meta name="description" content="{meta_description}">\n\n'
    
    if meta_tags:
        formatted_tags = format_comma_separated(meta_tags)
        processed_content += f'<meta name="tags" content="{formatted_tags}">\n\n'
    
    if meta_keywords:
        formatted_keywords = format_comma_separated(meta_keywords)
        processed_content += f'<meta name="keywords" content="{formatted_keywords}">\n\n'
    
    if remaining_content:
        # Remove trstdly.com references from content
        cleaned_content = remove_trstdly_references(remaining_content)
        processed_content += cleaned_content
    
    return processed_content


def generate_xml_header():
    """
    Generate XML header exactly like sample
    Returns:
        str: XML header
    """
    return '''<?xml version="1.0" encoding="UTF-8"?>
<!--This is a WordPress eXtended RSS file generated by WordPress as an export of your site.-->
<!--It contains information about your site's posts, pages, comments, categories, and other content.-->
<!--You may use this file to transfer that content from one site to another.-->
<!--This file is not intended to serve as a complete backup of your site.-->
<!--To import this information into a WordPress site follow these steps:-->
<!--1. Log in to that site as an administrator.-->
<!--2. Go to Tools: Import in the WordPress admin panel.-->
<!--3. Install the "WordPress" importer from the list.-->
<!--4. Activate & Run Importer.-->
<!--5. Upload this file using the form provided on that page.-->
<!--6. You will first be asked to map the authors in this export file to users on the site.-->
<!--7. WordPress will then import each of the posts, pages, comments, categories, etc. contained in this file into your site.-->
<rss version="2.0" xmlns:excerpt="http://wordpress.org/export/1.2/excerpt/" xmlns:content="http://purl.org/rss/1.0/modules/content/" xmlns:wfw="http://wellformedweb.org/CommentAPI/" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:wp="http://wordpress.org/export/1.2/">'''


def generate_channel_info():
    """
    Generate channel information exactly like sample
    Returns:
        str: Channel info XML
    """
    current_date = datetime.now().strftime('%a, %d %b %Y %H:%M:%S GMT')
    return f'''\t<channel>
\t\t<title><![CDATA[cara]]></title>
\t\t<link><![CDATA[https://cnc.galang.eu.org]]></link>
\t\t<description><![CDATA[cara]]></description>
\t\t<pubDate>{current_date}</pubDate>
\t\t<language>en-US</language>
\t\t<wp:wxr_version>1.2</wp:wxr_version>
\t\t<wp:base_site_url><![CDATA[https://cnc.galang.eu.org]]></wp:base_site_url>
\t\t<wp:base_blog_url><![CDATA[https://cnc.galang.eu.org]]></wp:base_blog_url>'''


def generate_author():
    """
    Generate single author exactly like sample
    Returns:
        str: Author XML
    """
    return '''\t\t<wp:author>
\t\t\t<wp:author_id>1</wp:author_id>
\t\t\t<wp:author_login><![CDATA[Galang]]></wp:author_login>
\t\t\t<wp:author_email><![CDATA[nganuwijaya05@gmail.com]]></wp:author_email>
\t\t\t<wp:author_display_name><![CDATA[Galang]]></wp:author_display_name>
\t\t\t<wp:author_first_name><![CDATA[]]></wp:author_first_name>
\t\t\t<wp:author_last_name><![CDATA[]]></wp:author_last_name>
\t\t</wp:author>'''


def generate_category():
    """
    Generate single category exactly like sample
    Returns:
        str: Category XML
    """
    return '''\t\t<wp:category>
\t\t\t<wp:term_id>1</wp:term_id>
\t\t\t<wp:category_nicename><![CDATA[cara]]></wp:category_nicename>
\t\t\t<wp:category_parent><![CDATA[]]></wp:category_parent>
\t\t\t<wp:cat_name><![CDATA[cara]]></wp:cat_name>
\t\t</wp:category>'''


def generate_generator():
    """
    Generate generator tag exactly like sample
    Returns:
        str: Generator XML
    """
    return '''\t\t<generator><![CDATA[https://wordpress.org/?v=6.7.2]]></generator>'''


def generate_article_item(article, post_id):
    """
    Generate single article item exactly like sample structure
    Args:
        article (dict): Article data
        post_id (int): WordPress post ID
    Returns:
        str: Article XML
    """
    # Use first meta_keywords as title, fallback to original title if no keywords
    first_keyword = get_first_keyword(article.get('meta_keywords', ''))
    article_title = first_keyword or article.get('title', '')
    
    slug = generate_slug(article_title)
    pub_date = format_pub_date(article.get('date_published', ''))
    post_date = format_wordpress_date(article.get('date_published', ''))
    post_date_gmt = format_wordpress_date(article.get('date_published', ''))
    modified_date = format_wordpress_date(article.get('date_modified', ''))
    modified_date_gmt = format_wordpress_date(article.get('date_modified', ''))
    
    processed_content = process_content(
        article.get('content', ''),
        article.get('meta_description', ''),
        article.get('meta_tag', ''),
        article.get('meta_keywords', '')
    )
    
    return f'''\t\t<item>
\t\t\t<title><![CDATA[{article_title}]]></title>
\t\t\t<link><![CDATA[{article.get('url', '')}]]></link>
\t\t\t<pubDate>{pub_date}</pubDate>
\t\t\t<dc:creator><![CDATA[Galang]]></dc:creator>
\t\t\t<guid isPermaLink="false"><![CDATA[{article.get('url', '')}]]></guid>
\t\t\t<description><![CDATA[]]></description>
\t\t\t<content:encoded><![CDATA[{processed_content}]]></content:encoded>
\t\t\t<excerpt:encoded><![CDATA[]]></excerpt:encoded>
\t\t\t<wp:post_id>{post_id}</wp:post_id>
\t\t\t<wp:post_date>{post_date}</wp:post_date>
\t\t\t<wp:post_date_gmt>{post_date_gmt}</wp:post_date_gmt>
\t\t\t<wp:post_modified>{modified_date}</wp:post_modified>
\t\t\t<wp:post_modified_gmt>{modified_date_gmt}</wp:post_modified_gmt>
\t\t\t<wp:comment_status>closed</wp:comment_status>
\t\t\t<wp:ping_status>closed</wp:ping_status>
\t\t\t<wp:post_name><![CDATA[{slug}]]></wp:post_name>
\t\t\t<wp:status>publish</wp:status>
\t\t\t<wp:post_parent>0</wp:post_parent>
\t\t\t<wp:menu_order>0</wp:menu_order>
\t\t\t<wp:post_type>post</wp:post_type>
\t\t\t<wp:post_password><![CDATA[]]></wp:post_password>
\t\t\t<wp:is_sticky>0</wp:is_sticky>
\t\t\t<wp:postmeta>
\t\t\t\t<wp:meta_key><![CDATA[_pingme]]></wp:meta_key>
\t\t\t\t<wp:meta_value><![CDATA[1]]></wp:meta_value>
\t\t\t</wp:postmeta>
\t\t\t<wp:postmeta>
\t\t\t\t<wp:meta_key><![CDATA[_encloseme]]></wp:meta_key>
\t\t\t\t<wp:meta_value><![CDATA[1]]></wp:meta_value>
\t\t\t</wp:postmeta>
\t\t\t<category domain="language" nicename="en"><![CDATA[English]]></category>
\t\t\t<category domain="category" nicename="cara"><![CDATA[cara]]></category>
\t\t</item>'''


def generate_xml_footer():
    """
    Generate XML footer
    Returns:
        str: XML footer
    """
    return '''\t</channel>
</rss>'''


def convert_json_to_wordpress_xml(json_data, output_file):
    """
    Main conversion function
    Args:
        json_data (dict): Parsed JSON data
        output_file (str): Output file path
    Returns:
        dict: Conversion result
    """
    try:
        # Penyesuaian: Mengambil data artikel dari kunci yang benar
        articles = next(iter(json_data.values()))
        if not isinstance(articles, list):
            raise ValueError("Invalid JSON structure: The first value is not a list of articles")
        
        xml_parts = []
        
        # Generate XML parts exactly like sample
        xml_parts.append(generate_xml_header())
        xml_parts.append(generate_channel_info())
        xml_parts.append(generate_author())
        xml_parts.append(generate_category())
        xml_parts.append(generate_generator())
        
        # Generate articles starting from post ID 40670 like in sample
        for index, article in enumerate(articles):
            post_id = 40670 + index
            xml_parts.append(generate_article_item(article, post_id))
        
        xml_parts.append(generate_xml_footer())
        
        # Join all parts
        xml_content = '\n'.join(xml_parts)
        
        # Ensure output directory exists
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(xml_content)
        
        # Get file size
        file_size = os.path.getsize(output_file)
        
        print(f"✅ Conversion completed successfully!")
        print(f"📄 Processed {len(articles)} articles")
        print(f"📁 Output file: {output_file}")
        print(f"📊 File size: {file_size / 1024:.2f} KB")
        
        return {
            'success': True,
            'articles_processed': len(articles),
            'output_file': output_file,
            'file_size': file_size
        }
        
    except Exception as error:
        print(f"❌ Conversion failed: {str(error)}")
        raise error


def main():
    """
    CLI interface
    """
    input_file = sys.argv[1] if len(sys.argv) > 1 else "scraped_articles_batch1.json"
    output_file = sys.argv[2] if len(sys.argv) > 2 else "output/wordpress-export.xml"
    
    if not os.path.exists(input_file):
        print(f"❌ Input file not found: {input_file}")
        sys.exit(1)
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        convert_json_to_wordpress_xml(json_data, output_file)
        sys.exit(0)
        
    except json.JSONDecodeError as e:
        print(f"❌ Failed to parse JSON file: {str(e)}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()