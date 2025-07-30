import os
from bs4 import BeautifulSoup

input_folder = r"datalama\output_html100"
long_alt_results = {}

for filename in os.listdir(input_folder):
    if filename.lower().endswith(".html"):
        file_path = os.path.join(input_folder, filename)
        with open(file_path, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f.read(), "html.parser")
            for tag in soup.find_all(attrs={"alt": True}):
                alt_text = tag.get("alt", "").strip()
                if len(alt_text) > 400:
                    if filename not in long_alt_results:
                        long_alt_results[filename] = []
                    long_alt_results[filename].append((tag.name, len(alt_text), alt_text[:100]))  # preview

# Tampilkan hasil
for file, alts in long_alt_results.items():
    print(f"\n📄 File: {file}")
    for tag_name, length, preview in alts:
        print(f" - <{tag_name}> alt length: {length}, preview: {preview}...")
