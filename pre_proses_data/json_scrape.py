import json
import os
from pathlib import Path

# Nama file input dan output
input_file = "scraped_articles_batch13.json"
output_folder = "cleaned"
output_path = Path(output_folder) / input_file

# Pastikan folder output ada
os.makedirs(output_folder, exist_ok=True)

# Baca file asli
with open(input_file, "r", encoding="utf-8") as f:
    raw_data = json.load(f)

# Ambil key query, lalu ambil datanya
query_key = next(iter(raw_data))
articles = raw_data[query_key]

# Bungkus ulang
wrapped_data = {
    "scraped_articles": articles
}

# Simpan ke file baru di folder cleaned/
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(wrapped_data, f, ensure_ascii=False, indent=2)

print(f"✅ File bersih disimpan di: {output_path}")
