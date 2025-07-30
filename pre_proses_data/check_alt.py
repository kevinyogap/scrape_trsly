from bs4 import BeautifulSoup
import sys

def extract_alts_from_xml(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        xml = f.read()
    soup = BeautifulSoup(xml, "xml")
    alt_list = []

    for item in soup.find_all("item"):
        content = item.find("content:encoded")
        if content:
            html = BeautifulSoup(content.text, "html.parser")
            for tag in html.find_all(attrs={"alt": True}):
                alt_text = tag.get("alt", "").strip()
                alt_list.append(alt_text)
    return alt_list

def compare_alts(file1, file2):
    alts1 = extract_alts_from_xml(file1)
    alts2 = extract_alts_from_xml(file2)

    set1 = set(alts1)
    set2 = set(alts2)

    only_in_file1 = set1 - set2
    only_in_file2 = set2 - set1
    common = set1 & set2

    print(f"\n📝 Total alt in {file1}: {len(alts1)}")
    print(f"📝 Total alt in {file2}: {len(alts2)}")

    print(f"\n❌ Alt only in {file1} (removed or modified):")
    for alt in sorted(only_in_file1):
        print(f"  - {alt[:100]}... [{len(alt)} chars]")

    print(f"\n➕ Alt only in {file2} (new or modified):")
    for alt in sorted(only_in_file2):
        print(f"  - {alt[:100]}... [{len(alt)} chars]")

    print(f"\n✅ Unchanged alt text (exists in both files): {len(common)}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python compare_alt_in_xml.py file1.xml file2.xml")
        sys.exit(1)

    compare_alts(sys.argv[1], sys.argv[2])
