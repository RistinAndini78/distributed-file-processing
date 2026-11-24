import re
from collections import Counter
from common.dataset_generator import CATEGORIES, COMMON_WORDS

STOPWORDS = set(COMMON_WORDS)
TOKEN_PATTERN = re.compile(r"[a-zA-Z]+")

# Daftar kata kunci default untuk fitur pencarian kata (keyword search)
DEFAULT_KEYWORDS = ["paralel", "terdistribusi", "server", "kuantum", "investasi", "vaksin"]

def process_single_file(filepath, keywords=None):
    """
    Memproses satu file teks:
    1. Tokenisasi & pembersihan tanda baca
    2. Perhitungan frekuensi kata (mengabaikan stopwords)
    3. Pencarian kemunculan kata kunci tertentu (keyword search)
    4. Pengelompokan (klasifikasi) dokumen berdasarkan skor kategori tertinggi
    """
    if keywords is None:
        keywords = DEFAULT_KEYWORDS

    with open(filepath, "r", encoding="utf-8") as f:
        text = f.read().lower()

    tokens = TOKEN_PATTERN.findall(text)
    total_words = len(tokens)

    # 1. Hitung frekuensi semua kata non-stopword
    filtered_tokens = [t for t in tokens if t not in STOPWORDS]
    word_freq = Counter(filtered_tokens)

    # 2. Pencarian kata kunci spesifik
    keyword_hits = {kw: word_freq.get(kw.lower(), 0) for kw in keywords}

    # 3. Pengelompokan dokumen (Document Clustering / Classification)
    category_scores = {}
    for cat_name, vocab in CATEGORIES.items():
        score = sum(word_freq.get(word, 0) for word in vocab)
        category_scores[cat_name] = score

    assigned_category = max(category_scores, key=category_scores.get)

    # Ambil top 10 kata paling sering muncul di file ini agar payload jaringan tetap ringkas
    top_words = dict(word_freq.most_common(10))

    return {
        "file": filepath,
        "total_words": total_words,
        "category": assigned_category,
        "keyword_hits": keyword_hits,
        "top_words": top_words
    }
