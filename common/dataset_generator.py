import os
import random
import time

CATEGORIES = {
    "teknologi": [
        "komputasi", "paralel", "terdistribusi", "jaringan", "server", "prosesor",
        "memori", "algoritma", "arsitektur", "sistem", "protokol", "socket",
        "thread", "node", "klaster", "latensi", "throughput", "enkripsi",
        "basisdata", "kompilasi", "perangkat", "lunak", "keras", "awan"
    ],
    "sains": [
        "fisika", "kimia", "biologi", "molekul", "atom", "energi", "gravitasi",
        "eksperimen", "hipotesis", "laboratorium", "kuantum", "spektrum",
        "organisme", "sel", "genetika", "ekosistem", "iklim", "atmosfer",
        "teleskop", "mikroskop", "reaksi", "katalis", "materi", "radiasi"
    ],
    "ekonomi": [
        "pasar", "investasi", "inflasi", "saham", "perdagangan", "ekspor",
        "impor", "fiskal", "moneter", "anggaran", "pendapatan", "produksi",
        "konsumsi", "modal", "kredit", "perbankan", "pajak", "industri",
        "pertumbuhan", "neraca", "komoditas", "kurs", "dividen", "aset"
    ],
    "kesehatan": [
        "medis", "dokter", "rumah", "sakit", "pasien", "terapi", "vaksin",
        "imunitas", "nutrisi", "diagnosis", "gejala", "obat", "farmasi",
        "klinis", "bedah", "anatomi", "fisiologi", "kardiovaskular",
        "metabolisme", "rehabilitasi", "epidemiologi", "sanitasi", "vitamin"
    ]
}

COMMON_WORDS = [
    "dan", "yang", "di", "ke", "dari", "pada", "dalam", "untuk", "dengan",
    "adalah", "ini", "itu", "oleh", "sebagai", "dapat", "akan", "telah",
    "atau", "juga", "lebih", "sangat", "hasil", "proses", "data", "analisis"
]

def generate_dataset(output_dir="dataset", num_files=200, sentences_per_file=1250, seed=42):
    """
    Menghasilkan 200 file teks besar (~25.000 kata/file, total ~5 juta kata)
    secara cepat untuk pengujian Distributed File Processing.
    """
    # Bersihkan file lama di folder dataset jika ada sisa proses sebelumnya
    os.makedirs(output_dir, exist_ok=True)
    for old_file in os.listdir(output_dir):
        if old_file.endswith(".txt"):
            os.remove(os.path.join(output_dir, old_file))

    rng = random.Random(seed)
    category_names = list(CATEGORIES.keys())

    # Buat pool 500 kalimat per kategori terlebih dahulu agar penulisan file sangat cepat
    sentence_pools = {}
    for cat in category_names:
        pool = []
        domain_vocab = CATEGORIES[cat]
        for _ in range(500):
            words = (
                rng.choices(domain_vocab, k=11)
                + rng.choices(COMMON_WORDS, k=6)
                + rng.choices(CATEGORIES[rng.choice(category_names)], k=3)
            )
            rng.shuffle(words)
            pool.append(" ".join(words).capitalize() + ".")
        sentence_pools[cat] = pool

    start_time = time.perf_counter()
    print(f"[Generator] Membuat {num_files} file teks besar di '{output_dir}'...")

    for i in range(1, num_files + 1):
        chosen_cat = category_names[i % len(category_names)]
        content = "\n".join(rng.choices(sentence_pools[chosen_cat], k=sentences_per_file))

        filename = os.path.join(output_dir, f"doc_{i:04d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write(content)

    elapsed = time.perf_counter() - start_time
    total_size_mb = sum(
        os.path.getsize(os.path.join(output_dir, fn))
        for fn in os.listdir(output_dir) if fn.endswith(".txt")
    ) / (1024 * 1024)

    print(f"[Generator] Selesai dalam {elapsed:.2f} detik! ({num_files} file, Total: {total_size_mb:.2f} MB)")

if __name__ == "__main__":
    generate_dataset(output_dir="dataset", num_files=200, sentences_per_file=1250)