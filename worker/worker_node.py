import os
import sys
import time
import socket
import argparse
import psutil
import multiprocessing as mp
from collections import Counter
from common.protocol import send_msg, recv_msg
from common.text_processor import TOKEN_PATTERN, STOPWORDS, DEFAULT_KEYWORDS
from common.dataset_generator import CATEGORIES

def process_text_payload(item):
    """
    Memproses konten teks yang dikirim langsung oleh Master melalui jaringan.
    Dijalankan secara paralel menggunakan multiprocessing pada node Worker.
    """
    filename, raw_text = item
    tokens = TOKEN_PATTERN.findall(raw_text.lower())
    total_words = len(tokens)

    filtered_tokens = [t for t in tokens if t not in STOPWORDS]
    word_freq = Counter(filtered_tokens)

    keyword_hits = {kw: word_freq.get(kw.lower(), 0) for kw in DEFAULT_KEYWORDS}

    category_scores = {}
    for cat_name, vocab in CATEGORIES.items():
        category_scores[cat_name] = sum(word_freq.get(word, 0) for word in vocab)

    assigned_category = max(category_scores, key=category_scores.get)
    top_words = dict(word_freq.most_common(10))

    return {
        "file": filename,
        "total_words": total_words,
        "category": assigned_category,
        "keyword_hits": keyword_hits,
        "top_words": top_words
    }

def start_worker(master_ip, master_port, worker_name, num_cores):
    cpu_cores = num_cores or os.cpu_count() or 2
    hostname = socket.gethostname()
    worker_id = f"{worker_name}@{hostname}"

    print(f"[{worker_id}] Menghubungkan ke Master {master_ip}:{master_port} (Alokasi: {cpu_cores} Core CPU)...")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((master_ip, master_port))

    # Kirim registrasi & spesifikasi kemampuan node ke Master
    cpu_load = psutil.cpu_percent(interval=0.2)
    mem_avail_mb = psutil.virtual_memory().available / (1024 * 1024)
    send_msg(sock, {
        "type": "REGISTER",
        "worker_id": worker_id,
        "cpu_cores": cpu_cores,
        "cpu_load": cpu_load,
        "mem_avail_mb": round(mem_avail_mb, 1)
    })

    print(f"[{worker_id}] Terdaftar! Menunggu distribusi task dari Master...")

    with mp.Pool(processes=cpu_cores) as pool:
        while True:
            msg = recv_msg(sock)
            if msg is None:
                print(f"[{worker_id}] Koneksi ke Master terputus.")
                break

            msg_type = msg.get("type")

            if msg_type == "TASK_BATCH":
                task_id = msg["task_id"]
                files_payload = msg["files"]
                print(f"[{worker_id}] Menerima Task #{task_id} ({len(files_payload)} file) -> Memproses paralel ({cpu_cores} workers)...")

                # Eksekusi paralel menggunakan multicore CPU Worker
                batch_results = pool.map(process_text_payload, files_payload)

                # Kirim hasil beserta metrik beban terkini ke Master
                current_cpu = psutil.cpu_percent(interval=None)
                current_mem = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
                send_msg(sock, {
                    "type": "TASK_RESULT",
                    "worker_id": worker_id,
                    "task_id": task_id,
                    "results": batch_results,
                    "cpu_percent": current_cpu,
                    "mem_mb": round(current_mem, 2)
                })
                print(f"[{worker_id}] Task #{task_id} selesai & dikirim ke Master.")

            elif msg_type == "ROUND_RESET":
                next_info = msg.get("next_round", "berikutnya")
                print(f"[{worker_id}] Ronde selesai! Tetap terhubung & standby untuk sesi: {next_info}...")

            elif msg_type == "SIMULATE_FAIL":
                print(f"[{worker_id}] [SIMULASI FAULT TOLERANCE] Node memutus koneksi mendadak saat sedang memproses task!")
                time.sleep(0.3)
                sock.close()
                os._exit(1)

            elif msg_type == "SHUTDOWN":
                print(f"[{worker_id}] Seluruh sesi benchmark selesai. Worker berhenti.")
                break

    try:
        sock.close()
    except Exception:
        pass

if __name__ == "__main__":
    mp.freeze_support()
    parser = argparse.ArgumentParser(description="Worker Node - Distributed File Processing")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="IP Address Master Node")
    parser.add_argument("--port", type=int, default=5000, help="Port Master Node")
    parser.add_argument("--name", type=str, default="Worker", help="Nama identitas Worker")
    parser.add_argument("--cores", type=int, default=None, help="Jumlah core CPU yang digunakan")
    args = parser.parse_args()

    start_worker(args.host, args.port, args.name, args.cores)