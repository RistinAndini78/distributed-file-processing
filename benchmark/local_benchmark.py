import os
import json
import time
import psutil
import multiprocessing as mp
from collections import Counter
from common.text_processor import process_single_file, DEFAULT_KEYWORDS

def get_dataset_files(dataset_dir="dataset"):
    files = [
        os.path.join(dataset_dir, f)
        for f in sorted(os.listdir(dataset_dir))
        if f.endswith(".txt")
    ]
    if not files:
        raise FileNotFoundError(f"Folder '{dataset_dir}' kosong. Jalankan dataset_generator terlebih dahulu.")
    return files

def aggregate_results(results):
    """Menggabungkan hasil pemrosesan dari seluruh file."""
    total_words = 0
    category_counts = Counter()
    total_keyword_hits = Counter()
    global_top_words = Counter()

    for res in results:
        total_words += res["total_words"]
        category_counts[res["category"]] += 1
        for kw, count in res["keyword_hits"].items():
            total_keyword_hits[kw] += count
        for word, count in res["top_words"].items():
            global_top_words[word] += count

    return {
        "total_files_processed": len(results),
        "total_words_processed": total_words,
        "document_groups": dict(category_counts),
        "keyword_search_results": dict(total_keyword_hits),
        "top_10_words": dict(global_top_words.most_common(10))
    }

def run_sequential(files):
    """Skenario 3a: Sequential Processing (1 proses sebagai baseline)."""
    process = psutil.Process(os.getpid())
    psutil.cpu_percent(interval=None)  # Reset pembacaan CPU

    start_time = time.perf_counter()
    results = [process_single_file(f, DEFAULT_KEYWORDS) for f in files]
    exec_time = time.perf_counter() - start_time

    cpu_util = psutil.cpu_percent(interval=0.1)
    mem_mb = process.memory_info().rss / (1024 * 1024)
    throughput = len(files) / exec_time

    return {
        "mode": "Sequential (1 Proses)",
        "workers": 1,
        "execution_time_sec": round(exec_time, 4),
        "speedup": 1.0,
        "efficiency_percent": 100.0,
        "throughput_files_per_sec": round(throughput, 2),
        "cpu_utilization_percent": round(cpu_util, 2),
        "memory_usage_mb": round(mem_mb, 2),
        "summary": aggregate_results(results)
    }

def run_local_parallel(files, baseline_time, num_processes=None):
    """Skenario 3b: Local Parallel Processing (multiprocessing pada 1 komputer)."""
    if num_processes is None:
        num_processes = os.cpu_count() or 4

    process = psutil.Process(os.getpid())
    psutil.cpu_percent(interval=None)

    start_time = time.perf_counter()
    with mp.Pool(processes=num_processes) as pool:
        results = pool.map(process_single_file, files)
    exec_time = time.perf_counter() - start_time

    cpu_util = psutil.cpu_percent(interval=0.1)
    mem_mb = process.memory_info().rss / (1024 * 1024)
    throughput = len(files) / exec_time
    speedup = baseline_time / exec_time
    efficiency = (speedup / num_processes) * 100

    return {
        "mode": f"Local Parallel ({num_processes} Core)",
        "workers": num_processes,
        "execution_time_sec": round(exec_time, 4),
        "speedup": round(speedup, 4),
        "efficiency_percent": round(efficiency, 2),
        "throughput_files_per_sec": round(throughput, 2),
        "cpu_utilization_percent": round(cpu_util, 2),
        "memory_usage_mb": round(mem_mb, 2),
        "summary": aggregate_results(results)
    }

def main():
    files = get_dataset_files("dataset")
    print(f"=== PENGUJIAN BASELINE: SEQUENTIAL VS LOCAL PARALLEL ({len(files)} File) ===\n")

    # 1. Jalankan Sequential
    print("[1/2] Menjalankan Sequential Processing...")
    seq_metrics = run_sequential(files)
    print(f"      Waktu Eksekusi : {seq_metrics['execution_time_sec']} detik")
    print(f"      Throughput     : {seq_metrics['throughput_files_per_sec']} file/detik")
    print(f"      CPU / Memory   : {seq_metrics['cpu_utilization_percent']}% / {seq_metrics['memory_usage_mb']} MB\n")

    # 2. Jalankan Local Parallel
    cores = os.cpu_count() or 4
    print(f"[2/2] Menjalankan Local Parallel Processing ({cores} logical cores)...")
    par_metrics = run_local_parallel(files, seq_metrics["execution_time_sec"], cores)
    print(f"      Waktu Eksekusi : {par_metrics['execution_time_sec']} detik")
    print(f"      Speedup        : {par_metrics['speedup']}x")
    print(f"      Efficiency     : {par_metrics['efficiency_percent']}%")
    print(f"      Throughput     : {par_metrics['throughput_files_per_sec']} file/detik")
    print(f"      CPU / Memory   : {par_metrics['cpu_utilization_percent']}% / {par_metrics['memory_usage_mb']} MB\n")

    # Tampilkan ringkasan hasil pemrosesan teks
    summary = par_metrics["summary"]
    print("=== HASIL ANALISIS DOKUMEN ===")
    print(f"Total Kata Diproses : {summary['total_words_processed']:,}")
    print(f"Pengelompokan Dokumen : {summary['document_groups']}")
    print(f"Pencarian Kata Kunci  : {summary['keyword_search_results']}")
    print(f"Top 10 Frekuensi Kata : {summary['top_10_words']}\n")

    # Simpan hasil ke folder results/ untuk data BAB IV Laporan
    os.makedirs("results", exist_ok=True)
    output_path = os.path.join("results", "baseline_results.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({"sequential": seq_metrics, "local_parallel": par_metrics}, f, indent=4)

    print(f"[Info] Hasil pengujian otomatis tersimpan di '{output_path}'.")

if __name__ == "__main__":
    mp.freeze_support()
    main()