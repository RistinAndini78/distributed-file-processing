# Ringkasan Pengujian Stabilitas - Distributed File Processing

Tanggal pelaksanaan: 11/24/2026 14:52:10
Host: DESKTOP-L081B77
Python: Python 3.14.3 | CPU logical cores: 4 | Dataset: 200 file teks (5.000.000 kata, 4 kategori x 50)
Konfigurasi Worker: --cores 1 (1 core per Worker agar beban mesin terkendala dan skala mengikuti --workers)
Batch size mode dynamic: 5 file/task (default)

## Hasil per skenario

| Run | Mode | Workers | Total File | Waktu (detik) | Speedup vs Seq | Efisiensi (%) | Throughput file/detik | Fault Recovery | Status |
|-----|------|---------|------------|---------------|----------------|---------------|-----------------------|----------------|--------|

Catatan: baseline waktu sequential = 5,2697 detik (results/baseline_results.json).

## Perbandingan Static vs Dynamic (9 Workers)
Data perbandingan static/dynamic 9 workers tidak tersedia lengkap.

## File hasil (arsip)
Semua log master/worker dan file JSON berada di: "F:\Distributed-File-Processing\results\stability_20261124_145138"

- baseline_results.json
- distributed_2w_dynamic.json
- distributed_9w_dynamic.json
