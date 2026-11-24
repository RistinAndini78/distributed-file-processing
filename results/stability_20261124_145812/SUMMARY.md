# Ringkasan Pengujian Stabilitas - Distributed File Processing

Tanggal pelaksanaan: 11/24/2026 15:44:21
Host: DESKTOP-L081B77
Python: Python 3.14.3 | CPU logical cores: 4 | Dataset: 200 file teks (5.000.000 kata, 4 kategori x 50)
Konfigurasi Worker: --cores 1 (1 core per Worker agar beban mesin terkendala dan skala mengikuti --workers)
Batch size mode dynamic: 5 file/task (default)

## Hasil per skenario

| Run | Mode | Workers | Total File | Waktu (detik) | Speedup vs Seq | Efisiensi (%) | Throughput file/detik | Fault Recovery | Status |
|-----|------|---------|------------|---------------|----------------|---------------|-----------------------|----------------|--------|
| 1w_dynamic | dynamic | 1 | 200 | 4.1922 | 1.257x | 125.7% | 47.71 | 0 | OK |
| 2w_dynamic | dynamic | 2 | 200 | 2.833 | 1.8601x | 93.01% | 70.6 | 0 | OK |
| 4w_dynamic | dynamic | 4 | 200 | 2.0769 | 2.5373x | 63.43% | 96.3 | 0 | OK |
| 6w_dynamic | dynamic | 6 | 200 | 1.9392 | 2.7174x | 45.29% | 103.13 | 0 | OK |
| 9w_dynamic | dynamic | 9 | 200 | 2.2094 | 2.3851x | 26.5% | 90.52 | 0 | OK |
| 9w_static | static | 9 | - | - | - | - | - | - | JSON_MISSING: Worker PID 9368 melebihi timeout | Worker PID 17268 melebihi timeout | Worker PID 21404 melebihi timeout | Worker PID 20884 melebihi timeout | Worker PID 19144 melebihi timeout | Worker PID 14944 melebihi timeout | Worker PID 15088 melebihi timeout | Worker PID 14148 melebihi timeout | Worker PID 15428 melebihi timeout | Master masih aktif setelah worker selesai, dihentikan paksa | File JSON hasil tidak ditemukan di results/ |

Catatan: baseline waktu sequential = 5,2697 detik (results/baseline_results.json).

## Perbandingan Static vs Dynamic (9 Workers)

| Metrik | Dynamic (9 workers) | Static (9 workers) | Keterangan |
|--------|---------------------|--------------------|------------|
| Waktu eksekusi | 2,2094 detik | GAGAL (deadlock) | Static tidak menghasilkan JSON |
| Speedup vs sequential | 2,3851x | - | - |
| Efisiensi | 26,5% | - | - |
| Throughput | 90,52 file/dtk | - | - |
| Fault Recovery | 0 | - | - |
| Distribusi beban | 30/25/25/25/30/30/25/5/5 | - | - |

Kesimpulan: pada 9 workers, mode **dynamic** berhasil menyelesaikan 200 file dalam 2,2094 detik (speedup 2,3851x). Mode **static** mengalami **deadlock permanen** — static membagi 200 file menjadi 10 task (chunk ~22), tetapi setiap worker hanya mengambil 1 task lalu berhenti, sehingga sisanya 1 task tidak pernah diambil dan `all_tasks_done` tidak pernah terpenuhi. Master menunggu selamanya dan tidak menulis file JSON. Oleh karena itu, mode static **tidak stabil** pada konfigurasi 9 workers pada mesin ini (4 logical cores).

## File hasil (arsip)
Semua log master/worker dan file JSON berada di: "F:\Distributed-File-Processing\results\stability_20261124_145812"

- baseline_results.json
- distributed_1w_dynamic.json
- distributed_2w_dynamic.json
- distributed_4w_dynamic.json
- distributed_6w_dynamic.json
- distributed_9w_dynamic.json
- master_1w_dynamic.err.log
- master_1w_dynamic.log
- master_2w_dynamic.err.log
- master_2w_dynamic.log
- master_4w_dynamic.err.log
- master_4w_dynamic.log
- master_6w_dynamic.err.log
- master_6w_dynamic.log
- master_9w_dynamic.err.log
- master_9w_dynamic.log
- master_9w_static.err.log
- master_9w_static.log
- worker1_1w_dynamic.err.log
- worker1_1w_dynamic.log
- worker1_2w_dynamic.err.log
- worker1_2w_dynamic.log
- worker1_4w_dynamic.err.log
- worker1_4w_dynamic.log
- worker1_6w_dynamic.err.log
- worker1_6w_dynamic.log
- worker1_9w_dynamic.err.log
- worker1_9w_dynamic.log
- worker1_9w_static.err.log
- worker1_9w_static.log
- worker2_2w_dynamic.err.log
- worker2_2w_dynamic.log
- worker2_4w_dynamic.err.log
- worker2_4w_dynamic.log
- worker2_6w_dynamic.err.log
- worker2_6w_dynamic.log
- worker2_9w_dynamic.err.log
- worker2_9w_dynamic.log
- worker2_9w_static.err.log
- worker2_9w_static.log
- worker3_4w_dynamic.err.log
- worker3_4w_dynamic.log
- worker3_6w_dynamic.err.log
- worker3_6w_dynamic.log
- worker3_9w_dynamic.err.log
- worker3_9w_dynamic.log
- worker3_9w_static.err.log
- worker3_9w_static.log
- worker4_4w_dynamic.err.log
- worker4_4w_dynamic.log
- worker4_6w_dynamic.err.log
- worker4_6w_dynamic.log
- worker4_9w_dynamic.err.log
- worker4_9w_dynamic.log
- worker4_9w_static.err.log
- worker4_9w_static.log
- worker5_6w_dynamic.err.log
- worker5_6w_dynamic.log
- worker5_9w_dynamic.err.log
- worker5_9w_dynamic.log
- worker5_9w_static.err.log
- worker5_9w_static.log
- worker6_6w_dynamic.err.log
- worker6_6w_dynamic.log
- worker6_9w_dynamic.err.log
- worker6_9w_dynamic.log
- worker6_9w_static.err.log
- worker6_9w_static.log
- worker7_9w_dynamic.err.log
- worker7_9w_dynamic.log
- worker7_9w_static.err.log
- worker7_9w_static.log
- worker8_9w_dynamic.err.log
- worker8_9w_dynamic.log
- worker8_9w_static.err.log
- worker8_9w_static.log
- worker9_9w_dynamic.err.log
- worker9_9w_dynamic.log
- worker9_9w_static.err.log
- worker9_9w_static.log
