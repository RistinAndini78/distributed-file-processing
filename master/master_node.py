import os
import json
import time
import socket
import threading
import argparse
import psutil
from collections import Counter, deque
from common.protocol import send_msg, recv_msg

class DistributedMaster:
    def __init__(self, host, port, expected_workers, mode="dynamic", batch_size=5, baseline_time=5.2697):
        self.host = host
        self.port = port
        self.expected_workers = expected_workers
        self.mode = mode  # 'static' atau 'dynamic'
        self.batch_size = batch_size
        self.baseline_time = baseline_time

        self.lock = threading.Lock()
        self.workers = {}          # worker_id -> info dict
        self.task_queue = deque()  # antrean task siap dikerjakan
        self.in_progress = {}      # task_id -> (task_data, worker_id, start_time)
        self.completed_results = []
        self.total_tasks = 0

        # Pencatatan statistik untuk laporan evaluasi (Load Balancing & Fault Tolerance)
        self.worker_task_counts = Counter()
        self.worker_file_counts = Counter()
        self.recovery_events = []
        self.all_workers_ready = threading.Event()
        self.all_tasks_done = threading.Event()

    def load_and_decompose_tasks(self, dataset_dir="dataset"):
        """Task Decomposition: Membaca dataset dan memecahnya menjadi unit task."""
        files = [
            os.path.join(dataset_dir, f)
            for f in sorted(os.listdir(dataset_dir))
            if f.endswith(".txt")
        ]
        if not files:
            raise FileNotFoundError("Dataset tidak ditemukan di folder 'dataset/'.")

        # Muat konten file ke memori Master agar siap dikirim lewat jaringan ke Worker
        loaded_files = []
        for fp in files:
            with open(fp, "r", encoding="utf-8") as f:
                loaded_files.append((os.path.basename(fp), f.read()))

        if self.mode == "static":
            # Pembagian rata statis di awal tanpa mempertimbangkan kecepatan/core Worker
            chunk_size = max(1, len(loaded_files) // self.expected_workers)
            task_id = 1
            for i in range(0, len(loaded_files), chunk_size):
                batch = loaded_files[i:i + chunk_size]
                self.task_queue.append({"task_id": task_id, "files": batch})
                task_id += 1
        else:
            # Dynamic Load Balancing: Granularitas batch kecil (misal 5 file/task)
            # sehingga Worker yang lebih cepat/banyak core otomatis mengambil lebih banyak task
            task_id = 1
            for i in range(0, len(loaded_files), self.batch_size):
                batch = loaded_files[i:i + self.batch_size]
                self.task_queue.append({"task_id": task_id, "files": batch})
                task_id += 1

        self.total_tasks = len(self.task_queue)
        print(f"[Master] Task Decomposition selesai: {len(loaded_files)} file dipecah menjadi {self.total_tasks} task (Mode: {self.mode.upper()}).")
        return len(loaded_files)

    def handle_worker(self, conn, addr):
        """Menangani komunikasi dengan satu Worker node secara konkuren."""
        worker_id = None
        try:
            reg_msg = recv_msg(conn)
            if not reg_msg or reg_msg.get("type") != "REGISTER":
                conn.close()
                return

            worker_id = reg_msg["worker_id"]
            with self.lock:
                self.workers[worker_id] = {
                    "conn": conn,
                    "addr": addr,
                    "cpu_cores": reg_msg["cpu_cores"],
                    "cpu_load": reg_msg["cpu_load"],
                    "status": "IDLE"
                }
                connected_count = len(self.workers)
                print(f"[Master] Worker terhubung ({connected_count}/{self.expected_workers}): {worker_id} [{reg_msg['cpu_cores']} Core, IP: {addr[0]}]")

                if connected_count >= self.expected_workers:
                    self.all_workers_ready.set()

            # Tunggu hingga seluruh target Worker terkoneksi sebelum mulai distribusi
            self.all_workers_ready.wait()

            # Loop distribusi pekerjaan ke Worker ini
            while not self.all_tasks_done.is_set():
                task_to_send = None
                with self.lock:
                    if self.task_queue:
                        # Pada mode dynamic, beri sedikit prioritas jeda bagi worker yang CPU-nya sangat tinggi (>85%)
                        # jika masih ada worker lain yang idle
                        task_to_send = self.task_queue.popleft()
                        self.in_progress[task_to_send["task_id"]] = (task_to_send, worker_id, time.perf_counter())
                        self.workers[worker_id]["status"] = "BUSY"

                if task_to_send is None:
                    # Antrean kosong, cek apakah masih ada task yang sedang dikerjakan worker lain (untuk antisipasi requeue)
                    with self.lock:
                        if not self.task_queue and not self.in_progress:
                            self.all_tasks_done.set()
                            break
                    time.sleep(0.1)
                    continue

                # Kirim task ke Worker
                send_msg(conn, {
                    "type": "TASK_BATCH",
                    "task_id": task_to_send["task_id"],
                    "files": task_to_send["files"]
                })

                # Tunggu hasil dari Worker
                resp = recv_msg(conn)
                if resp is None:
                    raise ConnectionError(f"Koneksi terputus mendadak dari {worker_id}")

                if resp.get("type") == "TASK_RESULT":
                    tid = resp["task_id"]
                    with self.lock:
                        if tid in self.in_progress:
                            del self.in_progress[tid]
                            self.completed_results.extend(resp["results"])
                            self.worker_task_counts[worker_id] += 1
                            self.worker_file_counts[worker_id] += len(resp["results"])
                            self.workers[worker_id]["status"] = "IDLE"
                            self.workers[worker_id]["cpu_load"] = resp.get("cpu_percent", 0)

                            done_tasks = self.total_tasks - len(self.task_queue) - len(self.in_progress)
                            print(f"[Master] Progress: {done_tasks}/{self.total_tasks} task selesai (dari {worker_id})")

                            if not self.task_queue and not self.in_progress:
                                self.all_tasks_done.set()

                # Pada mode static, tiap worker hanya mengambil 1 batch besar di awal lalu menunggu
                if self.mode == "static":
                    self.all_tasks_done.wait()
                    break

        except Exception as e:
            # FAULT TOLERANCE: Deteksi kegagalan Worker dan kembalikan task ke antrean (Requeue)
            failure_time = time.perf_counter()
            print(f"\n[FAULT TOLERANCE] Terdeteksi kegagalan pada {worker_id or addr}: {e}")
            with self.lock:
                requeued_ids = []
                for tid, (tdata, wid, _) in list(self.in_progress.items()):
                    if wid == worker_id:
                        self.task_queue.appendleft(tdata)
                        del self.in_progress[tid]
                        requeued_ids.append(tid)

                if worker_id in self.workers:
                    self.workers[worker_id]["status"] = "FAILED"

                if requeued_ids:
                    recovery_duration = (time.perf_counter() - failure_time) * 1000  # dalam milidetik
                    print(f"[FAULT TOLERANCE] Task {requeued_ids} berhasil di-requeue ke Worker aktif dalam {recovery_duration:.2f} ms!\n")
                    self.recovery_events.append({
                        "failed_worker": worker_id,
                        "requeued_tasks": requeued_ids,
                        "recovery_time_ms": round(recovery_duration, 3)
                    })
        finally:
            try:
                send_msg(conn, {"type": "SHUTDOWN"})
                conn.close()
            except Exception:
                pass

    def start(self):
        total_files = self.load_and_decompose_tasks("dataset")

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(20)

        print(f"[Master] Server berjalan di {self.host}:{self.port}")
        print(f"[Master] Menunggu {self.expected_workers} Worker terhubung untuk memulai eksekusi...\n")

        # Thread penerima koneksi Worker
        def accept_loop():
            while not self.all_tasks_done.is_set():
                try:
                    server.settimeout(1.0)
                    conn, addr = server.accept()
                    t = threading.Thread(target=self.handle_worker, args=(conn, addr), daemon=True)
                    t.start()
                except socket.timeout:
                    continue
                except OSError:
                    break

        accept_thread = threading.Thread(target=accept_loop, daemon=True)
        accept_thread.start()

        # Tunggu seluruh Worker terkoneksi
        self.all_workers_ready.wait()
        print("\n[Master] Semua Worker siap! Memulai pemrosesan terdistribusi...\n")

        psutil.cpu_percent(interval=None)
        start_time = time.perf_counter()

        # Tunggu hingga seluruh task selesai diproses
        self.all_tasks_done.wait()
        exec_time = time.perf_counter() - start_time
        server.close()

        # Hitung metrik performa akhir
        cpu_util = psutil.cpu_percent(interval=0.1)
        mem_mb = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
        throughput = total_files / exec_time
        speedup = self.baseline_time / exec_time
        efficiency = (speedup / self.expected_workers) * 100

        # Agregasi hasil akhir
        category_counts = Counter(r["category"] for r in self.completed_results)
        keyword_totals = Counter()
        for r in self.completed_results:
            for k, v in r["keyword_hits"].items():
                keyword_totals[k] += v

        report = {
            "scenario": f"Distributed ({self.expected_workers} Workers - {self.mode.upper()})",
            "num_workers": self.expected_workers,
            "load_balancing_mode": self.mode,
            "total_files": len(self.completed_results),
            "execution_time_sec": round(exec_time, 4),
            "speedup_vs_sequential": round(speedup, 4),
            "efficiency_percent": round(efficiency, 2),
            "throughput_files_per_sec": round(throughput, 2),
            "master_cpu_percent": round(cpu_util, 2),
            "master_memory_mb": round(mem_mb, 2),
            "load_distribution_files_per_worker": dict(self.worker_file_counts),
            "fault_tolerance_recoveries": self.recovery_events,
            "document_groups": dict(category_counts),
            "keyword_search_results": dict(keyword_totals)
        }

        print("\n" + "=" * 55)
        print(f"HASIL PENGUJIAN TERDISTRIBUSI ({self.expected_workers} WORKER | MODE: {self.mode.upper()})")
        print("=" * 55)
        print(f"Total File Diproses : {report['total_files']}")
        print(f"Waktu Eksekusi      : {report['execution_time_sec']} detik")
        print(f"Speedup (vs Seq)    : {report['speedup_vs_sequential']}x")
        print(f"Efficiency          : {report['efficiency_percent']}%")
        print(f"Throughput          : {report['throughput_files_per_sec']} file/detik")
        print(f"Distribusi Beban    : {report['load_distribution_files_per_worker']}")
        if self.recovery_events:
            print(f"Fault Recovery      : {self.recovery_events}")
        print(f"Kelompok Dokumen    : {report['document_groups']}")
        print("=" * 55)

        os.makedirs("results", exist_ok=True)
        out_file = os.path.join("results", f"distributed_{self.expected_workers}w_{self.mode}.json")
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=4)
        print(f"[Info] Laporan JSON tersimpan di '{out_file}'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Master Node - Distributed File Processing")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="IP Bind Master (0.0.0.0 untuk semua interface jaringan)")
    parser.add_argument("--port", type=int, default=5000, help="Port TCP Master")
    parser.add_argument("--workers", type=int, default=2, help="Jumlah Worker yang ditunggu")
    parser.add_argument("--mode", type=str, choices=["static", "dynamic"], default="dynamic", help="Mode Load Balancing")
    parser.add_argument("--batch", type=int, default=5, help="Ukuran batch file per task pada mode dynamic")
    args = parser.parse_args()

    master = DistributedMaster(
        host=args.host,
        port=args.port,
        expected_workers=args.workers,
        mode=args.mode,
        batch_size=args.batch
    )
    master.start()