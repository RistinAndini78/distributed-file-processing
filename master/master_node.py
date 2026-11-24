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
        self.mode = mode
        self.batch_size = batch_size
        self.baseline_time = baseline_time

        self.lock = threading.Lock()
        self.workers = {}              # worker_id -> info dict
        self.worker_order = []         # urutan masuk worker_id
        self.active_worker_ids = set() # worker yang aktif pada ronde saat ini
        self.task_queue = deque()
        self.in_progress = {}
        self.completed_results = []
        self.total_tasks = 0

        self.worker_task_counts = Counter()
        self.worker_file_counts = Counter()
        self.recovery_events = []

        self.all_workers_ready = threading.Event()
        self.round_start_event = threading.Event()
        self.round_done_event = threading.Event()
        self.shutdown_all = False

        self.current_mode = mode
        self.current_target_workers = expected_workers
        self.simulate_fault = False
        self.fault_victims = set()
        self.fault_triggered_count = 0

    def load_and_decompose_tasks(self, dataset_dir="dataset", mode="dynamic", target_workers=1):
        """Task Decomposition: Membaca dataset dan memecahnya menjadi unit task."""
        files = [
            os.path.join(dataset_dir, f)
            for f in sorted(os.listdir(dataset_dir))
            if f.endswith(".txt")
        ]
        if not files:
            raise FileNotFoundError("Dataset tidak ditemukan di folder 'dataset/'.")

        loaded_files = []
        for fp in files:
            with open(fp, "r", encoding="utf-8") as f:
                loaded_files.append((os.path.basename(fp), f.read()))

        self.task_queue.clear()
        self.in_progress.clear()
        self.completed_results.clear()
        self.worker_task_counts.clear()
        self.worker_file_counts.clear()
        self.recovery_events.clear()

        if mode == "static":
            chunk_size = max(1, len(loaded_files) // target_workers)
            task_id = 1
            for i in range(0, len(loaded_files), chunk_size):
                batch = loaded_files[i:i + chunk_size]
                self.task_queue.append({"task_id": task_id, "files": batch})
                task_id += 1
        else:
            task_id = 1
            for i in range(0, len(loaded_files), self.batch_size):
                batch = loaded_files[i:i + self.batch_size]
                self.task_queue.append({"task_id": task_id, "files": batch})
                task_id += 1

        self.total_tasks = len(self.task_queue)
        print(f"[Master] Task Decomposition: {len(loaded_files)} file -> {self.total_tasks} task (Mode: {mode.upper()}, Worker Aktif: {target_workers}).")
        return len(loaded_files)

    def handle_worker(self, conn, addr):
        """Menangani komunikasi satu Worker node sepanjang seluruh ronde pengujian."""
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
                self.worker_order.append(worker_id)
                connected_count = len(self.workers)
                print(f"[Master] Worker terhubung ({connected_count}/{self.expected_workers}): {worker_id} [{reg_msg['cpu_cores']} Core, IP: {addr[0]}]")

                if connected_count >= self.expected_workers:
                    self.all_workers_ready.set()

            while not self.shutdown_all:
                # Tunggu sinyal mulai ronde dari Master
                self.round_start_event.wait()
                if self.shutdown_all:
                    break

                # Jika worker ini tidak masuk kuota ronde ini (misal tes 1w, 2w, 4w), tunggu ronde selesai
                if worker_id not in self.active_worker_ids:
                    self.round_done_event.wait()
                    time.sleep(0.05)
                    continue

                # Loop pengerjaan task pada ronde aktif
                while not self.round_done_event.is_set() and not self.shutdown_all:
                    task_to_send = None
                    trigger_fail_now = False

                    with self.lock:
                        if self.task_queue:
                            task_to_send = self.task_queue.popleft()
                            self.in_progress[task_to_send["task_id"]] = (task_to_send, worker_id, time.perf_counter())
                            self.workers[worker_id]["status"] = "BUSY"

                            # Picu simulasi mati setelah task ke-10 berjalan pada ronde Fault Tolerance
                            done_so_far = self.total_tasks - len(self.task_queue) - len(self.in_progress)
                            if self.simulate_fault and worker_id in self.fault_victims and done_so_far >= 10:
                                trigger_fail_now = True
                                self.fault_victims.remove(worker_id)

                    if task_to_send is None:
                        with self.lock:
                            if not self.task_queue and not self.in_progress:
                                self.round_done_event.set()
                                break
                        time.sleep(0.05)
                        continue

                    if trigger_fail_now:
                        print(f"[Master] Memicu simulasi kegagalan pada {worker_id} saat memegang Task #{task_to_send['task_id']}...")
                        send_msg(conn, {"type": "SIMULATE_FAIL"})
                        raise ConnectionError(f"Simulasi kegagalan node {worker_id} (Fault Tolerance Test)")

                    send_msg(conn, {
                        "type": "TASK_BATCH",
                        "task_id": task_to_send["task_id"],
                        "files": task_to_send["files"]
                    })

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
                                    self.round_done_event.set()

                    if self.current_mode == "static":
                        self.round_done_event.wait()
                        break

                # Tunggu sinkronisasi antar-ronde sebelum lanjut
                self.round_done_event.wait()
                time.sleep(0.05)

        except Exception as e:
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
                if worker_id in self.active_worker_ids:
                    self.active_worker_ids.remove(worker_id)

                if requeued_ids:
                    recovery_duration = (time.perf_counter() - failure_time) * 1000
                    print(f"[FAULT TOLERANCE] Task {requeued_ids} berhasil di-requeue ke Worker aktif dalam {recovery_duration:.2f} ms!\n")
                    self.recovery_events.append({
                        "failed_worker": worker_id,
                        "requeued_tasks": requeued_ids,
                        "recovery_time_ms": round(recovery_duration, 3)
                    })

                if not self.task_queue and not self.in_progress:
                    self.round_done_event.set()
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def run_single_round(self, target_workers, mode, label, out_filename, simulate_fault=False):
        print("\n" + "#" * 60)
        print(f"MEMULAI RONDE: {label}")
        print("#" * 60)

        total_files = self.load_and_decompose_tasks("dataset", mode=mode, target_workers=target_workers)
        self.current_mode = mode
        self.current_target_workers = target_workers
        self.simulate_fault = simulate_fault

        with self.lock:
            alive_workers = [w for w in self.worker_order if self.workers[w]["status"] != "FAILED"]
            selected = alive_workers[:target_workers]
            self.active_worker_ids = set(selected)
            if simulate_fault and len(selected) >= 4:
                # Pilih 2 worker terakhir untuk disimulasikan mati di tengah jalan
                self.fault_victims = set(selected[-2:])
            else:
                self.fault_victims = set()

        self.round_done_event.clear()
        psutil.cpu_percent(interval=None)
        start_time = time.perf_counter()
        self.round_start_event.set()

        self.round_done_event.wait()
        self.round_start_event.clear()
        exec_time = time.perf_counter() - start_time

        cpu_util = psutil.cpu_percent(interval=0.1)
        mem_mb = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
        throughput = total_files / exec_time
        speedup = self.baseline_time / exec_time
        efficiency = (speedup / target_workers) * 100

        category_counts = Counter(r["category"] for r in self.completed_results)
        keyword_totals = Counter()
        for r in self.completed_results:
            for k, v in r["keyword_hits"].items():
                keyword_totals[k] += v

        report = {
            "scenario": label,
            "num_workers": target_workers,
            "load_balancing_mode": mode,
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
        print(f"HASIL: {label}")
        print("=" * 55)
        print(f"Total File Diproses : {report['total_files']}")
        print(f"Waktu Eksekusi      : {report['execution_time_sec']} detik")
        print(f"Speedup (vs Seq)    : {report['speedup_vs_sequential']}x")
        print(f"Efficiency          : {report['efficiency_percent']}%")
        print(f"Throughput          : {report['throughput_files_per_sec']} file/detik")
        print(f"Distribusi Beban    : {report['load_distribution_files_per_worker']}")
        if self.recovery_events:
            print(f"Fault Recovery      : {report['fault_tolerance_recoveries']}")
        print(f"Kelompok Dokumen    : {report['document_groups']}")
        print("=" * 55)

        os.makedirs("results", exist_ok=True)
        out_path = os.path.join("results", out_filename)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=4)
        print(f"[Info] Tersimpan di '{out_path}'.")
        return report

    def notify_round_reset(self, next_label):
        with self.lock:
            for wid, winfo in self.workers.items():
                if winfo["status"] != "FAILED":
                    try:
                        send_msg(winfo["conn"], {"type": "ROUND_RESET", "next_round": next_label})
                    except Exception:
                        pass
        time.sleep(0.5)

    def start(self, run_suite=False):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(20)

        print(f"[Master] Server berjalan di {self.host}:{self.port} (1 Port Terpadu)")
        print(f"[Master] Menunggu {self.expected_workers} Worker terhubung untuk memulai eksekusi...\n")

        def accept_loop():
            while not self.shutdown_all:
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

        self.all_workers_ready.wait()
        print("\n[Master] Seluruh Worker terhubung! Memulai pengujian...\n")

        if not run_suite:
            self.run_single_round(
                target_workers=self.expected_workers,
                mode=self.mode,
                label=f"Distributed ({self.expected_workers} Workers - {self.mode.upper()})",
                out_filename=f"distributed_{self.expected_workers}w_{self.mode}.json"
            )
        else:
            # Jalankan 7 skenario lengkap di 1 port (5000) dalam 1 sesi!
            rounds = [
                (1, "dynamic", "Scalability 1 Worker (Dynamic)", "distributed_1w_dynamic.json", False),
                (2, "dynamic", "Scalability 2 Workers (Dynamic)", "distributed_2w_dynamic.json", False),
                (4, "dynamic", "Scalability 4 Workers (Dynamic)", "distributed_4w_dynamic.json", False),
                (5, "dynamic", "Scalability 5 Workers (Dynamic)", "distributed_5w_dynamic.json", False),
                (9, "dynamic", "Scalability 9 Workers (Dynamic)", "distributed_9w_dynamic.json", False),
                (9, "static",  "Load Balancing Comparison 9 Workers (Static)", "distributed_9w_static.json", False),
                (9, "dynamic", "Fault Tolerance Test 9 Workers (2 Nodes Fail)", "distributed_9w_fault_tolerance.json", True),
            ]
            summary = []
            for idx, (w_count, r_mode, r_label, r_file, r_fault) in enumerate(rounds):
                rep = self.run_single_round(w_count, r_mode, r_label, r_file, simulate_fault=r_fault)
                summary.append({
                    "scenario": r_label,
                    "workers": w_count,
                    "mode": r_mode,
                    "time_sec": rep["execution_time_sec"],
                    "speedup": rep["speedup_vs_sequential"],
                    "efficiency_percent": rep["efficiency_percent"],
                    "throughput": rep["throughput_files_per_sec"]
                })
                if idx < len(rounds) - 1:
                    self.notify_round_reset(rounds[idx + 1][2])

            with open(os.path.join("results", "full_uas_benchmark_summary.json"), "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=4)

            print("\n" + "*" * 65)
            print("SELURUH 7 SKENARIO UJI UAS SELESAI DALAM 1 PORT (5000)!")
            print("*" * 65)
            for s in summary:
                print(f"- {s['scenario']:<46} | {s['time_sec']}s | Speedup: {s['speedup']}x | {s['throughput']} file/s")
            print("*" * 65)

        # Matikan semua worker yang masih tersisa
        self.shutdown_all = True
        self.round_start_event.set()
        with self.lock:
            for wid, winfo in self.workers.items():
                try:
                    send_msg(winfo["conn"], {"type": "SHUTDOWN"})
                    winfo["conn"].close()
                except Exception:
                    pass
        server.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Master Node - Distributed File Processing")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="IP Bind Master")
    parser.add_argument("--port", type=int, default=5000, help="Port TCP Master")
    parser.add_argument("--workers", type=int, default=9, help="Jumlah Worker yang ditunggu")
    parser.add_argument("--mode", type=str, choices=["static", "dynamic"], default="dynamic", help="Mode Load Balancing")
    parser.add_argument("--batch", type=int, default=5, help="Ukuran batch file per task pada mode dynamic")
    parser.add_argument("--suite", action="store_true", help="Jalankan seluruh 7 skenario UAS otomatis dalam 1 sesi")
    args = parser.parse_args()

    master = DistributedMaster(
        host=args.host,
        port=args.port,
        expected_workers=args.workers,
        mode=args.mode,
        batch_size=args.batch
    )
    master.start(run_suite=args.suite)