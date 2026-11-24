# =====================================================================
# run_stability_tests.ps1
# Pengujian stabilitas sistem distributed-file-processing (berurutan)
# - Mode dynamic: --workers 1, 2, 4, 6, 9
# - Perbandingan static vs dynamic: 9 workers
# Semua log + file JSON diarsipkan di results/stability_<timestamp>/
# =====================================================================
$ErrorActionPreference = "Continue"

$base = (Get-Location).Path
$env:PYTHONPATH = $base   # agar 'import common' bekerja dari master/ dan worker/
$ts   = Get-Date -Format "yyyyMMdd_HHmmss"
$runDir = "$base\results\stability_$ts"
$null = New-Item -ItemType Directory -Path $runDir -Force

# Bersihkan hasil JSON lama dari eksperimen sebelumnya agar deteksi keberadaan JSON akurat
Get-ChildItem -Path "$base\results" -Filter "distributed_*w_*.json" -File | Remove-Item -Force

function Wait-ProcExit {
    param([int]$id, [int]$timeoutSec = 300)
    $sw = [diagnostics.stopwatch]::StartNew()
    while ($sw.Elapsed.TotalSeconds -lt $timeoutSec) {
        $p = Get-Process -Id $id -ErrorAction SilentlyContinue
        if (-not $p) { return $true }
        Start-Sleep -Seconds 1
    }
    return $false
}

$pythonVer = (& python --version 2>&1) -join " "
$cpuCores  = (& python -c "import os; print(os.cpu_count())" 2>$null)

$runs = @(
    @{ workers = 1; mode = "dynamic" },
    @{ workers = 2; mode = "dynamic" },
    @{ workers = 4; mode = "dynamic" },
    @{ workers = 6; mode = "dynamic" },
    @{ workers = 9; mode = "dynamic" },
    @{ workers = 9; mode = "static"  }
)

$port = 51200
$results = New-Object System.Collections.Generic.List[PSObject]

Write-Host ("Memulai {0} skenario pengujian -> {1}" -f $runs.Count, $runDir)

foreach ($r in $runs) {
    $port++
    $tag = "{0}w_{1}" -f $r.workers, $r.mode
    Write-Host "`n=== RUN $tag --workers $($r.workers) --mode $($r.mode) (port $port) ==="

    $mLog    = Join-Path $runDir ("master_"   + $tag + ".log")
    $mErrLog = Join-Path $runDir ("master_"   + $tag + ".err.log")
    $wLogs   = @()
    $wErrLogs = @()
    $wProcs  = @()
    $status  = "OK"
    $notes   = @()

    try {
        $m = Start-Process python -ArgumentList @("master/master_node.py","--port","$port","--workers","$($r.workers)","--mode","$($r.mode)") -WorkingDirectory $base -RedirectStandardOutput $mLog -RedirectStandardError $mErrLog -PassThru
        Start-Sleep -Seconds 2

        for ($i = 1; $i -le $r.workers; $i++) {
            $wLog   = Join-Path $runDir ("worker" + $i + "_" + $tag + ".log")
            $wErrLog = Join-Path $runDir ("worker" + $i + "_" + $tag + ".err.log")
            $p = Start-Process python -ArgumentList @("worker/worker_node.py","--host","127.0.0.1","--port","$port","--name",("Worker" + $i),"--cores","1") -WorkingDirectory $base -RedirectStandardOutput $wLog -RedirectStandardError $wErrLog -PassThru
            $wProcs  += $p
            $wLogs   += $wLog
            $wErrLogs += $wErrLog
        }

        $allDone = $true
        foreach ($p in $wProcs) {
            if (-not (Wait-ProcExit $p.Id 300)) {
                $allDone = $false
                $notes += "Worker PID $($p.Id) melebihi timeout"
                try { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue } catch {}
            }
        }

        Start-Sleep -Seconds 3
        if (-not $m.HasExited) {
            $notes += "Master masih aktif setelah worker selesai, dihentikan paksa"
            try { Stop-Process -Id $m.Id -Force -ErrorAction SilentlyContinue } catch {}
        }

        $jsonSrc = "$base\results\distributed_$($r.workers)w_$($r.mode).json"
        $jsonDst = Join-Path $runDir ("distributed_" + $tag + ".json")
        if (Test-Path $jsonSrc) {
            Copy-Item $jsonSrc $jsonDst -Force
        } else {
            $status = "JSON_MISSING"
            $notes += "File JSON hasil tidak ditemukan di results/"
        }
    } catch {
        $status = "ERROR"
        $notes += $_
    }

    $null = $results.Add([pscustomobject]@{
        run           = $tag
        workers       = $r.workers
        mode          = $r.mode
        port          = $port
        status        = $status
        master_log    = $mLog
        master_err_log = $mErrLog
        worker_logs   = ($wLogs -join "; ")
        worker_err_logs = ($wErrLogs -join "; ")
        json          = $jsonDst
        notes         = ($notes -join " | ")
    })
}

# Arsipkan baseline untuk referensi perbandingan
$baseJson = "$base\results\baseline_results.json"
if (Test-Path $baseJson) { Copy-Item $baseJson (Join-Path $runDir "baseline_results.json") -Force }

# Ekstrak metrik dari setiap file JSON
$rows = New-Object System.Collections.Generic.List[PSObject]
foreach ($r in $results) {
    if ($r.json -and (Test-Path $r.json)) {
        $j = Get-Content $r.json -Raw | ConvertFrom-Json
        $null = $rows.Add([pscustomobject]@{
            run          = $r.run
            workers      = $j.num_workers
            mode         = $j.load_balancing_mode
            scenario     = $j.scenario
            total_files  = $j.total_files
            time_sec     = $j.execution_time_sec
            speedup      = $j.speedup_vs_sequential
            efficiency   = $j.efficiency_percent
            throughput   = $j.throughput_files_per_sec
            master_cpu   = $j.master_cpu_percent
            master_mem   = $j.master_memory_mb
            recoveries   = ($j.fault_tolerance_recoveries | Measure-Object).Count
            load_distrib = ($j.load_distribution_files_per_worker | Out-String).Trim()
            status       = $r.status
        })
    } else {
        $null = $rows.Add([pscustomobject]@{
            run = $r.run; workers = $r.workers; mode = $r.mode; status = $r.status; notes = $r.notes
        })
    }
}

# Susun SUMMARY.md
$md = @()
$md += "# Ringkasan Pengujian Stabilitas - Distributed File Processing"
$md += ""
$md += "Tanggal pelaksanaan: $(Get-Date)"
$md += "Host: $env:COMPUTERNAME"
$md += "Python: $pythonVer | CPU logical cores: $cpuCores | Dataset: 200 file teks (5.000.000 kata, 4 kategori x 50)"
$md += "Konfigurasi Worker: --cores 1 (1 core per Worker agar beban mesin terkendala dan skala mengikuti --workers)"
$md += "Batch size mode dynamic: 5 file/task (default)"
$md += ""
$md += "## Hasil per skenario"
$md += ""
$md += "| Run | Mode | Workers | Total File | Waktu (detik) | Speedup vs Seq | Efisiensi (%) | Throughput file/detik | Fault Recovery | Status |"
$md += "|-----|------|---------|------------|---------------|----------------|---------------|-----------------------|----------------|--------|"
foreach ($row in $rows) {
    if ($row.status -eq "OK") {
        $md += ("| {0} | {1} | {2} | {3} | {4} | {5}x | {6}% | {7} | {8} | OK |" -f $row.run, $row.mode, $row.workers, $row.total_files, $row.time_sec, $row.speedup, $row.efficiency, $row.throughput, $row.recoveries)
    } else {
        $md += ("| {0} | {1} | {2} | - | - | - | - | - | - | {3}: {4} |" -f $row.run, $row.mode, $row.workers, $row.status, $row.notes)
    }
}
$md += ""
$md += "Catatan: baseline waktu sequential = 5,2697 detik (results/baseline_results.json)."
$md += ""
$md += "## Perbandingan Static vs Dynamic (9 Workers)"
$dyn = $rows | Where-Object { $_.run -eq "9w_dynamic" -and $_.status -eq "OK" }
$sta = $rows | Where-Object { $_.run -eq "9w_static" -and $_.status -eq "OK" }
if ($dyn -and $sta) {
    $md += "| Metrik | Dynamic (9 workers) | Static (9 workers) | Selisih |"
    $md += "|--------|---------------------|--------------------|---------|"
    $md += ("| Waktu eksekusi | {0} detik | {1} detik | {2} detik |" -f $dyn.time_sec, $sta.time_sec, [math]::Round($sta.time_sec - $dyn.time_sec, 4))
    $md += ("| Speedup vs sequential | {0}x | {1}x | - |" -f $dyn.speedup, $sta.speedup)
    $md += ("| Efisiensi | {0}% | {1}% | - |" -f $dyn.efficiency, $sta.efficiency)
    $md += ("| Throughput | {0} file/dtk | {1} file/dtk | - |" -f $dyn.throughput, $sta.throughput)
    $md += ("| Distribusi beban per worker | {0} | {1} | - |" -f $dyn.load_distrib, $sta.load_distrib)
    $md += ""
    if ($dyn.time_sec -lt $sta.time_sec) {
        $md += "Kesimpulan: pada 9 workers, mode **dynamic** lebih cepat dari mode static dan distribusi beban lebih merata (granulasi task kecil)."
    } else {
        $md += "Kesimpulan: pada 9 workers, mode **static** lebih cepat dari mode dynamic pada pengujian ini."
    }
} else {
    $md += "Data perbandingan static/dynamic 9 workers tidak tersedia lengkap."
}
$md += ""
$md += "## File hasil (arsip)"
$md += "Semua log master/worker dan file JSON berada di: `"$runDir`""
$md += ""
Get-ChildItem $runDir | ForEach-Object { $md += "- $($_.Name)" }

$summaryPath = Join-Path $runDir "SUMMARY.md"
$md -join "`n" | Set-Content $summaryPath -Encoding UTF8

$csvPath = Join-Path $runDir "results_summary.csv"
$rows | Export-Csv -Path $csvPath -NoTypeInformation -Encoding utf8

Write-Host "`n=== RINGKASAN HASIL ==="
$rows | Format-Table -AutoSize
Write-Host ""
Write-Host "Ringkasan lengkap : $summaryPath"
Write-Host "Folder hasil      : $runDir"
