$base = (Get-Location).Path
$mLog = "$base\test_master.log"
$mErr = "$base\test_master.err.log"
$m = Start-Process python -ArgumentList @("master/master_node.py","--port","51301","--workers","1","--mode","dynamic") -WorkingDirectory $base -Environment @{PYTHONPATH = $base} -RedirectStandardOutput $mLog -RedirectStandardError $mErr -PassThru
"process id: $($m.Id)"
Start-Sleep -Seconds 3
"has exited: $($m.HasExited)"
"log exists: $(Test-Path $mLog)"
"err exists: $(Test-Path $mErr)"
if (-not $m.HasExited) { Stop-Process -Id $m.Id -Force }
"--- log content ---"
Get-Content $mLog -TotalCount 20 -ErrorAction SilentlyContinue
"--- err content ---"
Get-Content $mErr -TotalCount 20 -ErrorAction SilentlyContinue
