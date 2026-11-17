# Shows how much disk Arkhiva uses, largest first, with a bar per entry.
# Usage, from the repository root:
#   ./scripts/storage.ps1                 (repository + data/ breakdown)
#   ./scripts/storage.ps1 data/parquet    (drill into any folder)
param([string]$Path = (Split-Path $PSScriptRoot -Parent))

function Get-Size($item) {
    if (-not $item.PSIsContainer) { return $item.Length }
    $sum = (Get-ChildItem -LiteralPath $item.FullName -Recurse -File -Force -ErrorAction SilentlyContinue |
        Measure-Object Length -Sum).Sum
    if ($sum) { $sum } else { 0 }
}

function Format-Size([double]$b) {
    if ($b -ge 1GB) { '{0:N2} GB' -f ($b / 1GB) } elseif ($b -ge 1MB) { '{0:N1} MB' -f ($b / 1MB) } else { '{0:N0} KB' -f ($b / 1KB) }
}

function Show-Folder($dir) {
    $rows = Get-ChildItem -LiteralPath $dir -Force | ForEach-Object {
        [pscustomobject]@{ Name = $_.Name + $(if ($_.PSIsContainer) { '/' }); Bytes = Get-Size $_ }
    } | Sort-Object Bytes -Descending
    $total = ($rows | Measure-Object Bytes -Sum).Sum
    Write-Host "`n$((Resolve-Path $dir).Path)  —  total $(Format-Size $total)" -ForegroundColor Cyan
    foreach ($r in $rows) {
        $pct = if ($total) { $r.Bytes / $total } else { 0 }
        $bar = '#' * [math]::Round($pct * 30)
        Write-Host ('  {0,-38} {1,10}  {2,5:P0}  {3}' -f $r.Name, (Format-Size $r.Bytes), $pct, $bar)
    }
}

Show-Folder $Path
$data = Join-Path $Path 'data'
if (Test-Path $data) { Show-Folder $data }
$drive = Get-PSDrive ((Resolve-Path $Path).Drive.Name)
Write-Host "`nDrive $($drive.Name): free $(Format-Size $drive.Free) of $(Format-Size ($drive.Used + $drive.Free))" -ForegroundColor DarkGray
