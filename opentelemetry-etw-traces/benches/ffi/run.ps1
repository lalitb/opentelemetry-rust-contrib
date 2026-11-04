param(
    [ValidateSet('check','latency','memory')][string]$Mode = 'latency',
    [switch]$Smoke
)
$ErrorActionPreference = 'Stop'
$build = Join-Path $PSScriptRoot 'build'
$exe = Join-Path $build 'bin\driver.exe'
$capture = Join-Path $build 'bin\capture.exe'
$revision = (Get-Content (Join-Path $build 'revision.txt') -Raw).Trim()
function Check-Build {
    foreach ($manifest in @('build-sources.csv','build-binaries.csv')) {
        foreach ($entry in (Import-Csv (Join-Path $build $manifest))) {
            if ((Get-FileHash -LiteralPath $entry.Path).Hash -ne $entry.Hash) {
                throw "Source/binary differs from compiled manifest: $($entry.Path)"
            }
        }
    }
}
function Environment-Record {
    $system = Get-CimInstance Win32_ComputerSystem
    $os = Get-CimInstance Win32_OperatingSystem
    return [ordered]@{
        captured_at = (Get-Date -Format o)
        cpu = @(Get-CimInstance Win32_Processor | Select-Object Name,NumberOfCores,
                    NumberOfLogicalProcessors,MaxClockSpeed)
        manufacturer = $system.Manufacturer
        model = $system.Model
        ram_bytes = [uint64]$system.TotalPhysicalMemory
        os = $os.Caption
        os_version = $os.Version
        os_build = $os.BuildNumber
        affinity_mask = 1
        etw_mode = 'file, PID-filtered'
        etw_buffer_kib = 64
        etw_min_buffers = 2048
        etw_max_buffers = 4096
        etw_clock = 'perf'
        mode = $Mode
        warmup_per_batch = if ($Mode -eq 'latency') { 1000 } else { $null }
        timed_per_batch = if ($Mode -eq 'latency') { 10000 } else { $null }
        batches_per_process = if ($Mode -eq 'latency') { 7 } else { $null }
    }
}
Check-Build
$suffix = if ($Smoke) { '-smoke' } else { '' }
$root = Join-Path $PSScriptRoot ('results\' + (Get-Date -Format 'yyyyMMdd-HHmmss-fff') + "-$Mode$suffix")
New-Item -ItemType Directory $root | Out-Null
$root | Set-Content (Join-Path $build "latest-$Mode$suffix.txt")
Environment-Record | ConvertTo-Json -Depth 6 | Set-Content (Join-Path $root 'environment-before.json')
foreach ($file in @('Cargo.lock','Cargo.toml','run.ps1')) {
    Copy-Item (Join-Path $PSScriptRoot $file) $root
}
foreach ($file in @('compiler.txt','build-sources.csv','build-binaries.csv','revision.txt')) {
    Copy-Item (Join-Path $build $file) $root
}
$orders = @(
    @('native','wrapper','cpp'), @('wrapper','cpp','native'),
    @('cpp','native','wrapper'), @('cpp','wrapper','native'),
    @('wrapper','native','cpp'), @('native','cpp','wrapper')
)
$passes = if ($Smoke -or $Mode -eq 'check') { 1 } elseif ($Mode -eq 'latency') { 6 } else { 3 }
try {
    for ($pass = 0; $pass -lt $passes; ++$pass) {
        $workloads = if ($pass % 2) { @(3,0) } else { @(0,3) }
        foreach ($attributes in $workloads) {
            $variants = if ($Mode -eq 'check') { @('cpp') } else { $orders[$pass] }
            foreach ($variant in $variants) {
                $output = Join-Path $root "pass$pass-attrs$attributes-$variant"
                New-Item -ItemType Directory $output | Out-Null
                $ready = Join-Path $output 'ready.json'
                $gate = Join-Path $output 'go'
                $etl = Join-Path $output 'events.etl'
                $session = "EtwFfi-$PID-$Mode-$pass-$attributes-$variant"
                $process = Start-Process $exe `
                    -ArgumentList @($Mode, $variant, $attributes, "`"$ready`"", "`"$gate`"") `
                    -PassThru -NoNewWindow `
                    -RedirectStandardOutput (Join-Path $output 'output.json') `
                    -RedirectStandardError (Join-Path $output 'diagnostics.txt')
                $null = $process.Handle
                $started = $false
                try {
                    $deadline = (Get-Date).AddSeconds(30)
                    do {
                        if ($process.HasExited) { throw 'Benchmark exited before ready' }
                        Start-Sleep -Milliseconds 100
                        if ((Get-Date) -gt $deadline) { throw 'Readiness timed out' }
                    } until (Test-Path $ready)
                    $metadata = Get-Content -Raw $ready | ConvertFrom-Json
                    if ($metadata.revision -ne $revision -or
                        $metadata.attribute_count -ne $attributes -or $metadata.variant -ne $variant) {
                        throw 'Unexpected revision/workload'
                    }
                    $process.ProcessorAffinity = [IntPtr]1
                    & logman create trace $session -bs 64 -nb 2048 4096 -ct perf -o $etl -ow -ets |
                        Set-Content (Join-Path $output 'logman.txt')
                    if ($LASTEXITCODE -ne 0) { throw 'logman failed' }
                    $started = $true
                    & $capture enable $session $process.Id $metadata.provider_guid
                    if ($LASTEXITCODE -ne 0) { throw 'Provider enable failed' }
                    Start-Sleep -Milliseconds 250
                    New-Item -ItemType File $gate | Out-Null
                    if (!$process.WaitForExit(120000)) { throw 'Benchmark timed out' }
                    $process.WaitForExit()
                    $process.Refresh()
                    if ($process.ExitCode -ne 0) { throw "Benchmark failed; see $output\diagnostics.txt" }
                } finally {
                    if (!$process.HasExited) { Stop-Process -Id $process.Id }
                    if ($started) {
                        & $capture stop $session | Set-Content (Join-Path $output 'loss.json')
                        if ($LASTEXITCODE -ne 0) { throw 'ETW stop failed' }
                    }
                }
                & $capture inspect $etl $process.Id $attributes $metadata.provider_guid `
                    2> (Join-Path $output 'decode-diagnostics.txt') |
                    Set-Content (Join-Path $output 'events.json')
                if ($LASTEXITCODE -ne 0) { throw 'Payload validation failed' }
                $loss = Get-Content -Raw (Join-Path $output 'loss.json') | ConvertFrom-Json
                $events = Get-Content -Raw (Join-Path $output 'events.json') | ConvertFrom-Json
                $result = Get-Content -Raw (Join-Path $output 'output.json') | ConvertFrom-Json
                if ($events.validated_events -ne $metadata.expected_events -or
                    $events.unique_span_ids -ne $metadata.expected_spans -or
                    $result.expected_spans -ne $metadata.expected_spans -or
                    $result.affinity_mask -ne 1 -or
                    $loss.events_lost -or $loss.log_buffers_lost -or $loss.realtime_buffers_lost) {
                    throw 'Capture count/loss/affinity validation failed'
                }
                "$Mode $variant attributes=$attributes pass=$pass completed"
            }
        }
    }
    Check-Build
    Environment-Record | ConvertTo-Json -Depth 6 | Set-Content (Join-Path $root 'environment-after.json')
    "Accepted result: $root"
} catch {
    $_ | Out-String | Set-Content (Join-Path $root 'error.txt')
    throw
}
