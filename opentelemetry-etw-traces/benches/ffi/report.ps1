$ErrorActionPreference = 'Stop'
function Median($values) {
    $sorted = @($values | Sort-Object)
    if (!$sorted.Count) { throw 'Empty samples' }
    $mid = [int][Math]::Floor($sorted.Count / 2)
    if ($sorted.Count % 2) { return [double]$sorted[$mid] }
    return ([double]$sorted[$mid - 1] + [double]$sorted[$mid]) / 2
}
function Stats($values) {
    $v = @($values)
    $m = $v | Measure-Object -Minimum -Maximum -Average
    return [ordered]@{ count=$v.Count; min=$m.Minimum; median=(Median $v); max=$m.Maximum; mean=$m.Average }
}
$summary = [ordered]@{}
$total = 0
foreach ($mode in @('latency','memory')) {
    $root = Get-Content (Join-Path $PSScriptRoot "build\latest-$mode.txt")
    if (Test-Path (Join-Path $root 'error.txt')) { throw 'Cannot report a failed run' }
    $records = @(Get-ChildItem $root -Directory | ForEach-Object {
        $result = Get-Content (Join-Path $_.FullName 'output.json') -Raw | ConvertFrom-Json
        $events = Get-Content (Join-Path $_.FullName 'events.json') -Raw | ConvertFrom-Json
        $loss = Get-Content (Join-Path $_.FullName 'loss.json') -Raw | ConvertFrom-Json
        if ($events.validated_events -ne $result.expected_events -or
            $loss.events_lost -or $loss.log_buffers_lost -or $loss.realtime_buffers_lost) {
            throw 'Invalid capture'
        }
        $script:total += $events.validated_events
        $result | Add-Member -NotePropertyName run -NotePropertyValue $_.Name -PassThru
    })
    $section = [ordered]@{ capture=$root; cases=@() }
    foreach ($attributes in @(0,3)) {
        foreach ($variant in @('native','wrapper','cpp')) {
            $r = @($records | Where-Object { $_.attribute_count -eq $attributes -and $_.variant -eq $variant })
            $expected = if ($mode -eq 'latency') { 6 } else { 3 }
            if ($r.Count -ne $expected) { throw 'Unexpected process count' }
            $row = [ordered]@{ attributes=$attributes; variant=$variant; processes=$r.Count }
            if ($mode -eq 'latency') {
                $row.batch_ns = Stats @($r | ForEach-Object { $_.batch_ns })
                $row.process_medians = @($r | ForEach-Object {
                    [ordered]@{ run=$_.run; ns=(Median $_.batch_ns) }
                })
                $row.process_median_ns = Stats @($r | ForEach-Object { Median $_.batch_ns })
                $row.empty_exported_call_ns = Stats @($r | ForEach-Object { $_.empty_exported_call_ns })
            } else {
                $samples = @($r | ForEach-Object { $_.probe.samples })
                foreach ($field in @('allocations','requested_bytes','reallocations','reallocated_bytes',
                                     'peak_live_bytes','peak_above_start_bytes','live_after')) {
                    $row[$field] = Stats @($samples | ForEach-Object { $_.$field })
                }
                $row.reallocation_counts = @($samples | Group-Object reallocations | ForEach-Object {
                    [ordered]@{ reallocations=[int]$_.Name; observations=$_.Count }
                })
                foreach ($field in @('idle_retained_bytes','idle_retained_blocks',
                                     'after_thread_exit_bytes','after_thread_exit_blocks')) {
                    $row[$field] = Stats @($r | ForEach-Object { $_.probe.$field })
                }
                $row.cold = @($r | ForEach-Object { $_.probe.cold })
            }
            $section.cases += $row
        }
    }
    $summary[$mode] = $section
}
$comparisons = @()
foreach ($a in @(0,3)) {
    $cases = @($summary.latency.cases | Where-Object attributes -eq $a)
    $native = $cases | Where-Object variant -eq 'native'
    $wrapper = $cases | Where-Object variant -eq 'wrapper'
    $cpp = $cases | Where-Object variant -eq 'cpp'
    $pairs = @()
    foreach ($pass in 0..5) {
        $n = ($native.process_medians | Where-Object run -eq "pass$pass-attrs$a-native").ns
        $w = ($wrapper.process_medians | Where-Object run -eq "pass$pass-attrs$a-wrapper").ns
        $c = ($cpp.process_medians | Where-Object run -eq "pass$pass-attrs$a-cpp").ns
        $pairs += [ordered]@{
            pass=$pass; native_ns=$n; wrapper_ns=$w; cpp_ns=$c
            cpp_vs_native_percent=100*($c/$n-1); cpp_vs_wrapper_percent=100*($c/$w-1)
        }
    }
    $comparisons += [ordered]@{
        attributes=$a
        pooled_cpp_minus_native_ns=$cpp.batch_ns.median-$native.batch_ns.median
        pooled_cpp_vs_native_percent=100*($cpp.batch_ns.median/$native.batch_ns.median-1)
        pooled_wrapper_vs_native_percent=100*($wrapper.batch_ns.median/$native.batch_ns.median-1)
        pooled_cpp_minus_wrapper_ns=$cpp.batch_ns.median-$wrapper.batch_ns.median
        pooled_cpp_vs_wrapper_percent=100*($cpp.batch_ns.median/$wrapper.batch_ns.median-1)
        paired_cpp_vs_native_percent=(Stats @($pairs | ForEach-Object { $_.cpp_vs_native_percent }))
        paired_cpp_vs_wrapper_percent=(Stats @($pairs | ForEach-Object { $_.cpp_vs_wrapper_percent }))
        pairs=$pairs
    }
}
$summary.comparisons = $comparisons
$summary.validated_events = $total
$summary | ConvertTo-Json -Depth 15 | Set-Content (Join-Path $PSScriptRoot 'build\summary.json')
$summary.latency.cases | ForEach-Object {
    [pscustomobject]@{ attributes=$_.attributes; variant=$_.variant;
        median_us=$_.batch_ns.median/1000; process_min_us=$_.process_median_ns.min/1000;
        process_max_us=$_.process_median_ns.max/1000; empty_call_ns=$_.empty_exported_call_ns.median }
} | Format-Table
$summary.memory.cases | ForEach-Object {
    [pscustomobject]@{ attributes=$_.attributes; variant=$_.variant;
        allocations=$_.allocations.median; requested=$_.requested_bytes.median;
        peak=$_.peak_live_bytes.median; retained=$_.idle_retained_bytes.median;
        after_exit=$_.after_thread_exit_bytes.max }
} | Format-Table
$comparisons | ConvertTo-Json -Depth 8
"Validated latency/memory events: $total"
