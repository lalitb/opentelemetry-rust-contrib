param([switch]$Smoke)
$ErrorActionPreference = 'Stop'
$build = Join-Path $PSScriptRoot 'build'
'running' | Set-Content (Join-Path $build 'controller-status.txt')
Start-Transcript -Path (Join-Path $build 'controller.log') -Force | Out-Null
try {
    & (Join-Path $PSScriptRoot 'run.ps1') -Mode check
    & (Join-Path $PSScriptRoot 'run.ps1') -Mode memory -Smoke
    & (Join-Path $PSScriptRoot 'run.ps1') -Mode latency -Smoke:$Smoke
    if (!$Smoke) {
        & (Join-Path $PSScriptRoot 'run.ps1') -Mode memory
        & (Join-Path $PSScriptRoot 'report.ps1')
    } else {
        & (Join-Path $PSScriptRoot 'report.ps1') -Smoke
    }
    'completed' | Set-Content (Join-Path $build 'controller-status.txt')
} catch {
    $_ | Out-String | Write-Output
    'failed' | Set-Content (Join-Path $build 'controller-status.txt')
    throw
} finally {
    Stop-Transcript | Out-Null
}
