$ErrorActionPreference = 'Stop'
Push-Location $PSScriptRoot
try {
    foreach ($tool in @('cargo','rustc','cmake','git')) {
        Get-Command $tool -ErrorAction Stop | Out-Null
    }
    & cargo fmt --manifest-path Cargo.toml -- --check
    if ($LASTEXITCODE -ne 0) { throw 'Formatting check failed' }
    & cargo clippy --locked --manifest-path Cargo.toml --target-dir target -- -Dwarnings
    if ($LASTEXITCODE -ne 0) { throw 'Clippy failed' }
    & cargo build --release --locked --manifest-path Cargo.toml --target-dir target
    if ($LASTEXITCODE -ne 0) { throw 'Rust DLL build failed' }
    & cmake -S . -B build -A x64
    if ($LASTEXITCODE -ne 0) { throw 'CMake configuration failed' }
    & cmake --build build --config Release
    if ($LASTEXITCODE -ne 0) { throw 'C++ build failed' }

    $repo = (& git rev-parse --show-toplevel).Trim()
    if ($LASTEXITCODE -ne 0) { throw 'Cannot locate repository' }
    $inputs = @(
        'Cargo.toml','Cargo.lock','CMakeLists.txt','src\lib.rs','driver.cc','ffi.h',
        'heap_probe.h','capture.cc','build.ps1','run.ps1','run-all.ps1','report.ps1',
        '..\..\Cargo.toml'
    ) | ForEach-Object { (Resolve-Path $_).Path }
    $inputs += (Resolve-Path (Join-Path $repo 'Cargo.toml')).Path
    $inputs += (Resolve-Path (Join-Path $repo 'rust-toolchain.toml')).Path
    $inputs += Get-ChildItem '..\..\src' -Recurse -Filter '*.rs' |
        Select-Object -ExpandProperty FullName
    $inputs | Get-FileHash -Algorithm SHA256 |
        Export-Csv -NoTypeInformation build\build-sources.csv
    Get-FileHash build\bin\driver.exe,build\bin\etw_ffi_bench.dll,build\bin\capture.exe |
        Export-Csv -NoTypeInformation build\build-binaries.csv
    & rustc -vV | Set-Content build\compiler.txt
    if ($LASTEXITCODE -ne 0) { throw 'Cannot record compiler version' }
} finally {
    Pop-Location
}
