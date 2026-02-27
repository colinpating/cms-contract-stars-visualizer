param(
  [string]$Path = (Join-Path $PSScriptRoot '..\index.html')
)

$resolved = Resolve-Path $Path
Start-Process $resolved.Path
Write-Host "Opened $($resolved.Path)"
