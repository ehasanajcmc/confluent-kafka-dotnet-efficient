<#
.SYNOPSIS
Prints side-by-side legacy/efficient comparison tables and saves readable text and CSV.
.DESCRIPTION
By default this runs the ordinary, unprofiled benchmark for each requested message count.
The tables are printed in the terminal and saved as Comparison.txt and Comparison.csv.
Use -IncludeAllocationProfiles to ALSO collect full object-allocation .diagsession files.
Only that optional step requires Visual Studio 2019+ with .NET Object Allocation Tracking.
Full profiling tracks every allocation and can produce very large reports at 100 million sends.
Use Comparison.txt/CSV for timings; profiled timings include instrumentation overhead.
Object counts are not automatically extracted. The sender wrappers exclude setup and warmup,
but their call trees DO NOT include background delivery-thread allocations. Full-path object
counts require analyzing both threads within the measured interval; whole-trace totals also
include setup, warmup, and disposal. Do not use sender wrapper totals as full-path counts.
.EXAMPLE
.\Profile-Allocations.ps1 -Counts 1000
.EXAMPLE
.\Profile-Allocations.ps1 -OutputDirectory .\benchmark-results
.EXAMPLE
.\Profile-Allocations.ps1 -Counts 1000 -IncludeAllocationProfiles
#>
[CmdletBinding()]
param(
    [string]$BootstrapServers = '127.0.0.1:9092',
    [string]$Topic = 'dotnet-benchmark',
    [ValidateNotNullOrEmpty()]
    [int[]]$Counts = @(1000, 1000000, 100000000),
    [ValidateRange(0, [int]::MaxValue)]
    [int]$MessageSize = 100,
    [string]$OutputDirectory,
    [string]$CollectorPath,
    [switch]$IncludeAllocationProfiles,
    [switch]$LegacyFirst,
    [ValidateRange(1, [int]::MaxValue)]
    [int]$CaseTimeoutSeconds = 7200
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

foreach ($count in $Counts) {
    if ($count -le 0) {
        throw 'Every message count must be positive.'
    }
}

if (@($Counts | Select-Object -Unique).Count -ne $Counts.Count) {
    throw 'Message counts must be unique so that each profile has its own result file.'
}

$projectPath = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..\Confluent.Kafka.Benchmark.csproj')).Path
$projectDirectory = Split-Path -Parent $projectPath
$benchmarkDll = Join-Path $projectDirectory 'bin\Release\net8.0\Confluent.Kafka.Benchmark.dll'
$dotnetPath = (Get-Command dotnet -CommandType Application -ErrorAction Stop).Source

if ($IncludeAllocationProfiles) {
    if ([string]::IsNullOrWhiteSpace($CollectorPath)) {
        $vswherePath = Join-Path ([Environment]::GetFolderPath('ProgramFilesX86')) 'Microsoft Visual Studio\Installer\vswhere.exe'
        if (-not (Test-Path -LiteralPath $vswherePath)) {
            throw 'Visual Studio profiler was not found. Install its profiling tools or supply -CollectorPath.'
        }

        $installationPaths = @(& $vswherePath -all -sort -products '*' -property installationPath)
        if ($LASTEXITCODE -ne 0) {
            throw 'vswhere failed while locating the Visual Studio profiler.'
        }

        foreach ($installationPath in $installationPaths) {
            $candidate = Join-Path $installationPath 'Team Tools\DiagnosticsHub\Collector\VSDiagnostics.exe'
            $candidateAgent = Join-Path (Split-Path -Parent $candidate) 'Agents\DiagnosticsHub.DotNetObjectAllocAgent.dll'
            if ((Test-Path -LiteralPath $candidate) -and (Test-Path -LiteralPath $candidateAgent)) {
                $CollectorPath = $candidate
                break
            }
        }
    }

    if ([string]::IsNullOrWhiteSpace($CollectorPath) -or -not (Test-Path -LiteralPath $CollectorPath)) {
        throw 'VSDiagnostics.exe was not found. Supply -CollectorPath pointing to the Visual Studio profiler.'
    }
    $CollectorPath = (Resolve-Path -LiteralPath $CollectorPath).Path
    $agentPath = Join-Path (Split-Path -Parent $CollectorPath) 'Agents\DiagnosticsHub.DotNetObjectAllocAgent.dll'
    if (-not (Test-Path -LiteralPath $agentPath)) {
        throw 'The selected Visual Studio installation does not include .NET Object Allocation Tracking.'
    }
}

if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
    $OutputDirectory = Join-Path ([IO.Path]::GetTempPath()) ('confluent-kafka-comparison-' + [Guid]::NewGuid().ToString('N'))
}
$OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
[IO.Directory]::CreateDirectory($OutputDirectory) | Out-Null
$runDirectory = Join-Path $OutputDirectory ('run-' + [Guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory($runDirectory) | Out-Null

& $dotnetPath build $projectPath --configuration Release --framework net8.0 --verbosity minimal
if ($LASTEXITCODE -ne 0) {
    throw 'Benchmark build failed; no profiling workloads were started.'
}


$comparisonTextPath = Join-Path $runDirectory 'Comparison.txt'
$comparisonCsvPath = Join-Path $runDirectory 'Comparison.csv'
$comparisonArguments = @(
    $benchmarkDll, '-m', 'efficient',
    '-b', $BootstrapServers, '-t', $Topic,
    '--counts', ($Counts -join ','), '-s', $MessageSize.ToString(),
    '--output', $comparisonCsvPath
)
Write-Host "Running comparison. Results: $runDirectory"
if ($LegacyFirst) { $comparisonArguments += '--legacy-first' }
& $dotnetPath @comparisonArguments | Tee-Object -FilePath $comparisonTextPath
if ($LASTEXITCODE -ne 0) {
    throw 'The comparison benchmark failed. Partial console output is saved in Comparison.txt.'
}
Write-Host "Readable comparison: $comparisonTextPath"
Write-Host "Comparison CSV: $comparisonCsvPath"

if (-not $IncludeAllocationProfiles) {
    Write-Host 'No profiler traces were collected. Add -IncludeAllocationProfiles only when you want the large object-allocation reports.'
    return
}

$configPath = Join-Path $runDirectory 'FullAllocations.json'
$config = @{
    Agents = @(@{
        CLSID = 'F5DA0D98-EA8F-4905-AE46-4C360190DFDE'
        Name = 'DiagnosticsHub.DotNetObjectAllocAgent.dll'
        Config = @{ dotNetAllocSampleRate = 1 }
    })
} | ConvertTo-Json -Depth 5
[IO.File]::WriteAllText($configPath, $config, [Text.UTF8Encoding]::new($false))

function ConvertTo-LaunchArgument([string]$Value) {
    $escaped = [regex]::Replace($Value, '(\\*)"', '$1$1\"')
    $escaped = [regex]::Replace($escaped, '(\\+)$', '$1$1')
    return '"' + $escaped + '"'
}

$rows = [Collections.Generic.List[object]]::new()
$indexPath = Join-Path $runDirectory 'ReportIndex.csv'
Write-Host 'Full allocation tracking: one report per approach/count; do not compare these profiled timings.'
Write-Host "Reports: $runDirectory"
Write-Host 'Object counts require manual analysis of both sender and background delivery threads; sender wrapper totals are incomplete.'

foreach ($count in $Counts) {
    foreach ($approach in @('legacy', 'efficient')) {
        $caseName = "$count-$approach"
        $reportPath = Join-Path $runDirectory "$caseName.diagsession"
        $resultPath = Join-Path $runDirectory "$caseName.json"
        $sessionId = [Guid]::NewGuid().ToString()
        $launchArguments = @(
            $benchmarkDll, '-m', 'efficient-allocations',
            '-b', $BootstrapServers, '-t', $Topic, '-n', $count.ToString(),
            '-s', $MessageSize.ToString(), '--approach', $approach,
            '--profile-result', $resultPath
        )
        $launchCommandLine = ($launchArguments | ForEach-Object { ConvertTo-LaunchArgument $_ }) -join ' '
        $sessionStarted = $false
        $workloadFinished = $false
        $workloadProcess = $null
        Write-Host "Profiling $caseName..."

        try {
            & $CollectorPath start $sessionId "/loadConfig:$configPath" "/launch:$dotnetPath" "/launchArgs:$launchCommandLine"
            if ($LASTEXITCODE -ne 0) {
                throw "Could not start the profiler for $caseName."
            }
            $sessionStarted = $true
            $deadline = [DateTime]::UtcNow.AddSeconds($CaseTimeoutSeconds)
            $startupDeadline = [DateTime]::UtcNow.AddSeconds(60)
            while (-not (Test-Path -LiteralPath $resultPath)) {
                $startedPath = $resultPath + '.started'
                if ($null -eq $workloadProcess -and (Test-Path -LiteralPath $startedPath)) {
                    $workloadProcessId = [int](Get-Content -LiteralPath $startedPath -Raw)
                    $workloadProcess = Get-Process -Id $workloadProcessId -ErrorAction SilentlyContinue
                }
                if ($null -ne $workloadProcess -and $workloadProcess.HasExited) {
                    if (Test-Path -LiteralPath $resultPath) {
                        break
                    }
                    throw "The workload exited before writing its result for $caseName."
                }
                if ($null -eq $workloadProcess -and [DateTime]::UtcNow -ge $startupDeadline) {
                    throw "The profiler did not start the workload for $caseName within 60 seconds."
                }
                if ([DateTime]::UtcNow -ge $deadline) {
                    throw "Timed out waiting for $caseName. A partial profiler report will be saved."
                }
                Start-Sleep -Milliseconds 250
            }

            $workloadFinished = $true
            $result = Get-Content -LiteralPath $resultPath -Raw | ConvertFrom-Json
            if (-not $result.Succeeded) {
                throw "Profile workload $caseName failed: $($result.Error)"
            }
        }
        finally {
            if (-not $workloadFinished -and $null -ne $workloadProcess -and -not $workloadProcess.HasExited) {
                Stop-Process -InputObject $workloadProcess -ErrorAction SilentlyContinue
            }
            if ($sessionStarted) {
                & $CollectorPath stop $sessionId "/output:$reportPath"
                if ($LASTEXITCODE -ne 0) {
                    throw "Could not save the profiler report for $caseName."
                }
            }
        }

        if (-not (Test-Path -LiteralPath $reportPath)) {
            throw "The profiler did not create $reportPath."
        }

        $rows.Add([pscustomobject]@{
            Messages = $count
            Approach = $approach
            SamplingRate = 1
            MeasuredFunction = $result.MeasuredFunction
            Delivered = $result.Delivered
            Errors = $result.Errors
            AllocatedBytes = $result.AllocatedBytes
            SenderAllocatedBytes = $result.SenderAllocatedBytes
            DeliveryThreadAllocatedBytes = $result.DeliveryThreadAllocatedBytes
            ObjectCountLocation = 'Manual analysis required: both threads within measured interval'
            Report = $reportPath
            WorkloadResult = $resultPath
        })
        $rows | Export-Csv -LiteralPath $indexPath -NoTypeInformation -Encoding UTF8
        Write-Host "Saved: $reportPath"
    }
}

$rows | Format-Table Messages, Approach, MeasuredFunction, Delivered, AllocatedBytes -AutoSize
Write-Host "Report index: $indexPath"
Write-Host 'Open each .diagsession in Visual Studio for exact object counts and allocated types.'
