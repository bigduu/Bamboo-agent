#Requires -Version 7.2
[CmdletBinding()]
param(
    [ValidateSet('directory-flush', 'writer-lock')][string] $ProbeCase = 'directory-flush'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (-not $IsWindows -or
    [System.Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture -ne 'X64' -or
    $env:RUNNER_ENVIRONMENT -ne 'github-hosted' -or
    [string]::IsNullOrWhiteSpace($env:RUNNER_TEMP)) {
    throw 'The disposable account driver requires a GitHub-hosted Windows x64 runner.'
}

function Grant-ProbeAccess($Acl, [System.Security.Principal.SecurityIdentifier] $Sid, $Rights) {
    $inheritance = [System.Security.AccessControl.InheritanceFlags]::ContainerInherit -bor
        [System.Security.AccessControl.InheritanceFlags]::ObjectInherit
    $rule = [System.Security.AccessControl.FileSystemAccessRule]::new(
        $Sid, $Rights, $inheritance, 'None', 'Allow'
    )
    $Acl.SetAccessRule($rule)
}

function Assert-ProbeOutcome($Outcome, [string] $Name, [bool] $Attempted) {
    if ($Outcome.attempted -ne $Attempted -or $Outcome.succeeded -isnot [bool]) {
        throw "Invalid attempt status for $Name."
    }
    if ($Attempted) {
        if ($null -eq $Outcome.win32_error -or [string]$Outcome.win32_error -notmatch '^[0-9]+$' -or
            $Outcome.succeeded -ne ($Outcome.win32_error -eq 0)) {
            throw "Invalid Win32 result for $Name."
        }
    } elseif ($Outcome.succeeded -or $null -ne $Outcome.win32_error) {
        throw "Unexpected Win32 result for unattempted $Name."
    }
}

function Assert-WriterProbe($Receipt, [int] $PrimaryProcessId) {
    $proof = $Receipt.writer_lock
    $names = $proof.name_contender.names
    if ($Receipt.probe -cne 'writer-lock' -or $proof.owner.pid -ne $PrimaryProcessId -or
        $proof.same_process.pid -ne $PrimaryProcessId -or
        $proof.after_clean_release.pid -eq $PrimaryProcessId -or
        $proof.exit_holder_exited -ne $true -or $proof.exit_holder.pid -eq $PrimaryProcessId -or
        $proof.while_exit_holder_held.pid -eq $proof.exit_holder.pid -or
        $proof.name_contender.pid -eq $PrimaryProcessId -or
        $Receipt.writer_lock_name_stable -isnot [bool] -or
        $names.name_preserved -ne $Receipt.writer_lock_name_stable -or
        $names.qualified -isnot [bool] -or ($names.qualified -and -not $names.name_preserved)) {
        throw 'The writer receipt did not verify the process boundaries and name result.'
    }
    $contenders = @('same_process', 'contender', 'alias_contender', 'while_exit_holder_held')
    $successful = @('owner', 'after_clean_release', 'exit_holder', 'after_process_exit')
    $lockId = $proof.owner.file_id
    if ($lockId -cnotmatch '^[0-9a-f]{32}$' -or
        $proof.contender.pid -eq $PrimaryProcessId -or $proof.alias_contender.pid -eq $PrimaryProcessId) {
        throw 'The writer receipt did not identify the lock and independent contenders.'
    }
    foreach ($phase in ($contenders + $successful)) {
        $observation = $proof.$phase
        Assert-ProbeOutcome $observation.operation $phase $true
        $expectedError = if ($phase -in $contenders) { 33 } else { 0 }
        if ($observation.operation.win32_error -ne $expectedError -or
            $observation.file_id -cne $lockId -or $observation.volume_serial -cne $proof.owner.volume_serial) {
            throw "Unexpected lock or identity evidence during $phase."
        }
    }
    $expectedVerdict = if ($names.qualified) { 'available' } else { 'unavailable' }
    if ($Receipt.verdict -cne $expectedVerdict) {
        throw 'The writer verdict does not match the name qualification.'
    }
}

$runnerSid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
$systemSid = [System.Security.Principal.SecurityIdentifier]::new('S-1-5-18')
$usersSid = [System.Security.Principal.SecurityIdentifier]::new('S-1-5-32-545')
$nonce = [Guid]::NewGuid().ToString('N')
$accountName = "bamboo-$($nonce.Substring(0, 12))"
$workspace = Join-Path $env:RUNNER_TEMP "bamboo-ticket-flush-$nonce"
$account = $null
$process = $null
$createdWorkspace = $false
$password = [System.Security.SecureString]::new()

try {
    $random = [System.Security.Cryptography.RandomNumberGenerator]::GetBytes(24)
    foreach ($character in ('Aa1!' + [Convert]::ToHexString($random)).ToCharArray()) {
        $password.AppendChar($character)
    }
    [Array]::Clear($random, 0, $random.Length)
    $password.MakeReadOnly()

    $account = New-LocalUser -Name $accountName -Password $password -Description 'Disposable Bamboo Ticket flush probe' `
        -AccountExpires ([DateTime]::Now.AddHours(1))
    $members = Get-LocalGroupMember -SID $usersSid | Where-Object { $_.SID.Value -eq $account.SID.Value }
    if (-not $members) {
        Add-LocalGroupMember -SID $usersSid -Member $account
    }

    $null = New-Item -ItemType Directory -Path $workspace
    $createdWorkspace = $true
    $workspaceAcl = Get-Acl -LiteralPath $workspace
    $workspaceAcl.SetAccessRuleProtection($true, $false)
    Grant-ProbeAccess $workspaceAcl $runnerSid 'FullControl'
    Grant-ProbeAccess $workspaceAcl $systemSid 'FullControl'
    Grant-ProbeAccess $workspaceAcl $account.SID 'ReadAndExecute'
    Set-Acl -LiteralPath $workspace -AclObject $workspaceAcl

    $scratch = Join-Path $workspace 'scratch'
    $null = New-Item -ItemType Directory -Path $scratch
    $scratchAcl = Get-Acl -LiteralPath $scratch
    Grant-ProbeAccess $scratchAcl $account.SID 'Modify'
    Set-Acl -LiteralPath $scratch -AclObject $scratchAcl
    $fixture = Join-Path $scratch 'fixture'
    $receiptPath = Join-Path $scratch 'receipt.json'
    $null = New-Item -ItemType Directory -Path $fixture

    $copiedProbe = Join-Path $workspace 'probe.ps1'
    $probeFiles = [ordered]@{ 'windows-ticket-directory-flush-probe.ps1' = 'probe.ps1' }
    if ($ProbeCase -eq 'writer-lock') {
        $probeFiles['windows-ticket-writer-lock-native.cs'] = 'windows-ticket-writer-lock-native.cs'
        $probeFiles['windows-ticket-writer-lock-cases.ps1'] = 'windows-ticket-writer-lock-cases.ps1'
    }
    $stagedHashes = @{}
    foreach ($filename in $probeFiles.Keys) {
        $sourcePath = Join-Path $PSScriptRoot $filename
        $stagedPath = Join-Path $workspace $probeFiles[$filename]
        $sourceHash = (Get-FileHash -LiteralPath $sourcePath -Algorithm SHA256).Hash
        Copy-Item -LiteralPath $sourcePath -Destination $stagedPath
        if ((Get-FileHash -LiteralPath $stagedPath -Algorithm SHA256).Hash -ne $sourceHash) {
            throw "The staged file differs from the checked-out $filename."
        }
        $stagedHashes[$stagedPath] = $sourceHash
        if ($stagedPath -eq $copiedProbe) {
            Write-Output "Probe script SHA256 $sourceHash"
        } else {
            Write-Output "Probe companion $filename SHA256 $sourceHash"
        }
    }

    $launch = [System.Diagnostics.ProcessStartInfo]::new((Join-Path $PSHOME 'pwsh.exe'))
    $launch.WorkingDirectory = $scratch
    $launch.UseShellExecute = $false
    $launch.UserName = $account.Name
    $launch.Domain = $env:COMPUTERNAME
    $launch.Password = $password
    $launch.RedirectStandardOutput = $true
    $launch.RedirectStandardError = $true
    $launch.Environment.Clear()
    $launch.Environment['SystemRoot'] = $env:SystemRoot
    $launch.Environment['WINDIR'] = $env:SystemRoot
    $launch.Environment['PATH'] = Join-Path $env:SystemRoot 'System32'
    $launch.Environment['TEMP'] = $scratch
    $launch.Environment['TMP'] = $scratch

    foreach ($argument in @(
        '-NoLogo', '-NoProfile', '-NonInteractive', '-File', $copiedProbe,
        '-ExpectedSid', $account.SID.Value, '-FixtureDirectory', $fixture, '-ReceiptPath', $receiptPath,
        '-ProbeCase', $ProbeCase
    )) {
        $launch.ArgumentList.Add($argument)
    }
    $process = [System.Diagnostics.Process]::Start($launch)
    $stdout = $process.StandardOutput.ReadToEndAsync()
    $stderr = $process.StandardError.ReadToEndAsync()
    $timeoutSeconds = if ($ProbeCase -eq 'writer-lock') { 180 } else { 90 }
    if (-not $process.WaitForExit($timeoutSeconds * 1000)) {
        throw "The standard-user probe exceeded its $timeoutSeconds-second deadline."
    }
    if (-not [System.Threading.Tasks.Task]::WaitAll([System.Threading.Tasks.Task[]]@($stdout, $stderr), 5000)) {
        throw 'The standard-user probe output remained open after process exit.'
    }
    $output = $stdout.GetAwaiter().GetResult()
    $errorOutput = $stderr.GetAwaiter().GetResult()
    if ($output) { Write-Output $output.TrimEnd() }
    if ($errorOutput) { Write-Output $errorOutput.TrimEnd() }
    if ($process.ExitCode -ne 0) {
        throw "The standard-user probe exited $($process.ExitCode)."
    }

    $receiptJson = Get-Content -LiteralPath $receiptPath -Raw
    $receipt = $receiptJson | ConvertFrom-Json
    if ($receipt.schema -ne 1 -or $receipt.process_architecture -cne 'X64' -or
        [string]::IsNullOrWhiteSpace($receipt.windows_version) -or
        $receipt.token.sid -ne $account.SID.Value -or
        $receipt.token.sid -eq $runnerSid.Value -or $receipt.token.elevated -ne $false -or
        $receipt.token.integrity_sid -ne 'S-1-16-8192' -or
        $receipt.token.administrators_group_present -ne $false -or
        $receipt.token.local_account_group_present -ne $true -or
        @($receipt.token.excess_privileges_present).Count -ne 0 -or
        $receipt.volume.file_system -ne 'NTFS' -or $receipt.volume.drive_type_code -ne 3 -or
        $receipt.ordered_crash_persistence_proven -ne $false) {
        throw 'Probe output did not attest the native standard user and fixed NTFS or exceeded the probe scope.'
    }
    if ($ProbeCase -eq 'writer-lock') {
        Assert-WriterProbe $receipt $process.Id
    } else {
        Assert-ProbeOutcome $receipt.staged_file_flush 'staged file flush' $true
        Assert-ProbeOutcome $receipt.scratch_name_publication 'scratch name publication' $true
        if (-not $receipt.staged_file_flush.succeeded -or -not $receipt.scratch_name_publication.succeeded -or
            $receipt.warm_reopen.succeeded -ne $true -or $receipt.staged_file_id -cnotmatch '^[0-9a-f]{32}$' -or
            $receipt.warm_reopen.published_file_id -cne $receipt.staged_file_id -or $receipt.warm_reopen.bytes -le 0) {
            throw 'Probe output did not verify the staged file and same-file warm reopen.'
        }
        Assert-ProbeOutcome $receipt.directory_open 'writable directory open' $true
        Assert-ProbeOutcome $receipt.directory_flush_before 'directory flush before publication' $receipt.directory_open.succeeded
        Assert-ProbeOutcome $receipt.directory_flush_after 'directory flush after publication' $receipt.directory_open.succeeded
        $directoryAvailable = $receipt.directory_open.succeeded -and
            $receipt.directory_flush_before.succeeded -and $receipt.directory_flush_after.succeeded
        $expectedVerdict = if ($directoryAvailable) { 'available' } else { 'unavailable' }
        if ($receipt.verdict -cne $expectedVerdict) {
            throw 'The directory verdict does not match the reported native outcomes.'
        }
    }
    foreach ($stagedPath in $stagedHashes.Keys) {
        if ((Get-FileHash -LiteralPath $stagedPath -Algorithm SHA256).Hash -ne $stagedHashes[$stagedPath]) {
            throw 'A staged probe file changed during the trial.'
        }
    }
    Write-Output $receiptJson.TrimEnd()
} finally {
    try {
        if ($process) {
            if (-not $process.HasExited) {
                $process.Kill($true)
                if (-not $process.WaitForExit(5000)) {
                    throw 'The stopped probe did not exit within the cleanup deadline.'
                }
            }
            $process.Dispose()
        }
    } finally {
        $password.Dispose()
        try {
            if ($account) { Remove-LocalUser -SID $account.SID }
        } finally {
            if ($createdWorkspace) { Remove-Item -LiteralPath $workspace -Recurse -Force }
        }
    }
}
