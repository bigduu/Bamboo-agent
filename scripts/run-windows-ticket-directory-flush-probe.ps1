#Requires -Version 7.2
[CmdletBinding()]
param(
    [ValidateSet('directory-flush', 'writer-lock', 'publication', 'publication-process-exit', 'scope-identity', 'rust-writer', 'rust-immutable', 'rust-head', 'rust-graph')][string] $ProbeCase = 'directory-flush'
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

function Copy-ProbeFile([string] $Source, [string] $Staged) {
    $sha = (Get-FileHash -LiteralPath $Source -Algorithm SHA256).Hash
    [IO.File]::Copy($Source, $Staged, $false)
    if ((Get-FileHash -LiteralPath $Staged -Algorithm SHA256).Hash -cne $sha) {
        throw "The staged file differs from the selected probe input $Source."
    }
    return $sha
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

function Assert-PublicationProbe($Receipt) {
    $proof = $Receipt.publication
    if ($Receipt.probe -cne 'publication' -or $Receipt.publication_qualified -isnot [bool]) {
        throw 'The native publication receipt did not identify the measured case.'
    }
    $acquisition = $proof.lock.PSObject.Properties['acquisition']
    if ($null -ne $acquisition) {
        Assert-ProbeOutcome $acquisition.Value 'publication writer lock' $true
    }
    $qualified = $true
    foreach ($phase in @($proof.lock, $proof.immutable, $proof.head)) {
        if ($phase.qualified -isnot [bool]) { throw 'The publication receipt has an invalid phase.' }
        $qualified = $qualified -and $phase.qualified
    }
    if ($qualified -and ($null -eq $acquisition -or -not $acquisition.Value.succeeded -or
        $proof.lock.file_id -cnotmatch '^[0-9a-f]{32}$' -or $proof.lock.file_id -cne $proof.lock.final_file_id -or
        -not $proof.immutable.insert.qualified -or -not $proof.immutable.collision.qualified -or
        -not $proof.immutable.reuse.qualified -or -not $proof.immutable.conflict.qualified -or
        -not $proof.head.initial.qualified -or
        -not $proof.head.no_source_delete.qualified -or -not $proof.head.no_reader_delete_share.qualified -or
        -not $proof.head.replace.qualified)) {
        throw 'The publication verdict lacks a required native control.'
    }
    $expectedVerdict = if ($qualified) { 'available' } else { 'unavailable' }
    if ($Receipt.publication_qualified -ne $qualified -or $Receipt.verdict -cne $expectedVerdict) {
        throw 'The publication verdict does not match its native phases.'
    }
}

function Assert-PublicationProcessExitProbe($Receipt, [int] $CoordinatorProcessId) {
    $cuts = @('complete', 'before-head-replace', 'after-head-replace', 'after-scope-flush')
    if ($Receipt.probe -cne 'publication-process-exit' -or $Receipt.coordinator_pid -ne $CoordinatorProcessId -or
        $Receipt.process_exit_qualified -isnot [bool] -or @($Receipt.cases.PSObject.Properties).Count -ne $cuts.Count) {
        throw 'The publication process receipt did not identify its coordinator and cases.'
    }
    $qualified = $true
    foreach ($cut in $cuts) {
        $trial = $Receipt.cases.PSObject.Properties[$cut].Value
        if ($trial.cut -cne $cut -or $trial.qualified -isnot [bool]) {
            throw "Invalid publication process receipt for '$cut'."
        }
        $qualified = $qualified -and $trial.qualified
        if (-not $trial.qualified) { continue }
        $clean = $cut -eq 'complete'
        $writerRecord = if ($clean) { 'complete' } else { 'cut_reached' }
        $expectedExitCode = if ($clean) { 0 } else { 0x4254 }
        $expectedTermination = if ($clean) { $null } else { 0 }
        if ($trial.writer_terminated -ne (-not $clean) -or $trial.writer_exit_code -ne $expectedExitCode -or
            $trial.termination_win32_error -ne $expectedTermination -or $trial.writer.record -cne $writerRecord -or
            $trial.verifier.record -cne 'verification' -or $trial.writer.pid -eq $trial.verifier.pid -or
            $trial.writer.pid -eq $CoordinatorProcessId -or $trial.verifier.pid -eq $CoordinatorProcessId -or
            $trial.writer.lock.file_id -cne $trial.verifier.lock.file_id -or
            $trial.writer.volume.fixture_directory_file_id -cne $trial.verifier.volume.fixture_directory_file_id) {
            throw "The qualified '$cut' receipt lacks its process boundary or independent reopen."
        }
    }
    $expectedVerdict = if ($qualified) { 'available' } else { 'unavailable' }
    if ($Receipt.process_exit_qualified -ne $qualified -or $Receipt.verdict -cne $expectedVerdict) {
        throw 'The publication process verdict does not match its native cases.'
    }
}

function Assert-ScopeIdentityProbe($Receipt) {
    if ($Receipt.probe -cne 'scope-identity' -or $Receipt.scope_identity_qualified -isnot [bool] -or
        $Receipt.junction_removed -isnot [bool]) {
        throw 'The scope identity receipt did not identify the measured case or junction cleanup.'
    }
    $proof = $Receipt.scope_identity
    $preservedTarget = $Receipt.PSObject.Properties['junction_target_preserved_after_cleanup']
    $qualified = $Receipt.junction_removed -and $null -ne $preservedTarget -and $preservedTarget.Value -eq $true
    foreach ($phase in @($proof.positive, $proof.junction, $proof.multilink)) {
        if ($phase.qualified -isnot [bool]) { throw 'Invalid scope identity phase.' }
        $qualified = $qualified -and $phase.qualified
    }
    $expectedVerdict = if ($qualified) { 'available' } else { 'unavailable' }
    if ($Receipt.scope_identity_qualified -ne $qualified -or $Receipt.verdict -cne $expectedVerdict) {
        throw 'The scope identity verdict does not match its native phases.'
    }
}

$nativeBinary = $null
if ($ProbeCase -in @('rust-writer', 'rust-immutable', 'rust-head', 'rust-graph')) {
    . (Join-Path $PSScriptRoot 'windows-ticket-rust-writer-owner.ps1')
    $rustTest = Get-TicketRustProbeTest $ProbeCase
    $nativeBinary = Get-TicketRustProbeBinary
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

    $stagedHashes = @{}
    if ($nativeBinary) {
        $launchPath = Join-Path $workspace 'ticket-rust-writer.exe'
        $binaryHash = Copy-ProbeFile $nativeBinary $launchPath
        $stagedHashes[$launchPath] = $binaryHash
        Write-Output "Probe Rust test SHA256 $binaryHash"
    } else {
        $copiedProbe = Join-Path $workspace 'probe.ps1'
        $probeFiles = [ordered]@{ 'windows-ticket-directory-flush-probe.ps1' = 'probe.ps1' }
        if ($ProbeCase -ne 'directory-flush') {
            $probeFiles['windows-ticket-writer-lock-native.cs'] = 'windows-ticket-writer-lock-native.cs'
        }
        if ($ProbeCase -in @('writer-lock', 'publication-process-exit')) {
            $probeFiles['windows-ticket-writer-lock-cases.ps1'] = 'windows-ticket-writer-lock-cases.ps1'
        }
        if ($ProbeCase -in @('publication', 'publication-process-exit', 'scope-identity')) {
            $probeFiles['windows-ticket-publication-native.cs'] = 'windows-ticket-publication-native.cs'
        }
        if ($ProbeCase -eq 'publication-process-exit') {
            $probeFiles['windows-ticket-publication-process-exit-native.cs'] = 'windows-ticket-publication-process-exit-native.cs'
            $probeFiles['windows-ticket-publication-process-exit-cases.ps1'] = 'windows-ticket-publication-process-exit-cases.ps1'
        }
        if ($ProbeCase -eq 'scope-identity') {
            $probeFiles['windows-ticket-scope-identity-native.cs'] = 'windows-ticket-scope-identity-native.cs'
            $probeFiles['windows-ticket-scope-identity-cases.ps1'] = 'windows-ticket-scope-identity-cases.ps1'
        }
        foreach ($filename in $probeFiles.Keys) {
            $sourcePath = Join-Path $PSScriptRoot $filename
            $stagedPath = Join-Path $workspace $probeFiles[$filename]
            $sourceHash = Copy-ProbeFile $sourcePath $stagedPath
            $stagedHashes[$stagedPath] = $sourceHash
            if ($stagedPath -eq $copiedProbe) {
                Write-Output "Probe script SHA256 $sourceHash"
            } else {
                Write-Output "Probe companion $filename SHA256 $sourceHash"
            }
        }

        $launchPath = Join-Path $PSHOME 'pwsh.exe'
    }

    $launch = [System.Diagnostics.ProcessStartInfo]::new($launchPath)
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

    if ($nativeBinary) {
        $launch.Environment['BAMBOO_TICKET_WRITER_EXPECTED_SID'] = $account.SID.Value
        $launch.Environment['BAMBOO_TICKET_WRITER_FIXTURE'] = $fixture
        $launch.Environment['BAMBOO_TICKET_WRITER_RECEIPT'] = $receiptPath
        $launch.Environment['BAMBOO_TICKET_WRITER_ROLE'] = 'owner'
        $arguments = @($rustTest, '--exact', '--ignored', '--nocapture', '--test-threads=1')
    } else {
        $arguments = @(
            '-NoLogo', '-NoProfile', '-NonInteractive', '-File', $copiedProbe,
            '-ExpectedSid', $account.SID.Value, '-FixtureDirectory', $fixture, '-ReceiptPath', $receiptPath,
            '-ProbeCase', $ProbeCase
        )
    }
    foreach ($argument in $arguments) { $launch.ArgumentList.Add($argument) }
    $process = [System.Diagnostics.Process]::Start($launch)
    $stdout = $process.StandardOutput.ReadToEndAsync()
    $stderr = $process.StandardError.ReadToEndAsync()
    $timeoutSeconds = if ($ProbeCase -in @('writer-lock', 'publication-process-exit', 'rust-writer', 'rust-immutable', 'rust-head', 'rust-graph')) { 180 } else { 90 }
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
        if ($nativeBinary -and (Test-Path -LiteralPath $receiptPath)) {
            Write-Output (Get-Content -LiteralPath $receiptPath -Raw).TrimEnd()
        }
        throw "The standard-user probe exited $($process.ExitCode)."
    }
    if ($nativeBinary) { Assert-TicketRustProbeTestOutput $output $rustTest }

    $receiptJson = Get-Content -LiteralPath $receiptPath -Raw
    $receipt = $receiptJson | ConvertFrom-Json
    if ($nativeBinary) {
        $rustLabel = switch ($ProbeCase) {
            'rust-writer' { 'Rust writer' }
            'rust-immutable' { 'Rust immutable publication' }
            'rust-head' { 'Rust HEAD replacement' }
            'rust-graph' { 'Rust graph publication' }
        }
        Write-Output "Ticket $rustLabel probe verdict: $($receipt.verdict)"
    }
    $printBeforeValidation = $nativeBinary -or $ProbeCase -in @('publication', 'publication-process-exit', 'scope-identity')
    if ($printBeforeValidation) { Write-Output $receiptJson.TrimEnd() }
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
    } elseif ($ProbeCase -eq 'publication') {
        Assert-PublicationProbe $receipt
    } elseif ($ProbeCase -eq 'publication-process-exit') {
        Assert-PublicationProcessExitProbe $receipt $process.Id
    } elseif ($ProbeCase -eq 'scope-identity') {
        Assert-ScopeIdentityProbe $receipt
    } elseif ($ProbeCase -eq 'rust-writer') {
        Assert-RustWriterProbe $receipt $process.Id
    } elseif ($ProbeCase -eq 'rust-immutable') {
        Assert-RustImmutableProbe $receipt $process.Id
    } elseif ($ProbeCase -eq 'rust-head') {
        Assert-RustHeadProbe $receipt $process.Id
    } elseif ($ProbeCase -eq 'rust-graph') {
        Assert-RustGraphProbe $receipt $process.Id
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
    if (-not $printBeforeValidation) { Write-Output $receiptJson.TrimEnd() }
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
            if ($createdWorkspace) {
                if ($ProbeCase -eq 'scope-identity') {
                    . (Join-Path $PSScriptRoot 'windows-ticket-scope-identity-cases.ps1')
                    Remove-TicketScopeJunction (Join-Path $workspace 'scratch/fixture')
                }
                Remove-Item -LiteralPath $workspace -Recurse -Force
            }
        }
    }
}
