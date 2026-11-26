function Start-TicketPublicationProcess {
    param($Role, $Cut, $ExpectedSid, $Scope, $ReceiptPath, $EntryScript)

    $arguments = @(
        '-ExpectedSid', $ExpectedSid, '-FixtureDirectory', $Scope, '-ReceiptPath', $ReceiptPath,
        '-ProbeCase', 'publication-process-exit', '-ProcessRole', $Role
    )
    if ($Role -eq 'writer') { $arguments += @('-ProcessCut', $Cut) }
    return Start-TicketProbeChild -Role "$Role/$Cut" -EntryScript $EntryScript -Arguments $arguments
}

function Assert-TicketPublicationProcessFrame {
    param($Frame, $Child, [string]$Role, [string]$ExpectedSid)

    if ($Frame.schema -ne 1 -or $Frame.probe -cne 'publication-process-exit' -or
        $Frame.pid -ne $Child.Process.Id -or $Frame.pid -eq $PID -or $Frame.role -cne $Role -or
        $Frame.process_architecture -cne 'X64' -or $Frame.ordered_crash_persistence_proven -ne $false -or
        $Frame.token.sid -cne $ExpectedSid -or $Frame.token.elevated -ne $false -or
        $Frame.token.integrity_sid -cne 'S-1-16-8192' -or
        $Frame.token.local_account_group_present -ne $true -or
        $Frame.token.administrators_group_present -ne $false -or
        @($Frame.token.excess_privileges_present).Count -ne 0 -or
        $Frame.volume.file_system -cne 'NTFS' -or $Frame.volume.drive_type_code -ne 3 -or
        $Frame.volume.fixture_directory_file_id -cnotmatch '^[0-9a-f]{32}$' -or
        $Frame.volume.volume_serial -cnotmatch '^[0-9a-f]{16}$' -or
        $Frame.qualified -isnot [bool]) {
        throw "Invalid standard-user process attestation for '$($Child.Role)'."
    }
    if ($Frame.qualified -and ($Frame.verdict -cne 'available' -or
        $Frame.lock.file_id -cnotmatch '^[0-9a-f]{32}$' -or
        $Frame.lock.volume_serial -cne $Frame.volume.volume_serial -or
        $Frame.lock.acquisition.attempted -ne $true -or
        $Frame.lock.acquisition.succeeded -ne $true -or $Frame.lock.acquisition.win32_error -ne 0)) {
        throw "The qualified process '$($Child.Role)' did not attest its lock acquisition."
    }
}

function Assert-TicketPublicationWriterCut {
    param($Writer, [string]$Cut)

    $expectRename = $Cut -ne 'before-head-replace'
    $expectFlush = $Cut -in @('complete', 'after-scope-flush')
    $head = $Writer.operations.head
    if ($Writer.cut -cne $Cut -or $head.stage_flush.attempted -ne $true -or
        $head.stage_flush.succeeded -ne $true -or $head.stage_flush.win32_error -ne 0 -or
        $head.Contains('rename') -ne $expectRename -or $head.Contains('directory_flush') -ne $expectFlush -or
        $Writer.old_head.file_id -ceq $Writer.staged_head.file_id -or
        $Writer.old_head.commit_hash -cne $Writer.old_graph.commit_hash -or
        $Writer.staged_head.commit_hash -cne $Writer.new_graph.commit_hash -or
        $Writer.old_graph.sequence -ne 1 -or $Writer.new_graph.sequence -ne 2 -or
        $Writer.new_graph.parent -cne $Writer.old_graph.commit_hash) {
        throw "The native writer's recorded boundary does not match '$Cut'."
    }
    if ($expectRename -and ($head.rename.attempted -ne $true -or $head.rename.succeeded -ne $true -or
        $head.rename.win32_error -ne 0 -or $head.rename.ntstatus -cne '00000000' -or
        $head.rename.information_class -ne 65 -or $head.rename.flags -ne 3 -or
        $head.rename.destination -cne 'HEAD')) {
        throw "The writer did not successfully replace HEAD at '$Cut'."
    }
    if ($expectFlush -and ($head.directory_flush.attempted -ne $true -or
        $head.directory_flush.succeeded -ne $true -or $head.directory_flush.win32_error -ne 0 -or
        $head.directory_flush.file_id -cne $Writer.volume.fixture_directory_file_id)) {
        throw "The writer did not flush its observed scope at '$Cut'."
    }
}

function Assert-TicketPublicationReadback {
    param($Actual, $Expected, [string[]]$Fields, [string]$Context)

    if ($null -eq $Actual) { throw "Missing fresh readback for '$Context'." }
    foreach ($field in $Fields) {
        if ($Actual[$field] -cne $Expected[$field]) {
            throw "Fresh '$Context' differs from the expected $field."
        }
    }
}

function Assert-TicketPublicationReopen {
    param($Trial)

    $writer = $Trial.writer
    $verifier = $Trial.verifier
    if ($verifier.record -cne 'verification') { throw 'The verifier did not report an independent reopen.' }

    $headFields = @('file_id', 'volume_serial', 'commit_hash', 'content_hash', 'bytes_hex')
    $graphFields = @('sequence', 'parent', 'commit_hash', 'manifest_hash', 'object_hash')
    if ($Trial.cut -eq 'before-head-replace') {
        if ($verifier.stage_absent -ne $false) { throw 'The pre-replace staged HEAD must still exist.' }
        Assert-TicketPublicationReadback $verifier.named_head $writer.old_head $headFields 'old named HEAD'
        Assert-TicketPublicationReadback $verifier.graph $writer.old_graph $graphFields 'old published graph'
        Assert-TicketPublicationReadback $verifier.staged_head $writer.staged_head $headFields 'staged new HEAD'
        Assert-TicketPublicationReadback $verifier.staged_graph $writer.new_graph $graphFields 'unpublished new graph'
    } else {
        if ($verifier.stage_absent -ne $true -or $null -ne $verifier.staged_head) {
            throw 'The replaced staged name must be absent.'
        }
        Assert-TicketPublicationReadback $verifier.named_head $writer.staged_head $headFields 'new named HEAD'
        Assert-TicketPublicationReadback $verifier.graph $writer.new_graph $graphFields 'new published graph'
        Assert-TicketPublicationReadback $verifier.previous_graph $writer.old_graph $graphFields 'new HEAD parent graph'
    }
}

function Read-TicketPublicationProcessReopen {
    param($Trial, $ExpectedSid, $Scope, $ReceiptPath, $EntryScript)

    # Windows may release byte-range locks after the terminated process has
    # signalled. Only retry that exact denial against the same retained lock.
    $attempts = if ($Trial.writer_terminated) { 3 } else { 1 }
    for ($attempt = 1; $attempt -le $attempts; $attempt++) {
        $child = Start-TicketPublicationProcess 'verifier' $Trial.cut $ExpectedSid $Scope $ReceiptPath $EntryScript
        try {
            $verifier = Read-TicketProbeChildReceipt $child
            $Trial.verifier = $verifier
            Assert-TicketPublicationProcessFrame $verifier $child 'verifier' $ExpectedSid
            Complete-TicketProbeChild $child
            $writer = $Trial.writer
            if ($verifier.pid -eq $writer.pid -or $verifier.lock.file_id -cne $writer.lock.file_id -or
                $verifier.lock.volume_serial -cne $writer.lock.volume_serial -or
                $verifier.volume.fixture_directory_file_id -cne $writer.volume.fixture_directory_file_id) {
                throw 'The verifier did not attest a fresh process using the same physical scope and lock.'
            }
            if ($verifier.qualified) {
                Assert-TicketPublicationReopen $Trial
                return
            }
            $acquisition = $verifier.lock.acquisition
            if ($attempt -eq $attempts -or $acquisition.attempted -ne $true -or
                $acquisition.succeeded -ne $false -or $acquisition.win32_error -ne 33) {
                throw 'The native verifier did not qualify this reopen.'
            }
            $Trial.lock_teardown_attempts.Add($verifier)
        } finally {
            Stop-TicketProbeChild $child
        }
    }
}

function Invoke-TicketPublicationProcessCases {
    param([string]$ExpectedSid, [string]$FixtureDirectory, [string]$EntryScript)

    $trials = [ordered]@{}
    $attestation = $null
    $scopeIds = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($cut in @('complete', 'before-head-replace', 'after-head-replace', 'after-scope-flush')) {
        $scope = Join-Path $FixtureDirectory $cut
        $receiptPath = Join-Path $FixtureDirectory "$cut-receipt.json"
        $null = New-Item -ItemType Directory -Path $scope
        $trial = [ordered]@{
            cut = $cut
            writer = $null
            verifier = $null
            writer_exit_code = $null
            writer_terminated = $false
            termination_win32_error = $null
            lock_teardown_attempts = [System.Collections.Generic.List[object]]::new()
            qualified = $false
        }
        $trials[$cut] = $trial
        try {
            $child = Start-TicketPublicationProcess 'writer' $cut $ExpectedSid $scope $receiptPath $EntryScript
            try {
                $writer = Read-TicketProbeChildReceipt $child
                $trial.writer = $writer
                Assert-TicketPublicationProcessFrame $writer $child 'writer' $ExpectedSid
                if ($null -eq $attestation) { $attestation = $writer }
                if (-not $scopeIds.Add($writer.volume.fixture_directory_file_id) -or
                    $writer.volume.volume_serial -cne $attestation.volume.volume_serial) {
                    throw 'The publication trials did not retain independent scopes on the same volume.'
                }
                if (-not $writer.qualified) {
                    Complete-TicketProbeChild $child
                    throw 'The native writer did not qualify this boundary.'
                }
                Assert-TicketPublicationWriterCut $writer $cut
                if ($cut -eq 'complete') {
                    if ($writer.record -cne 'complete') { throw 'The uninterrupted control lacked its final reply.' }
                    Complete-TicketProbeChild $child
                } else {
                    if ($writer.record -cne 'cut_reached' -or $child.Process.HasExited) {
                        throw "The writer for '$cut' was not alive at its cut."
                    }
                    $exitCode = [WindowsTicketDirectoryFlushProbe]::PublicationProcessCutExitCode
                    $trial.termination_win32_error = [WindowsTicketDirectoryFlushProbe]::TerminatePublicationProcess(
                        $child.Process.SafeHandle)
                    if ($trial.termination_win32_error -ne 0) { throw "Could not terminate the writer for '$cut'." }
                    Complete-TicketProbeChild $child -ExpectedExitCode $exitCode
                    $trial.writer_terminated = $true
                }
            } finally {
                if ($child.Process.HasExited) { $trial.writer_exit_code = $child.Process.ExitCode }
                Stop-TicketProbeChild $child
            }

            Read-TicketPublicationProcessReopen $trial $ExpectedSid $scope $receiptPath $EntryScript
            if (Test-Path -LiteralPath $receiptPath) { throw 'A probe child wrote an unexpected receipt file.' }
            $trial.qualified = $true
        } catch {
            $trial.error = $_.Exception.Message
        }
    }
    if ($null -eq $attestation) {
        throw "No writer attested the ordinary-user publication fixture. $($trials | ConvertTo-Json -Depth 16 -Compress)"
    }
    $qualified = @($trials.Values | Where-Object { -not $_.qualified }).Count -eq 0
    return @{
        schema = 1
        probe = 'publication-process-exit'
        process_architecture = $attestation.process_architecture
        windows_version = $attestation.windows_version
        token = $attestation.token
        volume = $attestation.volume
        coordinator_pid = $PID
        ordered_crash_persistence_proven = $false
        process_exit_qualified = $qualified
        verdict = if ($qualified) { 'available' } else { 'unavailable' }
        cases = $trials
    }
}
