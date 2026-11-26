function Start-TicketProbeChild {
    param($Role, $EntryScript, [string[]]$Arguments)

    $launch = [System.Diagnostics.ProcessStartInfo]::new((Join-Path $PSHOME 'pwsh.exe'))
    $launch.UseShellExecute = $false
    $launch.WorkingDirectory = [IO.Path]::GetDirectoryName($EntryScript)
    $launch.RedirectStandardInput = $true
    $launch.RedirectStandardOutput = $true
    $launch.RedirectStandardError = $true
    foreach ($argument in (@('-NoLogo', '-NoProfile', '-NonInteractive', '-File', $EntryScript) + $Arguments)) {
        $launch.ArgumentList.Add($argument)
    }
    $process = [System.Diagnostics.Process]::Start($launch)
    $errorBuffer = [char[]]::new(4097)
    return @{
        Process = $process
        Role = $Role
        ErrorBuffer = $errorBuffer
        ErrorRead = $process.StandardError.ReadBlockAsync($errorBuffer, 0, $errorBuffer.Length)
    }
}

function Start-TicketWriterChild {
    param($Role, $ExpectedSid, $FixtureDirectory, $ReceiptPath, $EntryScript, $ExpectedLockId)

    $arguments = @(
        '-ExpectedSid', $ExpectedSid, '-FixtureDirectory', $FixtureDirectory,
        '-ReceiptPath', $ReceiptPath, '-ProbeCase', 'writer-lock', '-WriterRole', $Role
    )
    if ($ExpectedLockId) { $arguments += @('-WriterExpectedLockId', $ExpectedLockId) }
    return Start-TicketProbeChild -Role $Role -EntryScript $EntryScript -Arguments $arguments
}

function Stop-TicketProbeChild {
    param($Child)

    try {
        if (-not $Child.Process.HasExited) {
            $Child.Process.Kill($true)
            if (-not $Child.Process.WaitForExit(3000)) {
                throw "Ticket probe child '$($Child.Role)' failed to stop."
            }
        }
    } finally {
        $Child.Process.Dispose()
    }
}

function Read-TicketProbeChildReceipt {
    param($Child)

    # The holder keeps stdout open after its ready receipt, so read one bounded
    # line while draining bounded stderr concurrently.
    $buffer = [char[]]::new(32769)
    $count = 0
    $deadline = [System.Diagnostics.Stopwatch]::StartNew()
    while ($count -lt $buffer.Length) {
        $remaining = [TimeSpan]::FromSeconds(20) - $deadline.Elapsed
        if ($remaining -le [TimeSpan]::Zero) {
            throw "Ticket probe child '$($Child.Role)' timed out before its receipt."
        }
        $readTask = $Child.Process.StandardOutput.ReadAsync($buffer, $count, $buffer.Length - $count)
        $read = $readTask.WaitAsync($remaining).GetAwaiter().GetResult()
        if ($read -eq 0) { break }
        $lineEnd = [Array]::IndexOf($buffer, [char]10, $count, $read)
        $count += $read
        if ($lineEnd -ge 0) {
            if ($lineEnd -ne $count - 1) {
                throw "Ticket probe child '$($Child.Role)' emitted extra stdout."
            }
            $line = [string]::new($buffer, 0, $lineEnd).TrimEnd([char]13)
            return ConvertFrom-Json -InputObject $line -AsHashtable -Depth 16
        }
    }

    if ($count -eq $buffer.Length) {
        throw "Ticket probe child '$($Child.Role)' exceeded the stdout receipt limit."
    }
    Complete-TicketProbeChild $Child
    throw "Ticket probe child '$($Child.Role)' did not emit a complete receipt."
}

function Complete-TicketProbeChild {
    param($Child, [int]$ExpectedExitCode = 0)

    $process = $Child.Process
    if (-not $process.WaitForExit(20000)) {
        throw "Ticket probe child '$($Child.Role)' did not exit."
    }
    $limit = [TimeSpan]::FromSeconds(2)
    $errorLength = $Child.ErrorRead.WaitAsync($limit).GetAwaiter().GetResult()
    if ($errorLength -eq $Child.ErrorBuffer.Length) {
        throw "Ticket probe child '$($Child.Role)' exceeded the stderr limit."
    }
    if ($process.ExitCode -ne $ExpectedExitCode -or $errorLength -ne 0) {
        $errorText = [string]::new($Child.ErrorBuffer, 0, $errorLength).TrimEnd()
        throw "Ticket probe child '$($Child.Role)' exited $($process.ExitCode). $errorText"
    }
    $extra = [char[]]::new(1)
    if ($process.StandardOutput.ReadAsync($extra, 0, 1).WaitAsync($limit).GetAwaiter().GetResult() -ne 0) {
        throw "Ticket probe child '$($Child.Role)' emitted extra stdout."
    }
}

function Invoke-TicketWriterChild {
    param($Role, $ExpectedSid, $FixtureDirectory, $ReceiptPath, $EntryScript, $ExpectedLockId)

    $child = Start-TicketWriterChild @PSBoundParameters
    try {
        $result = Read-TicketProbeChildReceipt $child
        Complete-TicketProbeChild $child
        if ($result.pid -ne $child.Process.Id) {
            throw "Ticket probe child '$Role' did not attest its own process identity."
        }
        return $result
    } finally {
        Stop-TicketProbeChild $child
    }
}

function Get-TicketWriterLockReceipt {
    param($Scope, $Handle, $FixtureDirectory)

    return @{
        pid = $PID
        fixture = $FixtureDirectory
        token = $Scope.Token
        volume = $Scope.Volume
        file_id = $Handle.FileId
        volume_serial = $Handle.VolumeSerial
        operation = $Handle.TryLock()
    }
}

function Assert-TicketWriterLockReceipt {
    param($Receipt, $ExpectedSid, $ExpectedFileId, $ExpectedVolumeSerial, [int]$Win32, $Context)

    if ($Receipt.token.sid -cne $ExpectedSid -or
        $Receipt.file_id -cne $ExpectedFileId -or
        $Receipt.volume_serial -cne $ExpectedVolumeSerial -or
        $Receipt.operation.attempted -ne $true -or
        $Receipt.operation.succeeded -ne ($Win32 -eq 0) -or
        $Receipt.operation.win32_error -ne $Win32) {
        throw "Unexpected writer lock result ($Context). $($Receipt | ConvertTo-Json -Depth 16 -Compress)"
    }
}

function Invoke-TicketWriterLockCases {
    param(
        [string]$ExpectedSid,
        [string]$FixtureDirectory,
        [string]$ReceiptPath,
        [string]$EntryScript,
        [ValidateSet('primary', 'contender', 'alias-contender', 'name-contender', 'exit-holder')]
        [string]$Role,
        [string]$ExpectedLockId
    )

    $scope = [WindowsTicketDirectoryFlushProbe]::OpenWriterScope(
        $ExpectedSid, $FixtureDirectory, ($Role -eq 'primary'))
    try {
        if ($Role -eq 'name-contender') {
            if ($ExpectedLockId -cnotmatch '^[0-9a-f]{32}$') {
                throw 'The name contender requires the held lock file identity.'
            }
            $names = $scope.ProbeNames($ExpectedLockId)
            return @{
                pid = $PID
                fixture = $FixtureDirectory
                token = $scope.Token
                volume = $scope.Volume
                file_id = $names.initial_file_id
                volume_serial = $scope.Volume.volume_serial
                names = $names
            }
        }

        $handle = $scope.OpenLock(($Role -eq 'primary'))
        try {
            $owner = Get-TicketWriterLockReceipt $scope $handle $FixtureDirectory
            if ($Role -eq 'exit-holder') {
                Assert-TicketWriterLockReceipt $owner $ExpectedSid $owner.file_id $owner.volume_serial 0 'exit holder'
                [Console]::WriteLine(($owner | ConvertTo-Json -Depth 16 -Compress))
                [Console]::Out.Flush()
                # Exit with the lock handle live. The process must lose it at
                # OS teardown rather than through the normal disposal path.
                if ([Console]::ReadLine() -ceq 'exit') { [Environment]::Exit(0) }
                [Environment]::Exit(3)
            }
            if ($Role -ne 'primary') { return $owner }

            $fileId = $owner.file_id
            $volumeSerial = $owner.volume_serial
            if ($fileId -cnotmatch '^[0-9a-f]{32}$' -or $volumeSerial -cnotmatch '^[0-9a-f]{16}$') {
                throw 'The opened writer lock did not attest its physical file and volume identities.'
            }
            Assert-TicketWriterLockReceipt $owner $ExpectedSid $fileId $volumeSerial 0 'primary owner'

            $independent = $scope.OpenLock($false)
            try {
                $sameProcess = Get-TicketWriterLockReceipt $scope $independent $FixtureDirectory
                Assert-TicketWriterLockReceipt $sameProcess $ExpectedSid $fileId $volumeSerial 33 'independent handle'
            } finally {
                $independent.Dispose()
            }

            $childArgs = @{
                ExpectedSid = $ExpectedSid
                FixtureDirectory = $FixtureDirectory
                ReceiptPath = $ReceiptPath
                EntryScript = $EntryScript
            }
            $contender = Invoke-TicketWriterChild -Role contender @childArgs
            Assert-TicketWriterLockReceipt $contender $ExpectedSid $fileId $volumeSerial 33 'contender'
            $upperCase = $FixtureDirectory.ToUpperInvariant()
            if ($upperCase -ceq $FixtureDirectory) {
                throw 'The writer fixture path has no independent uppercase spelling.'
            }
            $childArgs.FixtureDirectory = $upperCase
            $alias = Invoke-TicketWriterChild -Role alias-contender @childArgs
            $childArgs.FixtureDirectory = $FixtureDirectory
            Assert-TicketWriterLockReceipt $alias $ExpectedSid $fileId $volumeSerial 33 'uppercase alias contender'
            if ($alias.fixture -cne $upperCase -or
                $alias.volume.fixture_directory_file_id -cne $owner.volume.fixture_directory_file_id) {
                throw 'The uppercase alias did not attest the same pinned fixture directory.'
            }
        } finally {
            $handle.Dispose()
        }

        $clean = Invoke-TicketWriterChild -Role contender @childArgs
        Assert-TicketWriterLockReceipt $clean $ExpectedSid $fileId $volumeSerial 0 'after clean release'

        $holder = Start-TicketWriterChild -Role exit-holder @childArgs
        try {
            $exitHolder = Read-TicketProbeChildReceipt $holder
            if ($exitHolder.pid -ne $holder.Process.Id -or $holder.Process.HasExited) {
                throw 'The exit holder did not attest a live process.'
            }
            Assert-TicketWriterLockReceipt $exitHolder $ExpectedSid $fileId $volumeSerial 0 'exit holder ready'
            $whileHeld = Invoke-TicketWriterChild -Role contender @childArgs
            Assert-TicketWriterLockReceipt $whileHeld $ExpectedSid $fileId $volumeSerial 33 'while exit holder is live'
            if ($holder.Process.HasExited) { throw 'The exit holder dropped before the exit signal.' }
            $holder.Process.StandardInput.WriteLine('exit')
            $holder.Process.StandardInput.Close()
            Complete-TicketProbeChild $holder
        } finally {
            Stop-TicketProbeChild $holder
        }

        $exitAttempts = [System.Collections.Generic.List[object]]::new()
        # Windows may release terminated-process locks after process exit.
        # Try at most three independently opened contenders, retrying only 33.
        for ($attempt = 0; $attempt -lt 3; $attempt++) {
            $afterExit = Invoke-TicketWriterChild -Role contender @childArgs
            $exitAttempts.Add($afterExit)
            if ($afterExit.operation.win32_error -ne 33) { break }
            Assert-TicketWriterLockReceipt $afterExit $ExpectedSid $fileId $volumeSerial 33 'process exit teardown'
        }
        Assert-TicketWriterLockReceipt $afterExit $ExpectedSid $fileId $volumeSerial 0 'after process exit'

        # Keep this challenge last. A permitted name change must remain
        # visible in the receipt without invalidating earlier lock controls.
        $handle = $scope.OpenLock($false)
        try {
            $nameOwner = Get-TicketWriterLockReceipt $scope $handle $FixtureDirectory
            Assert-TicketWriterLockReceipt $nameOwner $ExpectedSid $fileId $volumeSerial 0 'name challenge owner'
            $nameContender = Invoke-TicketWriterChild -Role name-contender -ExpectedLockId $fileId @childArgs
            if ($nameContender.token.sid -cne $ExpectedSid -or
                $nameContender.file_id -cne $fileId -or
                $nameContender.volume_serial -cne $volumeSerial) {
                throw 'The name contender did not attest the held writer lock identity.'
            }
        } finally {
            $handle.Dispose()
        }

        return @{
            schema = 1
            probe = 'writer-lock'
            process_architecture = [System.Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture.ToString()
            windows_version = [Environment]::OSVersion.Version.ToString()
            token = $scope.Token
            volume = $scope.Volume
            ordered_crash_persistence_proven = $false
            verdict = if ($nameContender.names.qualified) { 'available' } else { 'unavailable' }
            writer_lock_name_stable = [bool]$nameContender.names.name_preserved
            writer_lock = @{
                owner = $owner
                same_process = $sameProcess
                contender = $contender
                alias_contender = $alias
                after_clean_release = $clean
                exit_holder = $exitHolder
                while_exit_holder_held = $whileHeld
                exit_holder_exited = $true
                after_process_exit = $afterExit
                after_process_exit_attempts = $exitAttempts.ToArray()
                name_owner = $nameOwner
                name_contender = $nameContender
            }
        }
    } finally {
        $scope.Dispose()
    }
}
