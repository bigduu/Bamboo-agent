function Remove-TicketScopeJunction {
    param([string]$FixtureDirectory)

    $source = Join-Path $FixtureDirectory 'junction-source'
    try {
        $sourceAttributes = [IO.File]::GetAttributes($source)
    } catch [IO.FileNotFoundException], [IO.DirectoryNotFoundException] {
        return
    }
    if (-not ($sourceAttributes -band [IO.FileAttributes]::Directory) -or
        ($sourceAttributes -band [IO.FileAttributes]::ReparsePoint)) {
        throw 'The owned junction parent is no longer a plain scratch directory.'
    }

    $bridge = Join-Path $source 'bridge'
    try {
        $attributes = [IO.File]::GetAttributes($bridge)
    } catch [IO.FileNotFoundException], [IO.DirectoryNotFoundException] {
        return
    }
    if (-not ($attributes -band [IO.FileAttributes]::Directory)) {
        throw 'The owned junction entry is no longer a directory.'
    }
    if (($attributes -band [IO.FileAttributes]::ReparsePoint) -and
        (Get-Item -LiteralPath $bridge -Force).LinkType -cne 'Junction') {
        throw 'The owned junction entry has an unexpected reparse type.'
    }
    # Remove the entry itself. Recursive workspace cleanup can follow later.
    [IO.Directory]::Delete($bridge, $false)
    try {
        $null = [IO.File]::GetAttributes($bridge)
    } catch [IO.FileNotFoundException], [IO.DirectoryNotFoundException] {
        return
    }
    throw 'The owned junction name is still present after nonrecursive removal.'
}

function Invoke-TicketScopeIdentityCases {
    param([string]$ExpectedSid, [string]$FixtureDirectory)

    $source = Join-Path $FixtureDirectory 'junction-source'
    $target = Join-Path $FixtureDirectory 'junction-target'
    $bridge = Join-Path $source 'bridge'
    $null = New-Item -ItemType Directory -Path $source
    $null = New-Item -ItemType Directory -Path $target
    $null = New-Item -ItemType Directory -Path (Join-Path $target 'child')
    $receipt = $null
    try {
        $null = New-Item -ItemType Junction -Path $bridge -Target $target
        $receipt = [WindowsTicketDirectoryFlushProbe]::RunScopeIdentity($ExpectedSid, $FixtureDirectory)
    } finally {
        if ($null -ne $receipt) { $receipt['junction_removed'] = $false }
        try {
            Remove-TicketScopeJunction $FixtureDirectory
            if ($null -ne $receipt) {
                $receipt['junction_removed'] = $true
                $junction = $receipt['scope_identity']['junction']
                if ($junction['qualified']) {
                    $expected = $junction['after']['target_child']['witness']['bytes_hex']
                    $targetWitness = Join-Path $target 'child/target-witness.bin'
                    $actual = [Convert]::ToHexString([IO.File]::ReadAllBytes($targetWitness)).ToLowerInvariant()
                    $receipt['junction_target_preserved_after_cleanup'] = $actual -ceq $expected
                    if (-not $receipt['junction_target_preserved_after_cleanup']) {
                        $receipt['scope_identity_qualified'] = $false
                        $receipt['verdict'] = 'unavailable'
                    }
                }
            }
        } catch {
            if ($null -eq $receipt) { throw }
            $receipt['junction_target_preserved_after_cleanup'] = $false
            $receipt['junction_cleanup_error'] = $_.Exception.Message
            $receipt['scope_identity_qualified'] = $false
            $receipt['verdict'] = 'unavailable'
        }
    }
    return $receipt
}
