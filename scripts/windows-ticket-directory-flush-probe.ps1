#Requires -Version 7.2
[CmdletBinding()]
param(
  [Parameter(Mandatory)][string]$ExpectedSid,
  [Parameter(Mandatory)][string]$FixtureDirectory,
  [Parameter(Mandatory)][string]$ReceiptPath,
  [ValidateSet('directory-flush', 'writer-lock', 'publication')][string]$ProbeCase = 'directory-flush',
  [ValidateSet('primary', 'contender', 'alias-contender', 'name-contender', 'exit-holder')]
  [string]$WriterRole = 'primary',
  [ValidatePattern('^(?:[0-9a-f]{32})?$')][string]$WriterExpectedLockId = ''
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
if (-not $IsWindows) { throw 'The Ticket platform probe requires native Windows.' }
if ($ProbeCase -ne 'writer-lock' -and ($WriterRole -ne 'primary' -or $WriterExpectedLockId)) {
  throw 'Writer lock process roles require the writer-lock probe.'
}

$interopSource = @'
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.Principal;
using System.Text;
using Microsoft.Win32.SafeHandles;

public static partial class WindowsTicketDirectoryFlushProbe
{
    private const string AdministratorsSid = "S-1-5-32-544";
    private const string LocalAccountSid = "S-1-5-113";
    private const string MediumIntegritySid = "S-1-16-8192";
    private const int TokenGroups = 2, TokenPrivileges = 3, TokenElevation = 20, TokenIntegrityLevel = 25;
    private const int ErrorFileNotFound = 2, ErrorBadLength = 24, ErrorInsufficientBuffer = 122;
    private const uint DriveFixed = 3, FileTypeDisk = 1;
    private const uint GenericRead = 0x80000000, GenericWrite = 0x40000000, ReadAttributes = 0x80;
    private const uint ShareRead = 1, ShareWrite = 2, CreateNew = 1, OpenExisting = 3;
    private const uint BackupSemantics = 0x02000000, OpenReparsePoint = 0x00200000;
    private const uint DirectoryAttribute = 0x10, ReparseAttribute = 0x400;
    private const int FileAttributeTagInfo = 9, FileIdInfo = 18;
    private const string ExtendedPathPrefix = @"\\?\";

    [StructLayout(LayoutKind.Sequential)]
    private struct SidAndAttributes
    {
        public IntPtr Sid;
        public uint Attributes;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct TokenGroupList
    {
        public uint Count;
        public SidAndAttributes First;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct Luid
    {
        public uint Low;
        public int High;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct PrivilegeEntry
    {
        public Luid Id;
        public uint Attributes;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct AttributeTag
    {
        public uint Attributes;
        public uint ReparseTag;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct FileIdentity
    {
        public ulong VolumeSerial;
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = 16)] public byte[] FileId;

        public string Id => BitConverter.ToString(FileId).Replace("-", "").ToLowerInvariant();
        public string Volume => VolumeSerial.ToString("x16");
    }

    [DllImport("advapi32.dll", ExactSpelling = true, SetLastError = true)]
    private static extern bool GetTokenInformation(SafeAccessTokenHandle token, int informationClass,
        IntPtr buffer, uint bufferSize, out uint required);

    [DllImport("advapi32.dll", EntryPoint = "GetTokenInformation", ExactSpelling = true, SetLastError = true)]
    private static extern bool ReadTokenElevation(SafeAccessTokenHandle token, int informationClass,
        out uint elevation, uint bufferSize, out uint required);

    [DllImport("advapi32.dll", CharSet = CharSet.Unicode, ExactSpelling = true, SetLastError = true)]
    private static extern bool LookupPrivilegeValueW(string system, string name, out Luid id);

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, ExactSpelling = true, SetLastError = true)]
    private static extern SafeFileHandle CreateFileW(string name, uint access, uint share,
        IntPtr security, uint creation, uint flags, IntPtr template);

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, ExactSpelling = true)]
    private static extern uint GetDriveTypeW(string root);

    [DllImport("kernel32.dll", ExactSpelling = true, SetLastError = true)]
    private static extern uint GetFileType(SafeFileHandle handle);

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, ExactSpelling = true, SetLastError = true)]
    private static extern uint GetFinalPathNameByHandleW(SafeFileHandle handle, StringBuilder path,
        uint capacity, uint flags);

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, ExactSpelling = true, SetLastError = true)]
    private static extern bool GetVolumeInformationByHandleW(SafeFileHandle handle, IntPtr volumeName,
        uint volumeNameLength, out uint serial, out uint maxComponentLength, out uint flags,
        StringBuilder fileSystem, uint fileSystemLength);

    [DllImport("kernel32.dll", EntryPoint = "GetFileInformationByHandleEx", SetLastError = true)]
    private static extern bool ReadFileAttributes(SafeFileHandle handle, int informationClass,
        out AttributeTag value, uint size);

    [DllImport("kernel32.dll", EntryPoint = "GetFileInformationByHandleEx", SetLastError = true)]
    private static extern bool ReadFileIdentity(SafeFileHandle handle, int informationClass,
        out FileIdentity value, uint size);

    [DllImport("kernel32.dll", ExactSpelling = true, SetLastError = true)]
    private static extern bool WriteFile(SafeFileHandle handle, byte[] bytes, uint count,
        out uint written, IntPtr overlapped);

    [DllImport("kernel32.dll", ExactSpelling = true, SetLastError = true)]
    private static extern bool FlushFileBuffers(SafeFileHandle handle);

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, ExactSpelling = true, SetLastError = true)]
    private static extern bool MoveFileExW(string source, string destination, uint flags);

    private static Win32Exception WindowsError(string operation, int error) =>
        new Win32Exception(error, $"{operation} failed with Win32 error {error}");

    private static void RequireWin32(bool succeeded, string operation)
    {
        if (!succeeded) throw WindowsError(operation, FailureCode(operation));
    }

    private static int FailureCode(string operation)
    {
        int error = Marshal.GetLastWin32Error();
        if (error == 0) throw new InvalidOperationException($"{operation} failed without a Win32 error.");
        return error;
    }

    private static T InspectToken<T>(SafeAccessTokenHandle token, int informationClass, Func<IntPtr, T> inspect)
    {
        bool succeeded = GetTokenInformation(token, informationClass, IntPtr.Zero, 0, out uint size);
        int error = Marshal.GetLastWin32Error();
        if (succeeded || size == 0 || (error != ErrorInsufficientBuffer && error != ErrorBadLength))
            throw WindowsError($"Measure token information {informationClass}", error);

        IntPtr buffer = Marshal.AllocHGlobal(checked((int)size));
        try
        {
            RequireWin32(GetTokenInformation(token, informationClass, buffer, size, out _),
                $"Read token information {informationClass}");
            return inspect(buffer);
        }
        finally { Marshal.FreeHGlobal(buffer); }
    }

    private static Dictionary<string, object> AttestToken(string expectedSid)
    {
        using (WindowsIdentity identity = WindowsIdentity.GetCurrent())
        {
            SafeAccessTokenHandle token = identity.AccessToken;
            string sid = identity.User.Value;
            RequireWin32(ReadTokenElevation(token, TokenElevation, out uint elevation, sizeof(uint),
                out uint elevationSize), "Read token elevation");
            if (elevationSize != sizeof(uint)) throw new InvalidOperationException("Unexpected token elevation size.");
            bool elevated = elevation != 0;
            string integrity = InspectToken(token, TokenIntegrityLevel, p =>
                new SecurityIdentifier(Marshal.PtrToStructure<SidAndAttributes>(p).Sid).Value);
            var groups = InspectToken(token, TokenGroups, p =>
            {
                // Read the raw array so a deny-only administrator SID cannot be hidden.
                var result = new HashSet<string>(StringComparer.Ordinal);
                int offset = Marshal.OffsetOf<TokenGroupList>(nameof(TokenGroupList.First)).ToInt32();
                int stride = Marshal.SizeOf<SidAndAttributes>();
                int count = Marshal.ReadInt32(p);
                for (int index = 0; index < count; index++)
                {
                    var group = Marshal.PtrToStructure<SidAndAttributes>(p + offset + index * stride);
                    result.Add(new SecurityIdentifier(group.Sid).Value);
                }
                return result;
            });
            var excessPrivileges = InspectToken(token, TokenPrivileges, p =>
            {
                var result = new List<string>();
                int count = Marshal.ReadInt32(p);
                int stride = Marshal.SizeOf<PrivilegeEntry>();
                foreach (string name in new[] { "SeBackupPrivilege", "SeRestorePrivilege", "SeManageVolumePrivilege" })
                {
                    RequireWin32(LookupPrivilegeValueW(null, name, out Luid id), $"Resolve {name}");
                    for (int index = 0; index < count; index++)
                    {
                        var privilege = Marshal.PtrToStructure<PrivilegeEntry>(p + sizeof(uint) + index * stride);
                        if (privilege.Id.Low == id.Low && privilege.Id.High == id.High)
                        {
                            result.Add(name);
                            break;
                        }
                    }
                }
                return result;
            });

            bool local = groups.Contains(LocalAccountSid);
            bool administrators = groups.Contains(AdministratorsSid);
            if (sid != expectedSid || !local || elevated || integrity != MediumIntegritySid ||
                administrators || excessPrivileges.Count != 0)
                throw new InvalidOperationException($"Unexpected probe token: SID={sid}, local={local}, " +
                    $"elevated={elevated}, integrity={integrity}, administrators={administrators}, " +
                    $"excess privileges={string.Join(",", excessPrivileges)}");

            return new Dictionary<string, object>
            {
                ["sid"] = sid, ["elevated"] = elevated, ["integrity_sid"] = integrity,
                ["local_account_group_present"] = local,
                ["administrators_group_present"] = administrators,
                ["excess_privileges_present"] = excessPrivileges
            };
        }
    }

    private static SafeFileHandle Open(string path, uint access, uint share, uint creation, uint flags) =>
        CreateFileW(ExtendedPathPrefix + path, access, share, IntPtr.Zero, creation,
            flags | OpenReparsePoint, IntPtr.Zero);

    private static FileIdentity InspectName(SafeFileHandle handle, string path, bool directory)
    {
        RequireWin32(!handle.IsInvalid, $"Open {path}");
        if (GetFileType(handle) != FileTypeDisk)
            throw new InvalidOperationException($"Expected a disk object: {path}");
        RequireWin32(ReadFileAttributes(handle, FileAttributeTagInfo, out AttributeTag attributes,
            (uint)Marshal.SizeOf<AttributeTag>()), $"Read attributes for {path}");
        if ((attributes.Attributes & ReparseAttribute) != 0 ||
            ((attributes.Attributes & DirectoryAttribute) != 0) != directory)
            throw new InvalidOperationException($"Unexpected object type or reparse point: {path}");

        var actual = new StringBuilder(1024);
        uint length = GetFinalPathNameByHandleW(handle, actual, (uint)actual.Capacity, 0);
        RequireWin32(length != 0, $"Read the final name for {path}");
        string actualName = actual.ToString().TrimEnd('\\');
        string expectedName = ExtendedPathPrefix + path.TrimEnd('\\');
        if (length >= actual.Capacity || !String.Equals(actualName, expectedName, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException($"Probe path is not bound to the expected local name: {path}");

        RequireWin32(ReadFileIdentity(handle, FileIdInfo, out FileIdentity identity,
            (uint)Marshal.SizeOf<FileIdentity>()), $"Read identity for {path}");
        return identity;
    }

    private static int Flush(SafeFileHandle handle) =>
        FlushFileBuffers(handle) ? 0 : FailureCode("FlushFileBuffers");

    private static void RequireKnownDirectoryResult(int? error, string operation)
    {
        // An unsupported operation or access on this exact attested fixture is evidence.
        // Unrelated failures such as sharing violations and I/O errors invalidate the probe.
        if (error.HasValue && error != 0 && error != 1 && error != 5 && error != 6 && error != 50 && error != 87)
            throw WindowsError(operation, error.Value);
    }

    private static Dictionary<string, object> Operation(int? error) => new Dictionary<string, object>
    {
        ["attempted"] = error.HasValue, ["succeeded"] = error == 0, ["win32_error"] = error
    };

    public static Dictionary<string, object> Run(string expectedSid, string fixtureDirectory)
    {
        Dictionary<string, object> token = AttestToken(expectedSid);
        string drive = Path.GetPathRoot(fixtureDirectory);
        char letter = String.IsNullOrEmpty(drive) ? '\0' : Char.ToUpperInvariant(drive[0]);
        if (drive == null || drive.Length != 3 || drive[1] != ':' || drive[2] != '\\' ||
            letter < 'A' || letter > 'Z' || fixtureDirectory.Length <= drive.Length ||
            GetDriveTypeW(drive) != DriveFixed)
            throw new InvalidOperationException("The probe requires a fresh fixture on a fixed local drive.");

        // CreateFileW requires BackupSemantics for directory handles even without backup privileges.
        using (SafeFileHandle driveRoot = Open(drive, ReadAttributes, ShareRead | ShareWrite, OpenExisting, BackupSemantics))
        using (SafeFileHandle fixture = Open(fixtureDirectory, ReadAttributes, ShareRead | ShareWrite,
            OpenExisting, BackupSemantics))
        {
            FileIdentity rootId = InspectName(driveRoot, drive, true);
            FileIdentity fixtureId = InspectName(fixture, fixtureDirectory, true);
            var fileSystem = new StringBuilder(32);
            RequireWin32(GetVolumeInformationByHandleW(fixture, IntPtr.Zero, 0, out uint volumeSerial,
                out _, out uint volumeFlags, fileSystem, (uint)fileSystem.Capacity), "Inspect the fixture volume");
            if (fileSystem.ToString() != "NTFS" || rootId.VolumeSerial != fixtureId.VolumeSerial)
                throw new InvalidOperationException("The bound fixture is not on the fixed local NTFS drive.");
            using (var names = Directory.EnumerateFileSystemEntries(fixtureDirectory).GetEnumerator())
                if (names.MoveNext()) throw new InvalidOperationException("The fixture directory is not empty.");

            var bytes = Encoding.ASCII.GetBytes("Bamboo Ticket directory flush probe\n");
            string stagingName = Path.Combine(fixtureDirectory, "staged.bin");
            string publishedName = Path.Combine(fixtureDirectory, "published.bin");
            FileIdentity stagedId;
            using (SafeFileHandle staged = Open(stagingName, GenericWrite | ReadAttributes, 0, CreateNew, 0))
            {
                stagedId = InspectName(staged, stagingName, false);
                if (stagedId.VolumeSerial != fixtureId.VolumeSerial)
                    throw new InvalidOperationException("The staged file is on a different volume.");
                RequireWin32(WriteFile(staged, bytes, (uint)bytes.Length, out uint written, IntPtr.Zero),
                    "Write the positive-control staged file");
                if (written != bytes.Length) throw new IOException("Incomplete staged file write.");
                int error = Flush(staged);
                if (error != 0) throw WindowsError("Flush the positive-control staged file", error);
            }

            using (SafeFileHandle writableDirectory = Open(fixtureDirectory, GenericWrite | ReadAttributes,
                ShareRead | ShareWrite, OpenExisting, BackupSemantics))
            {
                int openError = writableDirectory.IsInvalid ? FailureCode("CreateFileW") : 0;
                RequireKnownDirectoryResult(openError, "Open the writable directory handle");
                FileIdentity? writableId = null;
                int? beforeError = null;
                int? afterError = null;
                if (openError == 0)
                {
                    writableId = InspectName(writableDirectory, fixtureDirectory, true);
                    if (writableId.Value.VolumeSerial != fixtureId.VolumeSerial || writableId.Value.Id != fixtureId.Id)
                        throw new InvalidOperationException("The writable handle does not identify the fixture directory.");
                    beforeError = Flush(writableDirectory);
                    RequireKnownDirectoryResult(beforeError, "Flush the directory before publication");
                }

                // Flags=0 requires one same-volume move to an absent destination.
                RequireWin32(MoveFileExW(ExtendedPathPrefix + stagingName, ExtendedPathPrefix + publishedName, 0),
                    "Publish the scratch sibling name");
                if (openError == 0)
                {
                    afterError = Flush(writableDirectory);
                    RequireKnownDirectoryResult(afterError, "Flush the directory after publication");
                }

                FileIdentity publishedId;
                using (SafeFileHandle published = Open(publishedName, GenericRead, ShareRead, OpenExisting, 0))
                {
                    publishedId = InspectName(published, publishedName, false);
                    if (publishedId.VolumeSerial != stagedId.VolumeSerial || publishedId.Id != stagedId.Id)
                        throw new InvalidOperationException("Warm reopen did not retain the staged file identity.");
                    using (var file = new FileStream(published, FileAccess.Read))
                    {
                        foreach (byte expected in bytes)
                            if (file.ReadByte() != expected) throw new IOException("Unexpected published bytes.");
                        if (file.ReadByte() != -1) throw new IOException("Unexpected published suffix.");
                    }
                }
                using (SafeFileHandle staged = Open(stagingName, ReadAttributes, ShareRead, OpenExisting, 0))
                {
                    if (!staged.IsInvalid) throw new IOException("The staged name remains after publication.");
                    int missing = FailureCode("Reopen the old staged name");
                    if (missing != ErrorFileNotFound) throw WindowsError("Reopen the old staged name", missing);
                }

                return new Dictionary<string, object>
                {
                    ["schema"] = 1,
                    ["windows_version"] = Environment.OSVersion.Version.ToString(),
                    ["process_architecture"] = RuntimeInformation.ProcessArchitecture.ToString(),
                    ["token"] = token,
                    ["volume"] = new Dictionary<string, object>
                    {
                        ["file_system"] = fileSystem.ToString(), ["drive_type"] = "fixed", ["drive_type_code"] = DriveFixed,
                        ["drive_root"] = drive, ["volume_serial"] = fixtureId.Volume,
                        ["file_system_serial"] = volumeSerial.ToString("x8"), ["file_system_flags"] = volumeFlags,
                        ["drive_root_file_id"] = rootId.Id, ["fixture_directory_file_id"] = fixtureId.Id,
                        ["writable_directory_file_id"] = writableId?.Id
                    },
                    ["staged_file_flush"] = Operation(0),
                    ["staged_file_id"] = stagedId.Id,
                    ["directory_open"] = Operation(openError),
                    ["directory_flush_before"] = Operation(beforeError),
                    ["directory_flush_after"] = Operation(afterError),
                    ["scratch_name_publication"] = Operation(0),
                    ["warm_reopen"] = new Dictionary<string, object>
                    {
                        ["succeeded"] = true, ["published_file_id"] = publishedId.Id, ["bytes"] = bytes.Length
                    },
                    ["verdict"] = beforeError == 0 && afterError == 0 ? "available" : "unavailable",
                    ["ordered_crash_persistence_proven"] = false
                };
            }
        }
    }
}
'@

if ($ProbeCase -ne 'directory-flush') {
  $nativeSource = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'windows-ticket-writer-lock-native.cs') -Raw
  $interopSource += [Environment]::NewLine + $nativeSource
}
if ($ProbeCase -eq 'publication') {
  $nativeSource = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'windows-ticket-publication-native.cs') -Raw
  $interopSource += [Environment]::NewLine + $nativeSource
}
Add-Type -TypeDefinition $interopSource

if (-not [IO.Path]::IsPathFullyQualified($FixtureDirectory) -or
    -not [IO.Path]::IsPathFullyQualified($ReceiptPath)) {
  throw 'Provide fully qualified fixture and receipt paths.'
}
$fixture = [IO.Path]::TrimEndingDirectorySeparator([IO.Path]::GetFullPath($FixtureDirectory))
$receiptFile = [IO.Path]::GetFullPath($ReceiptPath)
$receiptName = [IO.Path]::GetFileName($receiptFile)
if (-not [string]::Equals([IO.Path]::GetDirectoryName($receiptFile),
    [IO.Path]::GetDirectoryName($fixture), [StringComparison]::OrdinalIgnoreCase) -or
    [string]::IsNullOrWhiteSpace($receiptName) -or
    $receiptName.IndexOfAny([IO.Path]::GetInvalidFileNameChars()) -ge 0 -or
    $receiptFile.Equals($fixture, [StringComparison]::OrdinalIgnoreCase)) {
  throw 'The receipt must be a new sibling file beside the fresh fixture directory.'
}

if ($ProbeCase -eq 'writer-lock') {
  . (Join-Path $PSScriptRoot 'windows-ticket-writer-lock-cases.ps1')
  $caseArguments = @{
    ExpectedSid = $ExpectedSid
    FixtureDirectory = $fixture
    ReceiptPath = $receiptFile
    EntryScript = $PSCommandPath
    Role = $WriterRole
    ExpectedLockId = $WriterExpectedLockId
  }
  $receipt = Invoke-TicketWriterLockCases @caseArguments
  if ($WriterRole -eq 'exit-holder') { return }
  if ($WriterRole -ne 'primary') {
    [Console]::WriteLine((ConvertTo-Json -InputObject $receipt -Depth 16 -Compress))
    return
  }
  $probeLabel = 'writer lock'
} elseif ($ProbeCase -eq 'publication') {
  $receipt = [WindowsTicketDirectoryFlushProbe]::RunPublication($ExpectedSid, $fixture)
  $probeLabel = 'publication'
} else {
  $receipt = [WindowsTicketDirectoryFlushProbe]::Run($ExpectedSid, $fixture)
  $probeLabel = 'directory flush'
}
$json = ConvertTo-Json -InputObject $receipt -Depth 16
$stream = [IO.File]::Open($receiptFile, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::Read)
try {
  $utf8 = [Text.UTF8Encoding]::new($false).GetBytes($json + [Environment]::NewLine)
  $stream.Write($utf8, 0, $utf8.Length)
} finally {
  $stream.Dispose()
}
Write-Output "Ticket $probeLabel probe verdict: $($receipt['verdict'])"
