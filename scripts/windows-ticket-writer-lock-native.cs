public static partial class WindowsTicketDirectoryFlushProbe
{
    private const string WriterName = "writer.lock";
    private const uint WriterSynchronize = 0x00100000, WriterDelete = 0x00010000;
    private const uint WriterDirectoryAccess = ReadAttributes | WriterSynchronize | 0x20; // FILE_TRAVERSE
    private const uint WriterShare = ShareRead | ShareWrite;
    private const uint WriterShareDelete = 4;
    private const uint NtOpen = 1, NtCreate = 2, NtOpenIf = 3;
    private const uint NtDirectory = 1, NtNonDirectory = 0x40;
    private const uint NtSynchronous = 0x20, NtOpenReparsePoint = 0x00200000;
    private const uint ObjCaseInsensitive = 0x40, ObjDontReparse = 0x1000;
    private const uint RenameReplace = 1, RenamePosix = 2, ExclusiveNonblocking = 3;
    private const int StandardFileInfo = 1, RenameInfo = 10, RenameInfoEx = 65;
    private const int ObjectNameNotFound = unchecked((int)0xc0000034), NoSuchFile = unchecked((int)0xc000000f);

    [StructLayout(LayoutKind.Sequential)]
    private struct RelativeName
    {
        public ushort Length;
        public ushort MaximumLength;
        public IntPtr Buffer;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct RelativeObject
    {
        public uint Length;
        public IntPtr RootDirectory;
        public IntPtr ObjectName;
        public uint Attributes;
        public IntPtr SecurityDescriptor;
        public IntPtr SecurityQualityOfService;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct WriterIoStatus
    {
        public IntPtr Status;
        public UIntPtr Information;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct WriterStandardInfo
    {
        public long AllocationSize;
        public long EndOfFile;
        public uint NumberOfLinks;
        public byte DeletePending;
        public byte Directory;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct WriterRenameRequest
    {
        public uint Flags;
        public IntPtr RootDirectory;
        public uint FileNameLength;
        public ushort FirstCodeUnit;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct WriterOverlapped
    {
        public UIntPtr Internal;
        public UIntPtr InternalHigh;
        public uint Offset;
        public uint OffsetHigh;
        public IntPtr Event;
    }

    [DllImport("ntdll.dll", ExactSpelling = true)]
    private static extern int NtCreateFile(out IntPtr handle, uint access, ref RelativeObject attributes,
        out WriterIoStatus ioStatus, IntPtr allocationSize, uint fileAttributes, uint share,
        uint disposition, uint createOptions, IntPtr ea, uint eaLength);

    [DllImport("ntdll.dll", ExactSpelling = true)]
    private static extern int NtSetInformationFile(SafeFileHandle file, out WriterIoStatus ioStatus,
        IntPtr information, uint length, int informationClass);

    [DllImport("ntdll.dll", ExactSpelling = true)]
    private static extern uint RtlNtStatusToDosError(int status);

    [DllImport("kernel32.dll", EntryPoint = "GetFileInformationByHandleEx", ExactSpelling = true, SetLastError = true)]
    private static extern bool ReadWriterStandardInfo(SafeFileHandle handle, int informationClass,
        out WriterStandardInfo info, uint size);

    [DllImport("kernel32.dll", ExactSpelling = true, SetLastError = true)]
    private static extern bool LockFileEx(SafeFileHandle file, uint flags, uint reserved,
        uint lengthLow, uint lengthHigh, ref WriterOverlapped overlap);

    [DllImport("kernel32.dll", ExactSpelling = true, SetLastError = true)]
    private static extern bool UnlockFileEx(SafeFileHandle file, uint reserved,
        uint lengthLow, uint lengthHigh, ref WriterOverlapped overlap);

    private static void ValidateRelativeName(string name)
    {
        if (String.IsNullOrEmpty(name) || name == "." || name == ".." ||
            name.IndexOfAny(new[] { '\\', '/', ':', '\0' }) >= 0)
            throw new ArgumentException("Expected a literal relative probe name.");
    }

    private static SafeFileHandle OpenWriterRelative(SafeFileHandle parent, string name, uint access,
        uint share, uint disposition, bool directory, out int status)
    {
        ValidateRelativeName(name);
        ushort size = checked((ushort)(name.Length * sizeof(char)));
        var text = new RelativeName { Length = size, MaximumLength = size };
        IntPtr textBuffer = Marshal.AllocHGlobal(Marshal.SizeOf<RelativeName>());
        bool parentHeld = false;
        try
        {
            text.Buffer = Marshal.StringToHGlobalUni(name);
            Marshal.StructureToPtr(text, textBuffer, false);
            parent.DangerousAddRef(ref parentHeld);
            var attributes = new RelativeObject
            {
                Length = (uint)Marshal.SizeOf<RelativeObject>(),
                RootDirectory = parent.DangerousGetHandle(),
                ObjectName = textBuffer,
                Attributes = ObjCaseInsensitive | ObjDontReparse
            };
            status = NtCreateFile(out IntPtr handle, access, ref attributes, out _, IntPtr.Zero, 0,
                share, disposition, NtSynchronous | NtOpenReparsePoint | (directory ? NtDirectory : NtNonDirectory),
                IntPtr.Zero, 0);
            if (status < 0) return null;
            if (handle == IntPtr.Zero || handle == new IntPtr(-1))
                throw new IOException("Native relative open returned no valid handle.");
            return new SafeFileHandle(handle, ownsHandle: true);
        }
        finally
        {
            if (parentHeld) parent.DangerousRelease();
            Marshal.FreeHGlobal(text.Buffer);
            Marshal.FreeHGlobal(textBuffer);
        }
    }

    private static Win32Exception WriterNtError(string operation, int status) =>
        WindowsError($"{operation} (NTSTATUS 0x{status:x8})", (int)RtlNtStatusToDosError(status));

    private static FileIdentity InspectWriterFile(SafeFileHandle handle, string path)
    {
        FileIdentity identity = InspectName(handle, path, false);
        RequireWin32(ReadWriterStandardInfo(handle, StandardFileInfo, out WriterStandardInfo info,
            (uint)Marshal.SizeOf<WriterStandardInfo>()), $"Read link count for {path}");
        if (info.Directory != 0 || info.DeletePending != 0 || info.NumberOfLinks != 1)
            throw new IOException("The probe requires a single-link regular file with no pending deletion.");
        return identity;
    }

    public static WriterScope OpenWriterScope(string expectedSid, string fixtureDirectory, bool requireEmpty)
    {
        var token = AttestToken(expectedSid);
        if (!Path.IsPathFullyQualified(fixtureDirectory))
            throw new ArgumentException("The writer fixture path must be fully qualified.");
        string fullPath = Path.TrimEndingDirectorySeparator(Path.GetFullPath(fixtureDirectory));
        string drive = Path.GetPathRoot(fullPath);
        char letter = String.IsNullOrEmpty(drive) ? '\0' : Char.ToUpperInvariant(drive[0]);
        if (drive == null || drive.Length != 3 || drive[1] != ':' || drive[2] != '\\' ||
            letter < 'A' || letter > 'Z' || fullPath.Length <= drive.Length || GetDriveTypeW(drive) != DriveFixed)
            throw new ArgumentException("The writer fixture requires a fixed local drive.");

        var ancestry = new List<SafeFileHandle>();
        try
        {
            SafeFileHandle root = Open(drive, WriterDirectoryAccess, WriterShare, OpenExisting, BackupSemantics);
            ancestry.Add(root);
            FileIdentity rootId = InspectName(root, drive, true);
            FileIdentity fixtureId = rootId;
            string currentPath = drive;
            foreach (string component in fullPath.Substring(drive.Length).Split('\\'))
            {
                var parent = ancestry[ancestry.Count - 1];
                var next = OpenWriterRelative(parent, component, WriterDirectoryAccess, WriterShare,
                    NtOpen, directory: true, out int status);
                if (next == null) throw WriterNtError("Open a writer-scope directory", status);
                ancestry.Add(next);
                currentPath = Path.Combine(currentPath, component);
                fixtureId = InspectName(next, currentPath, true);
                if (fixtureId.VolumeSerial != rootId.VolumeSerial)
                    throw new IOException("The writer scope crosses a volume boundary.");
            }
            SafeFileHandle fixture = ancestry[ancestry.Count - 1];
            var fileSystem = new StringBuilder(32);
            RequireWin32(GetVolumeInformationByHandleW(fixture, IntPtr.Zero, 0, out uint serial,
                out _, out uint flags, fileSystem, (uint)fileSystem.Capacity), "Inspect the writer-scope volume");
            if (fileSystem.ToString() != "NTFS") throw new IOException("The writer fixture must be on NTFS.");
            if (requireEmpty)
            {
                using (var entries = Directory.EnumerateFileSystemEntries(fullPath).GetEnumerator())
                    if (entries.MoveNext()) throw new IOException("The writer fixture is not empty.");
            }

            var volume = new Dictionary<string, object>
            {
                ["file_system"] = fileSystem.ToString(), ["drive_type"] = "fixed", ["drive_type_code"] = DriveFixed,
                ["drive_root"] = drive, ["volume_serial"] = fixtureId.Volume,
                ["file_system_serial"] = serial.ToString("x8"), ["file_system_flags"] = flags,
                ["drive_root_file_id"] = rootId.Id, ["fixture_directory_file_id"] = fixtureId.Id,
                ["writable_directory_file_id"] = null
            };
            return new WriterScope(ancestry, fullPath, fixtureId.Id, fixtureId.Volume, token, volume);
        }
        catch
        {
            for (int index = ancestry.Count - 1; index >= 0; index--) ancestry[index].Dispose();
            throw;
        }
    }

    public sealed class WriterHandle : IDisposable
    {
        private readonly SafeFileHandle file;
        private bool held;
        public string FileId { get; }
        public string VolumeSerial { get; }

        internal WriterHandle(SafeFileHandle file, string id, string volume)
        {
            this.file = file;
            FileId = id;
            VolumeSerial = volume;
        }

        public Dictionary<string, object> TryLock()
        {
            if (held) throw new InvalidOperationException("This handle already holds the probe lock.");
            var overlap = new WriterOverlapped();
            // The offset and range match fs2::try_lock_exclusive on Windows.
            held = LockFileEx(file, ExclusiveNonblocking, 0, uint.MaxValue, uint.MaxValue, ref overlap);
            return Operation(held ? 0 : FailureCode("LockFileEx"));
        }

        public void Dispose()
        {
            try
            {
                if (held)
                {
                    held = false;
                    var overlap = new WriterOverlapped();
                    RequireWin32(UnlockFileEx(file, 0, uint.MaxValue, uint.MaxValue, ref overlap), "UnlockFileEx");
                }
            }
            finally { file.Dispose(); }
        }
    }

    private static Dictionary<string, object> RenameResult(int? status, string stage)
    {
        int? error = status.HasValue ? (int)RtlNtStatusToDosError(status.Value) : (int?)null;
        var result = Operation(error);
        result["ntstatus"] = status.HasValue ? status.Value.ToString("x8") : null;
        result["stage"] = stage;
        return result;
    }

    public sealed class WriterScope : IDisposable
    {
        private readonly List<SafeFileHandle> ancestry;
        private readonly string path;
        private readonly string folderId;
        private readonly string volumeSerial;
        public Dictionary<string, object> Token { get; }
        public Dictionary<string, object> Volume { get; }

        internal WriterScope(List<SafeFileHandle> ancestry, string path, string folderId, string volumeSerial,
            Dictionary<string, object> token, Dictionary<string, object> volume)
        {
            this.ancestry = ancestry;
            this.path = path;
            this.folderId = folderId;
            this.volumeSerial = volumeSerial;
            Token = token;
            Volume = volume;
        }

        private SafeFileHandle Folder => ancestry[ancestry.Count - 1];

        private void VerifyFolder()
        {
            FileIdentity identity = InspectName(Folder, path, true);
            if (identity.Id != folderId || identity.Volume != volumeSerial)
                throw new IOException("The retained writer scope changed identity.");
        }

        private FileIdentity VerifyFile(SafeFileHandle file, string name)
        {
            FileIdentity identity = InspectWriterFile(file, Path.Combine(path, name));
            if (identity.Volume != volumeSerial) throw new IOException("Probe file is outside the writer volume.");
            return identity;
        }

        public WriterHandle OpenLock(bool create)
        {
            return TryOpenLock(create, out int status) ??
                throw WriterNtError("Open writer.lock without delete sharing", status);
        }

        private WriterHandle TryOpenLock(bool create, out int status)
        {
            VerifyFolder();
            var handle = OpenWriterRelative(Folder, WriterName, GenericRead | GenericWrite | WriterSynchronize,
                WriterShare, create ? NtOpenIf : NtOpen, directory: false, out status);
            if (handle == null) return null;
            try
            {
                FileIdentity identity = VerifyFile(handle, WriterName);
                return new WriterHandle(handle, identity.Id, identity.Volume);
            }
            catch { handle.Dispose(); throw; }
        }

        private string NamedId(string name)
        {
            using (var file = OpenWriterRelative(Folder, name, ReadAttributes | WriterSynchronize,
                WriterShare | WriterShareDelete, NtOpen, directory: false, out int status))
            {
                if (file != null) return VerifyFile(file, name).Id;
                if (status == ObjectNameNotFound || status == NoSuchFile) return null;
                throw WriterNtError($"Read identity of {name}", status);
            }
        }

        private string CreateControlFile(string name)
        {
            using (var file = OpenWriterRelative(Folder, name, GenericWrite | ReadAttributes | WriterSynchronize,
                WriterShare, NtCreate, directory: false, out int status))
            {
                if (file == null) throw WriterNtError($"Create scratch file {name}", status);
                return VerifyFile(file, name).Id;
            }
        }

        private Dictionary<string, object> Rename(string source, string destination, uint flags)
        {
            ValidateRelativeName(destination);
            using (var file = OpenWriterRelative(Folder, source, WriterDelete | ReadAttributes | WriterSynchronize,
                WriterShare | WriterShareDelete, NtOpen, directory: false, out int status))
            {
                if (file == null) return RenameResult(status, "open_source");
                VerifyFile(file, source);
                byte[] name = Encoding.Unicode.GetBytes(destination);
                int nameOffset = Marshal.OffsetOf<WriterRenameRequest>(nameof(WriterRenameRequest.FirstCodeUnit)).ToInt32();
                int size = checked(Marshal.SizeOf<WriterRenameRequest>() + name.Length);
                IntPtr buffer = Marshal.AllocHGlobal(size);
                bool folderHeld = false;
                try
                {
                    Folder.DangerousAddRef(ref folderHeld);
                    Marshal.Copy(new byte[size], 0, buffer, size);
                    Marshal.StructureToPtr(new WriterRenameRequest
                    {
                        Flags = flags, RootDirectory = Folder.DangerousGetHandle(), FileNameLength = (uint)name.Length
                    }, buffer, false);
                    Marshal.Copy(name, 0, buffer + nameOffset, name.Length);
                    status = NtSetInformationFile(file, out _, buffer, (uint)size,
                        (flags & RenamePosix) == 0 ? RenameInfo : RenameInfoEx);
                    return RenameResult(status, "rename");
                }
                finally
                {
                    if (folderHeld) Folder.DangerousRelease();
                    Marshal.FreeHGlobal(buffer);
                }
            }
        }

        private Dictionary<string, object> RenameControl(string source, string destination, uint flags)
        {
            string sourceId = CreateControlFile(source);
            string priorId = (flags & RenameReplace) == 0 ? null : CreateControlFile(destination);
            var result = Rename(source, destination, flags);
            if ((bool)result["succeeded"])
            {
                if (NamedId(source) != null || NamedId(destination) != sourceId)
                    throw new IOException("The rename control did not move its source identity.");
            }
            else
            {
                int error = (int)result["win32_error"];
                if ((flags & RenamePosix) == 0 || (string)result["stage"] != "rename" ||
                    (error != 1 && error != 50 && error != 87) ||
                    NamedId(source) != sourceId || NamedId(destination) != priorId)
                    throw new IOException($"The unlocked rename control failed (Win32 error {error}, NTSTATUS {result["ntstatus"]}).");
            }
            return result;
        }

        private static bool ProvesDenial(Dictionary<string, object> result, bool lockedSource)
        {
            string stage = result["stage"] as string;
            // Only rename-away opens the held file itself. A replacement must reach the rename call.
            return (stage == "rename" || (lockedSource && stage == "open_source")) &&
                (bool)result["attempted"] && result["win32_error"] is int error &&
                (error == 5 || error == 32 || error == 33);
        }

        public Dictionary<string, object> ProbeNames(string expectedLockedId)
        {
            VerifyFolder();
            string initialId = NamedId(WriterName);
            if (initialId == null || initialId != expectedLockedId)
                throw new IOException("The writer name does not identify the live owner's file.");

            var renameAwayControl = RenameControl("control-away-from", "control-away-to", 0);
            var replacementControl = RenameControl("control-replace-from", "control-replace-to", RenameReplace);
            var posixControl = RenameControl("control-posix-from", "control-posix-to", RenameReplace | RenamePosix);
            bool posixSupported = (bool)posixControl["succeeded"];
            CreateControlFile("replace-writer-ordinary");
            CreateControlFile("replace-writer-posix");

            var renameAway = Rename(WriterName, "moved-writer", 0);
            string finalId = NamedId(WriterName);
            renameAway["name_file_id_after"] = finalId;
            var replacement = RenameResult(null, null);
            var posix = RenameResult(null, null);
            if (finalId == initialId)
            {
                replacement = Rename("replace-writer-ordinary", WriterName, RenameReplace);
                finalId = NamedId(WriterName);
                replacement["name_file_id_after"] = finalId;
            }
            if (finalId == initialId && posixSupported)
            {
                posix = Rename("replace-writer-posix", WriterName, RenameReplace | RenamePosix);
                finalId = NamedId(WriterName);
                posix["name_file_id_after"] = finalId;
            }
            VerifyFolder();

            bool preserved = finalId == initialId;
            var split = Operation(null);
            split["file_id"] = null;
            if (!preserved)
            {
                // This only touches the disposable fixture. The process driver keeps the first owner live.
                using (WriterHandle second = TryOpenLock(create: finalId == null, out int status))
                {
                    if (second == null)
                    {
                        split = RenameResult(status, "open_split_name");
                        split["file_id"] = finalId;
                    }
                    else
                    {
                        if (second.FileId == initialId)
                            throw new IOException("The observed name split disappeared during acquisition.");
                        split = second.TryLock();
                        split["file_id"] = second.FileId;
                    }
                }
            }

            return new Dictionary<string, object>
            {
                ["ordinary_rename_away_control"] = renameAwayControl,
                ["ordinary_replacement_control"] = replacementControl,
                ["posix_replace_control"] = posixControl,
                ["ordinary_rename_away"] = renameAway,
                ["ordinary_replacement"] = replacement,
                ["posix_replace"] = posix,
                ["posix_supported"] = posixSupported,
                ["initial_file_id"] = initialId, ["final_file_id"] = finalId, ["name_preserved"] = preserved,
                ["split_name_acquisition"] = split,
                ["qualified"] = preserved && posixSupported && ProvesDenial(renameAway, lockedSource: true) &&
                    ProvesDenial(replacement, lockedSource: false) && ProvesDenial(posix, lockedSource: false)
            };
        }

        public void Dispose()
        {
            for (int index = ancestry.Count - 1; index >= 0; index--) ancestry[index].Dispose();
        }
    }
}
