use std::{
    io,
    mem::{offset_of, size_of, MaybeUninit},
    os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
    ptr, slice,
};

use serde::{Deserialize, Serialize};
use windows_sys::Win32::{
    Foundation::{LocalFree, LUID},
    Security::{
        Authorization::ConvertSidToStringSidW, GetTokenInformation, IsWellKnownSid,
        LookupPrivilegeValueW, TokenElevation, TokenGroups, TokenIntegrityLevel, TokenPrivileges,
        TokenUser, WinBuiltinAdministratorsSid, WinLocalAccountSid, LUID_AND_ATTRIBUTES, PSID,
        SID_AND_ATTRIBUTES, TOKEN_ELEVATION, TOKEN_GROUPS, TOKEN_INFORMATION_CLASS,
        TOKEN_MANDATORY_LABEL, TOKEN_PRIVILEGES, TOKEN_QUERY, TOKEN_USER,
    },
    System::Threading::{GetCurrentProcess, OpenProcessToken},
};

#[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(super) struct Token {
    sid: String,
    elevated: bool,
    integrity_sid: String,
    local_account_group_present: bool,
    administrators_group_present: bool,
    excess_privileges_present: Vec<String>,
}

pub(super) fn attest(expected_sid: &str) -> Token {
    let mut raw = ptr::null_mut();
    // SAFETY: The current-process pseudo handle is valid, and raw is writable.
    check(
        unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut raw) },
        "open token",
    );
    // SAFETY: OpenProcessToken returned a newly owned real token handle.
    let process_token = unsafe { OwnedHandle::from_raw_handle(raw) };

    let user = token_info(&process_token, TokenUser);
    let integrity = token_info(&process_token, TokenIntegrityLevel);
    let elevated = token_elevated(&process_token);
    let groups = token_info(&process_token, TokenGroups);
    let privileges = token_info(&process_token, TokenPrivileges);

    // SAFETY: Each buffer comes from the corresponding token information class
    // and stays alive while inspecting the SIDs and variable-length arrays.
    let token = unsafe {
        let user: TOKEN_USER = user.read(0);
        let integrity: TOKEN_MANDATORY_LABEL = integrity.read(0);
        let group_count: u32 = groups.read(0);
        let group_list: &[SID_AND_ATTRIBUTES] =
            groups.array(offset_of!(TOKEN_GROUPS, Groups), group_count);
        let privilege_count: u32 = privileges.read(0);
        let privilege_list: &[LUID_AND_ATTRIBUTES] =
            privileges.array(offset_of!(TOKEN_PRIVILEGES, Privileges), privilege_count);
        Token {
            sid: sid_string(user.User.Sid),
            elevated,
            integrity_sid: sid_string(integrity.Label.Sid),
            local_account_group_present: group_list
                .iter()
                .any(|group| IsWellKnownSid(group.Sid, WinLocalAccountSid) != 0),
            // Inspect all group SIDs, including deny-only memberships.
            administrators_group_present: group_list
                .iter()
                .any(|group| IsWellKnownSid(group.Sid, WinBuiltinAdministratorsSid) != 0),
            excess_privileges_present: excess_privileges(privilege_list),
        }
    };

    assert!(
        token.sid == expected_sid
            && token.integrity_sid == "S-1-16-8192"
            && token.local_account_group_present
            && !token.elevated
            && !token.administrators_group_present
            && token.excess_privileges_present.is_empty(),
        "expected the supplied medium-integrity standard local user: {token:?}"
    );
    token
}

fn check(result: i32, operation: &str) {
    if result == 0 {
        panic!("{operation}: {}", io::Error::last_os_error());
    }
}

fn token_elevated(token: &OwnedHandle) -> bool {
    let mut elevation = TOKEN_ELEVATION { TokenIsElevated: 0 };
    let size = size_of::<TOKEN_ELEVATION>() as u32;
    let mut written = 0;
    // SAFETY: TokenElevation writes one TOKEN_ELEVATION to this sized buffer.
    let result = unsafe {
        GetTokenInformation(
            token.as_raw_handle(),
            TokenElevation,
            ptr::from_mut(&mut elevation).cast(),
            size,
            &mut written,
        )
    };
    check(result, "read token elevation");
    assert_eq!(written, size, "unexpected token elevation size");
    elevation.TokenIsElevated != 0
}

struct TokenInfo {
    storage: Vec<usize>,
    bytes: usize,
}

fn token_info(token: &OwnedHandle, class: TOKEN_INFORMATION_CLASS) -> TokenInfo {
    let raw = token.as_raw_handle();
    let mut bytes = 0;
    // SAFETY: A null zero-length buffer queries the required length.
    let result = unsafe { GetTokenInformation(raw, class, ptr::null_mut(), 0, &mut bytes) };
    let error = io::Error::last_os_error();
    assert!(
        result == 0
            && matches!(error.raw_os_error(), Some(24 | 122))
            && (1..=131_072).contains(&bytes),
        "measure token information class {class}: {error}, bytes={bytes}"
    );
    let mut storage = vec![0usize; (bytes as usize).div_ceil(size_of::<usize>())];
    let mut written = 0;
    // SAFETY: Storage is aligned for the Win32 token records and large enough.
    let result = unsafe {
        GetTokenInformation(raw, class, storage.as_mut_ptr().cast(), bytes, &mut written)
    };
    check(result, &format!("read token information class {class}"));
    assert!((1..=bytes).contains(&written));
    TokenInfo {
        storage,
        bytes: written as usize,
    }
}

impl TokenInfo {
    unsafe fn read<T: Copy>(&self, offset: usize) -> T {
        assert!(offset + size_of::<T>() <= self.bytes);
        // SAFETY: Caller supplies the matching initialized Win32 field layout.
        unsafe {
            self.storage
                .as_ptr()
                .cast::<u8>()
                .add(offset)
                .cast::<T>()
                .read_unaligned()
        }
    }

    unsafe fn array<T>(&self, offset: usize, count: u32) -> &[T] {
        let count = count as usize;
        assert!(offset <= self.bytes && count <= (self.bytes - offset) / size_of::<T>());
        // SAFETY: Caller supplies the matching initialized Win32 array layout.
        let start = unsafe { self.storage.as_ptr().cast::<u8>().add(offset).cast::<T>() };
        assert!(start.is_aligned());
        // SAFETY: The kernel returned count elements within this live buffer.
        unsafe { slice::from_raw_parts(start, count) }
    }
}

unsafe fn sid_string(sid: PSID) -> String {
    let mut wide = ptr::null_mut();
    // SAFETY: Caller supplies a valid SID from a live token buffer.
    check(
        unsafe { ConvertSidToStringSidW(sid, &mut wide) },
        "format token SID",
    );
    assert!(!wide.is_null());
    let mut len = 0;
    // SAFETY: ConvertSidToStringSidW allocated a null-terminated string.
    while len < 256 && unsafe { wide.add(len).read() } != 0 {
        len += 1;
    }
    // SAFETY: The counted string is valid until LocalFree.
    let text = String::from_utf16(unsafe { slice::from_raw_parts(wide, len) });
    // SAFETY: This is the object allocated by ConvertSidToStringSidW.
    assert!(unsafe { LocalFree(wide.cast()) }.is_null());
    assert!(len < 256, "token SID text exceeded its bound");
    text.expect("SID is UTF-16")
}

fn excess_privileges(privileges: &[LUID_AND_ATTRIBUTES]) -> Vec<String> {
    [
        "SeBackupPrivilege",
        "SeRestorePrivilege",
        "SeManageVolumePrivilege",
    ]
    .into_iter()
    .filter(|name| {
        let wide: Vec<_> = name.encode_utf16().chain([0]).collect();
        let mut id = MaybeUninit::<LUID>::uninit();
        // SAFETY: The name is terminated and id is writable.
        check(
            unsafe { LookupPrivilegeValueW(ptr::null(), wide.as_ptr(), id.as_mut_ptr()) },
            &format!("resolve {name}"),
        );
        // SAFETY: Successful lookup initialized id.
        let id = unsafe { id.assume_init() };
        privileges
            .iter()
            .any(|p| p.Luid.LowPart == id.LowPart && p.Luid.HighPart == id.HighPart)
    })
    .map(String::from)
    .collect()
}
