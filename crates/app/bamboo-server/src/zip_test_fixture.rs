// Keep ZIP structure and payload CRCs intact; encode the first letter as CP437 é.
fn zip_with_cp437_name(mut bytes: Vec<u8>, name: &str) -> Vec<u8> {
    let mut changed = 0;
    for (signature, flags, start, length) in
        [(*b"PK\x03\x04", 6, 30, 26), (*b"PK\x01\x02", 8, 46, 28)]
    {
        let positions: Vec<_> = bytes
            .windows(4)
            .enumerate()
            .filter_map(|(offset, value)| (value == signature).then_some(offset))
            .collect();
        for offset in positions {
            let size = u16::from_le_bytes(
                bytes[offset + length..offset + length + 2]
                    .try_into()
                    .unwrap(),
            ) as usize;
            if &bytes[offset + start..offset + start + size] != name.as_bytes() {
                continue;
            }
            let value = u16::from_le_bytes(
                bytes[offset + flags..offset + flags + 2]
                    .try_into()
                    .unwrap(),
            ) & !(1 << 11);
            bytes[offset + flags..offset + flags + 2].copy_from_slice(&value.to_le_bytes());
            bytes[offset + start] = 0x82;
            changed += 1;
        }
    }
    assert_eq!(changed, 2, "one local and one central name must be changed");
    bytes
}
