use std::borrow::Cow;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FileReference {
    pub(crate) path: String,
    pub(crate) name: String,
}

/// Interpret a paste as file references only when it consists of paths.
pub(crate) fn pasted_files(text: &str) -> Option<Vec<FileReference>> {
    if text.trim().is_empty() {
        return None;
    }
    text.trim()
        .lines()
        .map(|line| {
            let path = line.trim();
            Some(FileReference {
                path: path.to_string(),
                name: file_name(path)?.into_owned(),
            })
        })
        .collect()
}

/// Recognize standalone paths without assuming the server shares this filesystem.
pub(crate) fn file_name(text: &str) -> Option<Cow<'_, str>> {
    let text = text.trim();
    if text.chars().any(char::is_control) {
        return None;
    }
    let (path, quote) = ['\'', '"']
        .into_iter()
        .find_map(|quote| {
            let unquoted = text.strip_prefix(quote)?.strip_suffix(quote)?;
            Some((unquoted, Some(quote)))
        })
        .unwrap_or((text, None));
    if quote.is_none()
        && path
            .split_whitespace()
            .skip(1)
            .any(|word| word.starts_with('/'))
    {
        return None;
    }
    if let Some(scheme) = path.split_once("://").map(|(scheme, _)| scheme) {
        if !scheme.eq_ignore_ascii_case("file") {
            return None;
        }
        let url = reqwest::Url::parse(path).ok()?;
        if url.fragment().is_some() || url.query().is_some() || url.path().ends_with('/') {
            return None;
        }
        let file_path = url.to_file_path().ok()?;
        let name = file_path.file_name()?.to_str()?;
        return (!name.chars().any(char::is_control)).then(|| Cow::Owned(name.to_string()));
    }

    let windows = matches!(
        path.as_bytes(), [drive, b':', b'/' | b'\\', ..] if drive.is_ascii_alphabetic()
    ) || path.starts_with("\\\\")
        || path.starts_with(".\\")
        || path.starts_with("..\\");
    let (parent, name) = if windows {
        path.rsplit_once(['/', '\\'])?
    } else {
        path.rsplit_once('/')?
    };
    if name.is_empty() || matches!(name, "." | "..") {
        return None;
    }
    if parent.is_empty() && !name.contains('.') {
        // A bare slash command has the same shape as an extensionless root entry.
        return None;
    }
    let rooted = windows
        || path.starts_with('/')
        || path.starts_with("./")
        || path.starts_with("../")
        || path.starts_with("~/");
    if !rooted
        && (parent.starts_with('-')
            || (quote.is_none() && parent.split('/').next()?.contains(char::is_whitespace))
            || !name.trim_start_matches('.').contains('.'))
    {
        return None;
    }
    if windows || quote == Some('\'') || !name.contains('\\') {
        return Some(Cow::Borrowed(name));
    }
    let mut display = String::with_capacity(name.len());
    let mut chars = name.chars().peekable();
    while let Some(character) = chars.next() {
        // macOS terminals can paste a dragged filename as a shell-escaped word.
        if character == '\\'
            && chars.peek().is_some_and(|next| {
                matches!(
                    next,
                    ' ' | '\\' | '"' | '\'' | '(' | ')' | '[' | ']' | '$' | '&'
                )
            })
        {
            display.extend(chars.next());
        } else {
            display.push(character);
        }
    }
    Some(Cow::Owned(display))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pasted_files_keep_paths_and_display_basenames() {
        let cases = [
            ("/Users/me/screenshots/工作图.png", "工作图.png"),
            ("fixtures/reports/test run.txt", "test run.txt"),
            ("../docs/README", "README"),
            (r#"C:\Users\me\report (copy).pdf"#, "report (copy).pdf"),
            (r#"'./notes/my draft.txt'"#, "my draft.txt"),
            (r#"/tmp/my\ notes\ \(copy\).txt"#, "my notes (copy).txt"),
        ];
        let paste = cases.map(|(path, _)| path).join("\r\n");
        let files = pasted_files(&paste).unwrap();
        for (file, (path, name)) in files.iter().zip(cases) {
            assert_eq!(file.path, path);
            assert_eq!(file.name, name);
        }
    }

    #[test]
    fn plain_text_and_web_links_stay_in_the_text_editor() {
        for paste in [
            "",
            "Read ./my-file.txt",
            "Open the link https://example.com/image.png",
            "https://example.com/image.png",
            "/tmp/work/data.txt\nPlease check this",
            "/tmp/first.txt /tmp/second.txt",
            "a/b",
            "/",
            "/loop",
            "/tmp/work/",
        ] {
            assert!(pasted_files(paste).is_none(), "{paste:?}");
        }
    }

    #[test]
    fn file_urls_show_the_decoded_name_and_keep_the_original_reference() {
        let url = "file:///C:/exports/Bamboo%20draft.png";
        let files = pasted_files(url).unwrap();
        assert_eq!(files[0].name, "Bamboo draft.png");
        assert_eq!(files[0].path, url);
    }
}
