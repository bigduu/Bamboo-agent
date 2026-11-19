use std::sync::LazyLock;

use regex::Regex;

static ENGLISH_INTERVAL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\bevery\s+(?:([a-z]+(?:[- ][a-z]+)?|[0-9]+)\s+)?(seconds?|minutes?|hours?|days?)\b",
    )
    .expect("fixed English interval pattern")
});
static CHINESE_INTERVAL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"每隔?\s*([0-9]+|[一二两三四五六七八九十]+)?\s*个?\s*(秒钟?|分钟|小时|天)")
        .expect("fixed Chinese interval pattern")
});
static POLITE_PREFIX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)^(?:(?:please|(?:can|could|would|will) you(?: please)?)\s+|请\s*|麻烦\s*|帮我\s*)",
    )
    .expect("fixed request prefix")
});
static TASK_REQUEST: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?ix)^(?:
          (?: audit | check | collect | compare | fetch | inspect | look | monitor
            | poll | read | refresh | remind | report | review | run | scan | send
            | summarize | summarise | test | track | try | update | verify | watch | write )\s+
          | 检查 | 看看 | 查看 | 关注 | 跟踪 | 查询 | 监控 | 轮询 | 刷新 | 验证 | 运行
          | 执行 | 测试 | 汇总 | 总结 | 对比 | 同步 | 收集 | 提醒 | 通知 | 汇报 | 更新 | 记录
        )",
    )
    .expect("fixed task request pattern")
});
static QUESTION_BEFORE_CADENCE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?:whether|why|how|when|if)\b|是否|为什么|原因|怎么|如何|有没有")
        .expect("fixed cadence question pattern")
});
static ALTERNATIVE_AFTER_CADENCE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^(?:or\b|或|还是)").expect("fixed cadence alternative pattern")
});
static STOP: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^(?:(?:please |(?:can|could|would|will) you (?:please )?)?(?:stop|cancel|disable|end) (?:this |the |my )?(?:recurring )?(?:loop|scheduled task)(?: please)?|(?:麻烦|请|帮我)?(?:停止|取消|结束|关闭|停掉)(?:一下|这个|当前的?|刚才的|我的)?(?:循环|定时任务)(?:吧)?)$")
        .expect("fixed stop pattern")
});
static RESUME: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^(?:(?:please |(?:can|could|would|will) you (?:please )?)?(?:resume|restart|re-enable) (?:this |the |my )?(?:recurring )?(?:loop|scheduled task)(?: please)?|(?:麻烦|请|帮我)?(?:恢复|继续|重新开启)(?:一下|这个|当前的?|刚才的|我的)?(?:循环|定时任务)(?:吧)?)$")
        .expect("fixed resume pattern")
});

pub(super) fn task(text: &str) -> &str {
    text.split_once(char::is_whitespace)
        .filter(|(prefix, _)| prefix.eq_ignore_ascii_case("/loop"))
        .map_or(text, |(_, body)| body.trim_start())
}

pub(super) fn stop(text: &str) -> bool {
    STOP.is_match(control_text(text))
}

pub(super) fn resume(text: &str) -> bool {
    RESUME.is_match(control_text(text))
}

pub(super) fn has_mentioned_interval(text: &str) -> bool {
    ENGLISH_INTERVAL.is_match(text) || CHINESE_INTERVAL.is_match(text)
}

fn control_text(text: &str) -> &str {
    task(text)
        .trim_end_matches(['.', '。', '!', '！', '?', '？'])
        .trim_end()
}

pub(super) fn interval(text: &str, quote: &str) -> Option<u64> {
    let work = task(text);
    if work.contains(['"', '“', '”', '‘', '\u{60}', '\n', '\r']) {
        return None;
    }
    let mut cadences = ENGLISH_INTERVAL
        .captures_iter(work)
        .chain(CHINESE_INTERVAL.captures_iter(work));
    let cadence = cadences.next()?;
    let phrase = cadence.get(0)?;
    let before = work[..phrase.start()].trim_start();
    let before = strip_polite_prefix(before).trim_end_matches([' ', ',', '，', '、']);
    let after = work[phrase.end()..].trim_start_matches([' ', ',', '，', '、']);
    let affirmative = if before.is_empty() {
        TASK_REQUEST.is_match(strip_polite_prefix(after))
    } else {
        TASK_REQUEST.is_match(before) && !QUESTION_BEFORE_CADENCE.is_match(before)
    };
    // Spelling the slash command explicitly also selects the scheduling
    // operation for tasks outside the conversational request patterns.
    let explicit = work != text
        && [before, after]
            .iter()
            .any(|part| part.chars().any(char::is_alphabetic));
    if !(affirmative || explicit)
        || cadences.next().is_some()
        || ALTERNATIVE_AFTER_CADENCE.is_match(after)
        || !quote.contains(phrase.as_str())
    {
        return None;
    }
    let count = cadence
        .get(1)
        .map_or(Some(1), |value| count(value.as_str()))?;
    let unit = match cadence[2].to_ascii_lowercase().as_str() {
        "second" | "seconds" | "秒" | "秒钟" => 1,
        "minute" | "minutes" | "分钟" => 60,
        "hour" | "hours" | "小时" => 3600,
        "day" | "days" | "天" => 86400,
        _ => return None,
    };
    count.checked_mul(unit)
}

fn strip_polite_prefix(text: &str) -> &str {
    POLITE_PREFIX
        .find(text)
        .map_or(text, |prefix| &text[prefix.end()..])
}

fn count(text: &str) -> Option<u64> {
    if let Ok(value) = text.parse() {
        return Some(value);
    }
    let lower = text.to_ascii_lowercase();
    let small = [
        "one",
        "two",
        "three",
        "four",
        "five",
        "six",
        "seven",
        "eight",
        "nine",
        "ten",
        "eleven",
        "twelve",
        "thirteen",
        "fourteen",
        "fifteen",
        "sixteen",
        "seventeen",
        "eighteen",
        "nineteen",
    ];
    if let Some(index) = small.iter().position(|word| *word == lower) {
        return Some(index as u64 + 1);
    }
    let (tens, ones) = lower
        .split_once([' ', '-'])
        .map_or((lower.as_str(), None), |(tens, ones)| (tens, Some(ones)));
    let decades = [
        "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety",
    ];
    if let Some(index) = decades.iter().position(|word| *word == tens) {
        let ones = ones.map_or(Some(0), |value| {
            small[..9]
                .iter()
                .position(|word| *word == value)
                .map(|i| i as u64 + 1)
        })?;
        return Some((index as u64 + 2) * 10 + ones);
    }
    let digit = |text: &str| {
        ["一", "二", "三", "四", "五", "六", "七", "八", "九"]
            .iter()
            .position(|word| *word == text)
            .map(|i| i as u64 + 1)
            .or((text == "两").then_some(2))
    };
    if let Some((tens, ones)) = text.split_once('十') {
        let tens = if tens.is_empty() { 1 } else { digit(tens)? };
        let ones = if ones.is_empty() { 0 } else { digit(ones)? };
        return Some(tens * 10 + ones);
    }
    digit(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paced_loop_natural_interval_preserves_exact_cadence() {
        for (text, quote, seconds) in [
            ("/loop 每隔五分钟检查构建", "每隔五分钟", 300),
            ("每二十五分钟检查构建", "每二十五分钟", 1500),
            (
                "Check CI every twenty-five minutes",
                "every twenty-five minutes",
                1500,
            ),
            ("Can you check CI every hour?", "every hour", 3600),
            ("Check CI every 2 hours", "every 2 hours", 7200),
            ("Please check CI every hour", "every hour", 3600),
            ("Every hour, summarize CI logs", "Every hour", 3600),
            ("请每隔五分钟检查构建", "每隔五分钟", 300),
            ("检查构建每隔五分钟", "每隔五分钟", 300),
        ] {
            assert_eq!(interval(text, quote), Some(seconds), "{text}");
        }
        for text in [
            "Summarize this file",
            "What does check the build every five minutes mean?",
            "Do not check CI every hour",
            "Please don't check CI every five minutes.",
            "CI checks currently run every five minutes.",
            "Can you explain why CI runs every five minutes?",
            "Check whether CI already runs every five minutes",
            "Check CI every five minutes or ten minutes.",
            "请不要每隔五分钟检查构建",
            "看看构建是否每隔五分钟执行一次",
            "Explain how to set a loop every hour",
            "Please quote “check CI every hour” back to me",
            "每隔五分钟",
            "每隔五分钟检查构建，或者每隔十分钟检查构建",
        ] {
            assert_eq!(interval(text, text), None, "{text}");
        }
        assert_eq!(interval("每隔五分钟检查构建", "检查构建"), None);
        assert!(stop("停止这个循环。"));
        assert!(resume("Can you resume this scheduled task?"));
        assert!(!stop("请汇报最新构建状态。"));
    }
}
