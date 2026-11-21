//! A minimal JSON writer for the reports (no dependency needed for a few
//! fixed-shape documents).

use std::fmt::{self, Write};

/// A JSON value.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Json {
    Null,
    Bool(bool),
    Uint(u64),
    Int(i64),
    /// Non-finite values are written as `null`.
    Float(f64),
    Str(String),
    Array(Vec<Json>),
    Object(Vec<(&'static str, Json)>),
}

impl Json {
    pub(crate) fn object() -> Json {
        Json::Object(Vec::new())
    }

    /// Add a member to an object (no-op on other values).
    pub(crate) fn with(mut self, key: &'static str, value: impl Into<Json>) -> Json {
        if let Json::Object(members) = &mut self {
            members.push((key, value.into()));
        }
        self
    }
}

impl From<bool> for Json {
    fn from(v: bool) -> Json {
        Json::Bool(v)
    }
}

impl From<u64> for Json {
    fn from(v: u64) -> Json {
        Json::Uint(v)
    }
}

impl From<u32> for Json {
    fn from(v: u32) -> Json {
        Json::Uint(v.into())
    }
}

impl From<u16> for Json {
    fn from(v: u16) -> Json {
        Json::Uint(v.into())
    }
}

impl From<u8> for Json {
    fn from(v: u8) -> Json {
        Json::Uint(v.into())
    }
}

impl From<usize> for Json {
    fn from(v: usize) -> Json {
        Json::Uint(v as u64)
    }
}

impl From<i64> for Json {
    fn from(v: i64) -> Json {
        Json::Int(v)
    }
}

impl From<f64> for Json {
    fn from(v: f64) -> Json {
        Json::Float(v)
    }
}

impl From<&str> for Json {
    fn from(v: &str) -> Json {
        Json::Str(v.to_string())
    }
}

impl From<String> for Json {
    fn from(v: String) -> Json {
        Json::Str(v)
    }
}

impl<T: Into<Json>> From<Option<T>> for Json {
    fn from(v: Option<T>) -> Json {
        v.map_or(Json::Null, Into::into)
    }
}

impl<T: Into<Json>> From<Vec<T>> for Json {
    fn from(v: Vec<T>) -> Json {
        Json::Array(v.into_iter().map(Into::into).collect())
    }
}

fn write_str(f: &mut fmt::Formatter<'_>, s: &str) -> fmt::Result {
    f.write_char('"')?;
    for c in s.chars() {
        match c {
            '"' => f.write_str("\\\"")?,
            '\\' => f.write_str("\\\\")?,
            '\n' => f.write_str("\\n")?,
            '\r' => f.write_str("\\r")?,
            '\t' => f.write_str("\\t")?,
            c if u32::from(c) < 0x20 => write!(f, "\\u{:04x}", u32::from(c))?,
            c => f.write_char(c)?,
        }
    }
    f.write_char('"')
}

impl fmt::Display for Json {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Json::Null => f.write_str("null"),
            Json::Bool(b) => write!(f, "{b}"),
            Json::Uint(n) => write!(f, "{n}"),
            Json::Int(n) => write!(f, "{n}"),
            Json::Float(x) if x.is_finite() => {
                // Three decimals is plenty for the milliseconds and rates
                // reported, and keeps the output stable.
                let s = format!("{x:.3}");
                let s = s.trim_end_matches('0').trim_end_matches('.');
                f.write_str(if s == "-0" { "0" } else { s })
            }
            Json::Float(_) => f.write_str("null"),
            Json::Str(s) => write_str(f, s),
            Json::Array(items) => {
                f.write_char('[')?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        f.write_char(',')?;
                    }
                    item.fmt(f)?;
                }
                f.write_char(']')
            }
            Json::Object(members) => {
                f.write_char('{')?;
                for (i, (key, value)) in members.iter().enumerate() {
                    if i > 0 {
                        f.write_char(',')?;
                    }
                    write_str(f, key)?;
                    f.write_char(':')?;
                    value.fmt(f)?;
                }
                f.write_char('}')
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_valid_compact_json() {
        let v = Json::object()
            .with("s", "a\"b\\c\nd\u{1}é")
            .with("n", 42u64)
            .with("i", -3i64)
            .with("f", 2.5f64)
            .with("whole", 40.0f64)
            .with("nan", f64::NAN)
            .with("none", None::<u64>)
            .with("list", vec![1u64, 2])
            .with("empty", Json::Array(vec![]))
            .with("t", true);
        assert_eq!(
            v.to_string(),
            r#"{"s":"a\"b\\c\nd\u0001é","n":42,"i":-3,"f":2.5,"whole":40,"nan":null,"none":null,"list":[1,2],"empty":[],"t":true}"#
        );
        assert_eq!(Json::Float(-0.0001).to_string(), "0");
    }
}
