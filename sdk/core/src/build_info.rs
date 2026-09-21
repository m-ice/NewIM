//! Diagnostic identity, independent of wire and storage versions.

use std::fmt;

pub const SDK_VERSION: &str = env!("CARGO_PKG_VERSION");

/// 仅校验源码版本语法，不证明真实性。 / Validates revision syntax, not authenticity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Revision<'a>(&'a str);

impl<'a> Revision<'a> {
    /// 完整小写 Git 摘要；不接受缩写。 / Requires a full lowercase Git digest.
    pub fn parse(value: &'a str) -> Result<Self, BuildInfoError> {
        if !matches!(value.len(), 40 | 64)
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(BuildInfoError::InvalidRevision);
        }
        Ok(Self(value))
    }

    pub fn as_str(self) -> &'a str {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BuildInfoError {
    InvalidRevision,
}

impl fmt::Display for BuildInfoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NIM_BUILD_INVALID_METADATA")
    }
}

impl std::error::Error for BuildInfoError {}

/// 缺失信息保持未知，不推断干净工作树。 / Missing provenance never implies a clean tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BuildInfo<'a> {
    revision: Option<Revision<'a>>,
}

impl<'a> BuildInfo<'a> {
    /// 缺失与空字符串有区别。 / An absent revision differs from an empty one.
    pub fn from_revision(revision: Option<&'a str>) -> Result<Self, BuildInfoError> {
        Ok(Self {
            revision: revision.map(Revision::parse).transpose()?,
        })
    }

    pub fn sdk_version(self) -> &'static str {
        SDK_VERSION
    }

    pub fn revision(self) -> Option<&'a str> {
        self.revision.map(Revision::as_str)
    }
}

/// 读取编译时标签；无运行时环境或网络访问。 / Reads compile-time labels without runtime I/O.
pub fn current() -> Result<BuildInfo<'static>, BuildInfoError> {
    BuildInfo::from_revision(option_env!("NEWIM_BUILD_REVISION"))
}

impl fmt::Display for BuildInfo<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "sdkVersion={}", self.sdk_version())?;
        if let Some(revision) = self.revision() {
            write!(formatter, " sourceRevision={revision}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_and_empty_revisions_are_distinct() {
        let unknown = BuildInfo::from_revision(None).unwrap();
        assert_eq!(unknown.revision(), None);
        assert!(!unknown.to_string().contains("sourceRevision"));
        assert_eq!(
            BuildInfo::from_revision(Some("")),
            Err(BuildInfoError::InvalidRevision)
        );
    }

    #[test]
    fn revision_length_boundaries() {
        for length in 0..=80 {
            let input = "a".repeat(length);
            assert_eq!(
                Revision::parse(&input).is_ok(),
                matches!(length, 40 | 64),
                "length {length}"
            );
        }
    }

    #[test]
    fn invalid_characters_do_not_enter_identity() {
        for input in [
            "A".repeat(40),
            "g".repeat(40),
            "a".repeat(39) + " ",
            "a".repeat(39) + "\n",
            "a".repeat(38) + "é",
        ] {
            assert_eq!(
                BuildInfo::from_revision(Some(&input)),
                Err(BuildInfoError::InvalidRevision)
            );
        }
    }

    #[test]
    fn valid_revision_round_trips_without_normalization() {
        for input in [
            "0123456789abcdef0123456789abcdef01234567".to_owned(),
            "0123456789abcdef".repeat(4),
        ] {
            let info = BuildInfo::from_revision(Some(&input)).unwrap();
            assert_eq!(info.revision(), Some(input.as_str()));
            assert!(
                info.to_string()
                    .ends_with(&format!("sourceRevision={input}"))
            );
        }
    }

    #[test]
    fn errors_have_a_stable_code_without_raw_input() {
        let error = Revision::parse("private-input").unwrap_err();
        assert_eq!(error.to_string(), "NIM_BUILD_INVALID_METADATA");
    }
}
