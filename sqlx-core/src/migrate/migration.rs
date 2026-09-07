use std::borrow::Cow;
use std::cmp::Ordering;

use sha2::{Digest, Sha384};

use super::MigrationType;

#[derive(Debug, Clone)]
pub struct Migration {
    pub version: i64,
    pub description: Cow<'static, str>,
    pub migration_type: MigrationType,
    pub sql: Cow<'static, str>,
    pub checksum: Cow<'static, [u8]>,
    pub no_tx: bool,
}

impl PartialEq for Migration {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version && self.migration_type == other.migration_type
    }
}

impl Eq for Migration {}

impl PartialOrd for Migration {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Migration {
    fn cmp(&self, other: &Self) -> Ordering {
        self.version
            .cmp(&other.version)
            .then_with(|| self.migration_type.cmp(&other.migration_type))
    }
}

impl Migration {
    pub fn new(
        version: i64,
        description: Cow<'static, str>,
        migration_type: MigrationType,
        sql: Cow<'static, str>,
        no_tx: bool,
    ) -> Self {
        let checksum = Cow::Owned(Vec::from(Sha384::digest(sql.as_bytes()).as_slice()));

        Migration {
            version,
            description,
            migration_type,
            sql,
            checksum,
            no_tx,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn migration(version: i64, migration_type: MigrationType, sql: &'static str) -> Migration {
        Migration::new(
            version,
            Cow::Borrowed("test migration"),
            migration_type,
            Cow::Borrowed(sql),
            false,
        )
    }

    #[test]
    fn ordering_is_consistent_with_equality() {
        let first = migration(7, MigrationType::ReversibleUp, "SELECT 1");
        let same_ordering_key = migration(7, MigrationType::ReversibleUp, "SELECT 2");

        assert_eq!(first, same_ordering_key);
        assert_eq!(first.cmp(&same_ordering_key), Ordering::Equal);
    }

    #[test]
    fn ordering_uses_version_then_canonical_migration_type() {
        let mut migrations = vec![
            migration(2, MigrationType::Simple, "SELECT 4"),
            migration(1, MigrationType::ReversibleDown, "SELECT 3"),
            migration(1, MigrationType::ReversibleUp, "SELECT 2"),
            migration(1, MigrationType::Simple, "SELECT 1"),
        ];

        migrations.sort();

        let ordering_keys: Vec<_> = migrations
            .iter()
            .map(|migration| (migration.version, migration.migration_type))
            .collect();
        assert_eq!(
            ordering_keys,
            vec![
                (1, MigrationType::Simple),
                (1, MigrationType::ReversibleUp),
                (1, MigrationType::ReversibleDown),
                (2, MigrationType::Simple),
            ]
        );
    }
}

#[derive(Debug, Clone)]
pub struct AppliedMigration {
    pub version: i64,
    pub checksum: Cow<'static, [u8]>,
}
