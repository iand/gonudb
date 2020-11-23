# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

These are changes that will probably be included in the next release.

### Added
 
### Fixed

### Changed
 
### Removed

## [v0.2.0] - 2020-11-23

### Added

 * Support variable length keys
 * Compute number of records when store is opened

### Changed

 * Simplify method signatures of Fetch and FetchReader (breaking change)

## [v0.1.1] - 2020-11-18

### Fixed

 * Reduce scope of insert locking to unblock reads when inserts are throttled 
 * Fix bucket scanner not detecting EOF

## [v0.1.0] - 2020-11-18

Initial release
