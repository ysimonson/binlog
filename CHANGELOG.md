# Changelog

## 0.2.0 (4/6/2022)

* Redis streaming store (PR #22)
* Python interface for the redis streaming store (PR #23)
* Use `i64`s for timestamps instead of `Duration` (PR #19)
* Split up the store trait (PR #18)
* Merged the python and rust libraries into one (PR #7)
* Added tests, CI (PR #9)
* Added examples (PR #12)
* Added benchmarks (PR #16)
* Added `Cargo.lock` since the package can now be built as a cdylib
* Python: Support for pushing to sqlite stores
* Sqlite: Enabled WAL2 mode (PR #17)

## 0.1.0 (3/31/2022)

Initial release
