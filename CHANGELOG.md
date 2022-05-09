# Changelog

## 0.5.0 (5/8/2022)

* Support for pub/sub timeouts
* Redis store
	* Removed separate thread for pub/sub iterators

## 0.4.0 (4/21/2022)

* Redis store
	* Made pub/sub more resilient (PR #38)
	* Remove option to set channel size, it is now always 1 (PR #38)
* Improvements to unit tests (PR #38)

## 0.3.0 (4/7/2022)

* Rust library
	* Support for getting the latest value (PR #30)
	* Accept `Into<Atom>` rather than `Atom`, providing a more ergonomic interface (PR #36)
	* Fixed range check error found by the fuzzer (PR #35)
	* Hide test macros from the docs (PR #32)
	* Reorganize code (PR #31, #27)
* Python library
	* Added support for sqlite ranges (PR #37)
	* Release the GIL where possible for better performance (PR #37)
* Sqlite store
	* Removed the global compressor/decompressor for better multithreaded performance (PR #34)

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
