# PRQL test-databases

> Test PRQL queries against different SQL RDMS

## Data

Database chinook.db was downloaded from https://www.sqlitetutorial.net/sqlite-sample-database/

Columns are renamed to snake_case, so Postgres does not struggle with them.

Could be compressed:

rw-r--r-- 1 aljaz aljaz 864K nov 29 2015 chinook.db
-rw-r--r-- 1 aljaz aljaz 326K nov 29 2015 chinook.db.gz
-rw-r--r-- 1 aljaz aljaz 1,1M jun 23 15:56 chinook.sql
-rw-r--r-- 1 aljaz aljaz 296K jun 23 16:15 chinook.zip

## Cargo test

When run with `cargo test`, this will run queries only against SQLite and
DuckDB and assert snapshots of the result, serialized as CSV.

## Docker compose

There is also a proof on concept for testing done against Postgres, which can
be run by running `docker-compose up`. This will:

- build a docker image for Postgres (with data already loaded in)
- build a docker image for this crate (+sqlite database file), compiled with --tests
- run the two images, executing the tests.
