fn main() {}

#[cfg(test)]
mod tests {
    use std::fs;

    use insta::{assert_snapshot, glob};

    #[test]
    fn test() {

        let mut pg_client = postgres::connect();
        let sqlite_conn = sqlite::connect();

        // for each of the queries
        glob!("queries/**/*.prql", |path| {
            // read
            let prql = fs::read_to_string(path).unwrap();

            if prql.contains("skip_test") {
                return;
            }

            // compile
            let sql = prql_compiler::compile(&prql).unwrap();

            // save both csv files as same snapshot
            assert_snapshot!("", sqlite::query_csv(&sqlite_conn, &sql));

            if let Some(pg_client) = &mut pg_client {
                assert_snapshot!("", postgres::query_csv(pg_client, &sql));
            }
        });
    }

    mod sqlite {
        use rusqlite::{types::ValueRef, Connection};
        use std::{path::Path};

        pub fn connect() -> Connection {
            let path = Path::new("./chinook.db");
            Connection::open(path).unwrap()
        }

        pub fn query_csv(conn: &Connection, sql: &str) -> String {
            let mut statement = conn.prepare(sql).unwrap();

            let csv_header = statement.column_names().join("\t");
            let column_count = statement.column_count();

            let csv_rows = statement
                .query_map([], |row| {
                    Ok((0..column_count)
                        .map(|i| match row.get_ref_unwrap(i) {
                            ValueRef::Null => "".to_string(),
                            ValueRef::Integer(i) => i.to_string(),
                            ValueRef::Real(r) => r.to_string(),
                            ValueRef::Text(t) => String::from_utf8_lossy(t).to_string(),
                            ValueRef::Blob(_) => unimplemented!(),
                        })
                        .collect::<Vec<_>>()
                        .join("\t"))
                })
                .unwrap()
                .into_iter()
                .take(100) // truncate to 100 rows
                .map(|r| r.unwrap())
                .collect::<Vec<String>>()
                .join("\n");

            csv_header + "\n" + &csv_rows
        }
    }

    mod postgres {
        use std::time::SystemTime;

        use postgres::types::{FromSql, Type};
        use postgres::{Client, NoTls, Row};
        use chrono::{DateTime, Utc};

        pub fn connect() -> Option<Client> {
            let host = std::env::var("POSTGRES_HOST").ok()?;

            let client = Client::connect(&format!("host={} user=postgres", host), NoTls).unwrap();

            Some(client)
        }

        pub fn query_csv(client: &mut Client, sql: &str) -> String {
            let statement = client.prepare(sql).unwrap();

            let csv_header = statement
                .columns()
                .iter()
                .map(|c| c.name())
                .take(100) // truncate to 100 rows
                .collect::<Vec<_>>()
                .join("\t");

            let rows = client.query(&statement, &[]).unwrap();

            fn get<'a, T: ToString + FromSql<'a>>(row: &'a Row, idx: usize) -> String {
                row.get::<usize, Option<T>>(idx)
                    .map(|v| v.to_string())
                    .unwrap_or_default()
            }

            let mut csv_rows = vec![csv_header];
            for row in rows.into_iter().take(100) {
                csv_rows.push(
                    (0..row.len())
                        .map(|i| match row.columns()[i].type_() {
                            &Type::BOOL => get::<bool>(&row, i),
                            &Type::INT2 => get::<i16>(&row, i),
                            &Type::INT4 => get::<i32>(&row, i),
                            &Type::INT8 => get::<i64>(&row, i),
                            &Type::TEXT | &Type::VARCHAR => get::<String>(&row, i),
                            &Type::JSON | &Type::JSONB => get::<String>(&row, i),
                            &Type::FLOAT4 => get::<f32>(&row, i),
                            &Type::FLOAT8 => get::<f32>(&row, i),
                            &Type::TIMESTAMPTZ | &Type::TIMESTAMP => get::<Timestamp>(&row, i),
                            t => unimplemented!("postgres type {t}"),
                        })
                        .collect::<Vec<_>>()
                        .join("\t"),
                );
            }

            csv_rows.join("\n")
        }

        struct Timestamp(SystemTime);
        impl<'a> FromSql<'a> for Timestamp {
            fn from_sql(
                ty: &Type,
                raw: &'a [u8],
            ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
                SystemTime::from_sql(ty, raw).map(Timestamp)
            }

            fn accepts(ty: &Type) -> bool {
                SystemTime::accepts(ty)
            }
        }
        impl ToString for Timestamp {
            fn to_string(&self) -> String {
                let dt = DateTime::<Utc>::from(self.0);
                dt.format("%Y-%m-%d %H:%M:%S").to_string()
            }
        }
    }
}
