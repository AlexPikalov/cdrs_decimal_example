#[macro_use]
extern crate cdrs;
#[macro_use]
extern crate cdrs_helpers_derive;

use cdrs::authenticators::NoneAuthenticator;
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::load_balancing::RoundRobin;
use cdrs::query::*;

use cdrs::frame::IntoBytes;
use cdrs::types::from_cdrs::FromCDRSByName;
use cdrs::types::prelude::*;

type CurrentSession = Session<RoundRobin<TcpConnectionPool<NoneAuthenticator>>>;

fn main() {
    let node = NodeTcpConfigBuilder::new("127.0.0.1:9042", NoneAuthenticator {}).build();
    let cluster_config = ClusterTcpConfig(vec![node]);
    let no_compression: CurrentSession =
        new_session(&cluster_config, RoundRobin::new()).expect("session should be created");

    create_keyspace(&no_compression);
    create_table(&no_compression);
    insert_struct(&no_compression);
    select_struct(&no_compression);
}

#[derive(Clone, Debug, IntoCDRSValue, TryFromRow, PartialEq)]
struct RowStruct {
    key: i32,
    decimal: Decimal,
}

impl RowStruct {
    fn into_query_values(self) -> QueryValues {
        query_values!("key" => self.key, "decimal" => self.decimal)
    }
}

fn create_keyspace(session: &CurrentSession) {
    let create_ks: &'static str =
        "CREATE KEYSPACE IF NOT EXISTS test_decimal WITH REPLICATION = { \
         'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
    session.query(create_ks).expect("Keyspace creation error");
}

fn create_table(session: &CurrentSession) {
    let create_table_cql =
        "CREATE TABLE IF NOT EXISTS test_decimal.my_test_table (key int PRIMARY KEY, \
         decimal decimal);";
    session
        .query(create_table_cql)
        .expect("Table creation error");
}

fn insert_struct(session: &CurrentSession) {
    let row = RowStruct {
        key: 3i32,
        decimal: Decimal::from(1546998816i64),
    };

    let insert_struct_cql = "INSERT INTO test_decimal.my_test_table \
                             (key, decimal) VALUES (?, ?)";
    session
        .query_with_values(insert_struct_cql, row.into_query_values())
        .expect("insert");
}

fn select_struct(session: &CurrentSession) {
    let select_struct_cql = "SELECT * FROM test_decimal.my_test_table";
    let rows = session
        .query(select_struct_cql)
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");

    for row in rows {
        let my_row: RowStruct = RowStruct::try_from_row(row).expect("into RowStruct");
        println!("struct got: {:?}", my_row);
    }
}
