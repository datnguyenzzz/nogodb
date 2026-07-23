//! Integration tests for ANSI SQL `CREATE TABLE` statements without
//! column-level constraints (NOT NULL, DEFAULT, PRIMARY KEY, etc.).
//!
//! These tests exercise the public API from the outside, like a real
//! downstream user would. Run with `cargo test --test sqlparser_ansisql`.

use query_engine::sql_parser::{
    Parser,
    ast::{
        data_type::{DataType, ExactNumberInfo},
        ddl::{ColumnDef, CreateTable},
        expr::Ident,
        statements::Statement,
    },
    tokenizer::{Location, Span},
};

// Helpers

/// Build a quoted-style-less `Ident` with a zero span.
///
/// The parser currently produces spans with all-zero line/column because
/// `Tokenizer::tokenize` is a stub; we mirror that here so the resulting
/// `Ident` is `PartialEq` to the parsed one.
fn ident(value: &str) -> Ident {
    Ident {
        value: value.to_string(),
        quote_style: None,
        span: zero_span(),
    }
}

const fn zero_span() -> Span {
    Span {
        start: Location { line: 0, column: 0 },
        end: Location { line: 0, column: 0 },
    }
}

/// Parse a SQL string, asserting that it produces exactly one statement.
fn parse_one(sql: &str) -> Statement {
    let mut parser = Parser::default();
    let stmts = parser
        .parse_sql(sql)
        .unwrap_or_else(|e| panic!("failed to parse `{sql}`: {e:?}"));
    assert_eq!(
        stmts.len(),
        1,
        "expected exactly one statement from `{sql}`, got {}",
        stmts.len(),
    );
    stmts.into_iter().next().unwrap()
}

/// Unwrap a `Statement::CreateTable` or panic with a useful message.
fn expect_create_table(stmt: Statement) -> CreateTable {
    match stmt {
        Statement::CreateTable(ct) => ct,
        other => panic!("expected CREATE TABLE statement, got {other:?}"),
    }
}

// Tests

#[test]
fn create_table_with_multiple_columns_no_constraints() {
    // One column per data type that `parse_data_type` recognises.
    // Every type the parser supports must appear here exactly once, so a
    // regression in any branch of `parse_data_type` shows up in this test.
    let sql = "CREATE TABLE all_types (\
        aaa INT, \
        bbb INT(11), \
        ccc BIGINT, \
        ddd BIGINT(20), \
        eee BOOLEAN, \
        fff DATE, \
        ggg FLOAT, \
        hhh FLOAT(8), \
        iii FLOAT(8, 2), \
        jjj DOUBLE, \
        kkk DOUBLE(10, 2), \
        lll DOUBLE PRECISION, \
        mmm DOUBLE PRECISION UNSIGNED, \
        nnn VARCHAR, \
        ooo VARCHAR(255)\
    )";

    let stmt = parse_one(sql);
    let ct = expect_create_table(stmt);

    let expected = CreateTable {
        table_name: ident("all_types"),
        columns: vec![
            ColumnDef {
                name: ident("aaa"),
                data_type: DataType::Int(None),
            },
            ColumnDef {
                name: ident("bbb"),
                data_type: DataType::Int(Some(11)),
            },
            ColumnDef {
                name: ident("ccc"),
                data_type: DataType::BigInt(None),
            },
            ColumnDef {
                name: ident("ddd"),
                data_type: DataType::BigInt(Some(20)),
            },
            ColumnDef {
                name: ident("eee"),
                data_type: DataType::Boolean,
            },
            ColumnDef {
                name: ident("fff"),
                data_type: DataType::Date,
            },
            ColumnDef {
                name: ident("ggg"),
                data_type: DataType::Float(ExactNumberInfo::None),
            },
            ColumnDef {
                name: ident("hhh"),
                data_type: DataType::Float(ExactNumberInfo::Precision(8)),
            },
            ColumnDef {
                name: ident("iii"),
                data_type: DataType::Float(ExactNumberInfo::PrecisionAndScale(8, 2)),
            },
            ColumnDef {
                name: ident("jjj"),
                data_type: DataType::Double(ExactNumberInfo::None),
            },
            ColumnDef {
                name: ident("kkk"),
                data_type: DataType::Double(ExactNumberInfo::PrecisionAndScale(10, 2)),
            },
            ColumnDef {
                name: ident("lll"),
                data_type: DataType::DoublePrecision,
            },
            ColumnDef {
                name: ident("mmm"),
                data_type: DataType::DoublePrecisionUnsigned,
            },
            ColumnDef {
                name: ident("nnn"),
                data_type: DataType::Varchar(None),
            },
            ColumnDef {
                name: ident("ooo"),
                data_type: DataType::Varchar(Some(255)),
            },
        ],
    };

    assert_eq!(ct, expected);
}

#[test]
fn create_table_uses_quoted_identifier() {
    // Backtick-quoted identifier should be preserved as `quote_style = Some('`')`.
    let stmt = parse_one("CREATE TABLE `orders` (`order_id` BIGINT)");
    let ct = expect_create_table(stmt);

    assert_eq!(ct.table_name.value, "orders");
    assert_eq!(ct.table_name.quote_style, Some('`'));

    assert_eq!(ct.columns.len(), 1);
    assert_eq!(ct.columns[0].name.value, "order_id");
    assert_eq!(ct.columns[0].name.quote_style, Some('`'));
    assert_eq!(ct.columns[0].data_type, DataType::BigInt(None));
}
