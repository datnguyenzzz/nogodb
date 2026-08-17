//! Integration tests for the `INSERT INTO ... [VALUES (...)]` statement.
//!
//! These tests exercise the public API from the outside, like a real
//! downstream user would. Run with `cargo test --test sqlparser_insert`.
//!
//! Tests assert only the public `Insert` / `Query` / `Statement` AST the
//! parser produces. If a test has to know about tokenization internals, it
//! does not belong here.

use crate::sql_parser::{
    Parser,
    ast::{
        dml::Insert,
        expr::{Expr, Ident, Parens, SetExpr, Value},
        query::{Query, TableFactor},
        statements::Statement,
    },
    tokenizer::{Location, Span},
};

// AST construction helpers

const fn zero_span() -> Span {
    Span {
        start: Location { line: 0, column: 0 },
        end: Location { line: 0, column: 0 },
    }
}

fn id(name: &str) -> Ident {
    Ident {
        value: name.to_string(),
        quote_style: None,
        span: zero_span(),
    }
}

fn table_factor(value: &str, alias: Option<Ident>) -> TableFactor {
    TableFactor::Table {
        name: id(value),
        alias: alias,
    }
}

fn id_expr(name: &str) -> Expr {
    Expr::Identifier(id(name))
}

fn compound_expr(parts: &[&str]) -> Expr {
    Expr::CompoundIdentifier(parts.iter().map(|p| id(p)).collect())
}

fn num(n: &str) -> Expr {
    Expr::Value(Value::Number(n.to_string(), false))
}

fn sq(v: &str) -> Expr {
    Expr::Value(Value::SingleQuotedString(v.to_string()))
}

fn boolean(b: bool) -> Expr {
    Expr::Value(Value::Boolean(b))
}

fn null() -> Expr {
    Expr::Value(Value::Null)
}

fn values_query(exprs: Vec<Expr>) -> Box<Query> {
    Box::new(Query {
        body: Box::new(SetExpr::Values(Parens { content: exprs })),
        order_by: None,
    })
}

fn insert_stmt(table: &str, columns: Vec<Ident>, values: Vec<Expr>) -> Statement {
    Statement::Insert(Insert {
        table: table_factor(table, None),
        columns,
        source: Some(values_query(values)),
    })
}

// Parse helpers

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

fn parse_insert(sql: &str) -> Insert {
    match parse_one(sql) {
        Statement::Insert(i) => i,
        other => panic!("expected Statement::Insert from `{sql}`, got {other:?}"),
    }
}

fn try_parse(sql: &str) -> Result<Vec<Statement>, String> {
    let mut parser = Parser::default();
    parser.parse_sql(sql).map_err(|e| format!("{e:?}"))
}

// A. Shape

#[test]
fn insert_into_table_with_values() {
    let i = parse_insert("INSERT INTO t VALUES (1);");
    let table_name = match &i.table {
        TableFactor::Table { name, .. } => name.clone(),
    };
    assert_eq!(table_name, id("t"));
    assert_eq!(i.columns, vec![]);
    assert_eq!(i.source, Some(values_query(vec![num("1")])),);
}

#[test]
fn insert_into_table_with_columns_and_values() {
    let i = parse_insert("INSERT INTO t (a, b) VALUES (1, 2);");
    let table_name = match &i.table {
        TableFactor::Table { name, .. } => name.clone(),
    };
    assert_eq!(table_name, id("t"));
    assert_eq!(i.columns, vec![id("a"), id("b")]);
    assert_eq!(i.source, Some(values_query(vec![num("1"), num("2")])),);
}

#[test]
fn insert_lowercase_keywords() {
    let i = parse_insert("insert into t values (1);");
    let table_name = match &i.table {
        TableFactor::Table { name, .. } => name.clone(),
    };
    assert_eq!(table_name, id("t"));
    assert_eq!(i.columns, vec![]);
    assert_eq!(i.source, Some(values_query(vec![num("1")])));
}

#[test]
fn insert_mixed_case_keywords() {
    let i = parse_insert("InSeRt InTo t (a) VaLuEs (1);");
    let table_name = match &i.table {
        TableFactor::Table { name, .. } => name.clone(),
    };
    assert_eq!(table_name, id("t"));
    assert_eq!(i.columns, vec![id("a")]);
    assert_eq!(i.source, Some(values_query(vec![num("1")])));
}

#[test]
fn insert_trailing_semicolon_consumed() {
    let i = parse_insert("INSERT INTO t VALUES (1);");
    let table_name = match &i.table {
        TableFactor::Table { name, .. } => name.clone(),
    };
    assert_eq!(table_name, id("t"));
}

// B. Multiple columns / values

#[test]
fn insert_single_column_single_value() {
    let i = parse_insert("INSERT INTO t (a) VALUES (1);");
    assert_eq!(i.columns, vec![id("a")]);
    assert_eq!(i.source, Some(values_query(vec![num("1")])));
}

#[test]
fn insert_many_columns_many_values() {
    let i = parse_insert("INSERT INTO users (id, name, age, active) VALUES (1, 'foo', 18, TRUE);");
    assert_eq!(
        i.columns,
        vec![id("id"), id("name"), id("age"), id("active")],
    );
    assert_eq!(
        i.source,
        Some(values_query(vec![
            num("1"),
            sq("foo"),
            num("18"),
            boolean(true),
        ])),
    );
}

#[test]
fn insert_three_columns_three_values() {
    let i = parse_insert("INSERT INTO t (a, b, c) VALUES (1, 2, 3);");
    assert_eq!(i.columns, vec![id("a"), id("b"), id("c")]);
    assert_eq!(
        i.source,
        Some(values_query(vec![num("1"), num("2"), num("3")])),
    );
}

// C. Value expressions in VALUES (...)

#[test]
fn insert_values_string_literal() {
    let i = parse_insert("INSERT INTO t (name) VALUES ('hello');");
    assert_eq!(i.source, Some(values_query(vec![sq("hello")])));
}

#[test]
fn insert_values_null_literal() {
    let i = parse_insert("INSERT INTO t (a) VALUES (NULL);");
    assert_eq!(i.source, Some(values_query(vec![null()])));
}

#[test]
fn insert_values_boolean_literal() {
    let i = parse_insert("INSERT INTO t (a) VALUES (TRUE);");
    assert_eq!(i.source, Some(values_query(vec![boolean(true)])));
}

#[test]
fn insert_values_identifier() {
    let i = parse_insert("INSERT INTO t (a) VALUES (b);");
    assert_eq!(i.source, Some(values_query(vec![id_expr("b")])));
}

#[test]
fn insert_values_arithmetic_expression() {
    // 1 + 2 is a full sub-expression inside VALUES
    let i = parse_insert("INSERT INTO t (a) VALUES (1 + 2);");
    let expected = Expr::BinaryOp {
        left: Box::new(num("1")),
        op: crate::sql_parser::ast::operators::BinaryOperator::Plus,
        right: Box::new(num("2")),
    };
    assert_eq!(i.source, Some(values_query(vec![expected])));
}

#[test]
fn insert_values_mixed_types() {
    let i = parse_insert("INSERT INTO t (a, b, c, d) VALUES (1, 'x', TRUE, NULL);");
    assert_eq!(
        i.source,
        Some(values_query(
            vec![num("1"), sq("x"), boolean(true), null(),]
        )),
    );
}

#[test]
fn insert_values_negative_literal() {
    // -42 is a unary minus on the literal
    let i = parse_insert("INSERT INTO t (a) VALUES (-42);");
    let expected = Expr::UnaryOp {
        op: crate::sql_parser::ast::operators::UnaryOperator::Minus,
        expr: Box::new(num("42")),
    };
    assert_eq!(i.source, Some(values_query(vec![expected])));
}

// D. Multiple statements

#[test]
fn insert_two_statements_in_one_sql() {
    let mut parser = Parser::default();
    let stmts = parser
        .parse_sql("INSERT INTO t VALUES (1); INSERT INTO t VALUES (2);")
        .unwrap();
    assert_eq!(stmts.len(), 2);
    assert_eq!(stmts[0], insert_stmt("t", vec![], vec![num("1")]));
    assert_eq!(stmts[1], insert_stmt("t", vec![], vec![num("2")]));
}

#[test]
fn insert_followed_by_delete() {
    let mut parser = Parser::default();
    let stmts = parser
        .parse_sql("INSERT INTO t VALUES (1); DELETE FROM t;")
        .unwrap();
    assert_eq!(stmts.len(), 2);
    assert!(matches!(stmts[0], Statement::Insert(_)));
    assert!(matches!(stmts[1], Statement::Delete(_)));
}

// E. Whitespace handling

#[test]
fn insert_whitespace_newlines_and_tabs() {
    let i = parse_insert("INSERT\n\tINTO\tt\n(\ta,\tb\t)\nVALUES\n\t(\t1,\t2\t)\n;");
    let table_name = match &i.table {
        TableFactor::Table { name, .. } => name.clone(),
    };
    assert_eq!(table_name, id("t"));
    assert_eq!(i.columns, vec![id("a"), id("b")]);
    assert_eq!(i.source, Some(values_query(vec![num("1"), num("2")])),);
}

#[test]
fn insert_no_spaces_at_all() {
    let i = parse_insert("INSERT INTO t(a,b)VALUES(1,2);");
    assert_eq!(i.columns, vec![id("a"), id("b")]);
    assert_eq!(i.source, Some(values_query(vec![num("1"), num("2")])),);
}

#[test]
fn insert_leading_and_trailing_whitespace() {
    let i = parse_insert("   INSERT INTO t VALUES (1)   ;   ");
    let table_name = match &i.table {
        TableFactor::Table { name, .. } => name.clone(),
    };
    assert_eq!(table_name, id("t"));
    assert_eq!(i.source, Some(values_query(vec![num("1")])));
}

// F. Negative / error cases

#[test]
fn err_insert_missing_into() {
    assert!(try_parse("INSERT t VALUES (1);").is_err());
}

#[test]
fn err_insert_missing_table() {
    assert!(try_parse("INSERT INTO VALUES (1);").is_err());
}

#[test]
fn err_insert_missing_values_keyword() {
    // no VALUES keyword after the column list
    assert!(try_parse("INSERT INTO t (a) (1);").is_err());
}

#[test]
fn err_insert_missing_values_clause() {
    // VALUES keyword but no value list
    assert!(try_parse("INSERT INTO t VALUES;").is_err());
}

#[test]
fn err_insert_unclosed_paren_in_columns() {
    assert!(try_parse("INSERT INTO t (a, b VALUES (1);").is_err());
}

#[test]
fn err_insert_unclosed_paren_in_values() {
    assert!(try_parse("INSERT INTO t (a) VALUES (1;").is_err());
}

#[test]
fn err_insert_empty_columns() {
    // The parser requires 1+ columns when the column list is present.
    // `INSERT INTO t () VALUES (1);` is not yet supported.
    let err = try_parse("INSERT INTO t () VALUES (1);");
    assert!(
        err.is_err(),
        "empty column list is not yet supported, got: {err:?}"
    );
}

#[test]
fn err_insert_dangling_comma_in_values() {
    // trailing comma with no value after
    assert!(try_parse("INSERT INTO t VALUES (1,);").is_err());
}

#[test]
fn err_insert_dangling_comma_in_columns() {
    assert!(try_parse("INSERT INTO t (a,) VALUES (1);").is_err());
}

#[test]
fn err_insert_table_is_literal() {
    assert!(try_parse("INSERT INTO 123 VALUES (1);").is_err());
}

#[test]
fn err_insert_garbage_input() {
    assert!(try_parse("INSERT INTO t !@#;").is_err());
}

// G. Compound identifiers in VALUES

#[test]
fn insert_value_is_compound_identifier() {
    // VALUES (a.b) — the inserted value is a two-part identifier.
    let i = parse_insert("INSERT INTO t (x) VALUES (a.b);");
    assert_eq!(
        i.source,
        Some(values_query(vec![compound_expr(&["a", "b"])]))
    );
}

#[test]
fn insert_value_is_three_part_compound() {
    let i = parse_insert("INSERT INTO t (x) VALUES (schema.users.id);");
    assert_eq!(
        i.source,
        Some(values_query(vec![compound_expr(&[
            "schema", "users", "id"
        ])])),
    );
}

#[test]
fn insert_value_is_four_part_compound() {
    let i = parse_insert("INSERT INTO t (x) VALUES (catalog.schema.users.id);");
    assert_eq!(
        i.source,
        Some(values_query(vec![compound_expr(&[
            "catalog", "schema", "users", "id",
        ])])),
    );
}

#[test]
fn insert_mix_compound_and_literal() {
    // (a.b, 1, 'foo')
    let i = parse_insert("INSERT INTO t (a, b, c) VALUES (a.b, 1, 'foo');");
    assert_eq!(
        i.source,
        Some(values_query(vec![
            compound_expr(&["a", "b"]),
            num("1"),
            sq("foo"),
        ])),
    );
}

#[test]
fn insert_compound_in_arithmetic() {
    // VALUES (a.b + 1)
    let i = parse_insert("INSERT INTO t (x) VALUES (a.b + 1);");
    let expected = Expr::BinaryOp {
        left: Box::new(compound_expr(&["a", "b"])),
        op: crate::sql_parser::ast::operators::BinaryOperator::Plus,
        right: Box::new(num("1")),
    };
    assert_eq!(i.source, Some(values_query(vec![expected])));
}

#[test]
fn insert_compound_in_comparison() {
    // VALUES (a.b = 1)
    let i = parse_insert("INSERT INTO t (x) VALUES (a.b = 1);");
    let expected = Expr::BinaryOp {
        left: Box::new(compound_expr(&["a", "b"])),
        op: crate::sql_parser::ast::operators::BinaryOperator::Eq,
        right: Box::new(num("1")),
    };
    assert_eq!(i.source, Some(values_query(vec![expected])));
}

#[test]
fn insert_compound_in_cast() {
    let i = parse_insert("INSERT INTO t (x) VALUES (CAST(a.b AS INT));");
    let expected = Expr::Cast {
        kind: crate::sql_parser::ast::expr::CastKind::Cast,
        expr: Box::new(compound_expr(&["a", "b"])),
        data_type: crate::sql_parser::ast::data_type::DataType::Int(None),
    };
    assert_eq!(i.source, Some(values_query(vec![expected])));
}

#[test]
fn insert_compound_in_ceil() {
    let i = parse_insert("INSERT INTO t (x) VALUES (CEIL(a.b));");
    let expected = Expr::Ceil {
        expr: Box::new(compound_expr(&["a", "b"])),
    };
    assert_eq!(i.source, Some(values_query(vec![expected])));
}

#[test]
fn insert_compound_both_sides_of_eq() {
    // WHERE a.b = c.d (here as a value expression)
    let i = parse_insert("INSERT INTO t (x) VALUES (a.b = c.d);");
    let expected = Expr::BinaryOp {
        left: Box::new(compound_expr(&["a", "b"])),
        op: crate::sql_parser::ast::operators::BinaryOperator::Eq,
        right: Box::new(compound_expr(&["c", "d"])),
    };
    assert_eq!(i.source, Some(values_query(vec![expected])));
}

#[test]
fn insert_err_dangling_period_in_value() {
    // VALUES (a.) — period must be followed by another ident.
    assert!(try_parse("INSERT INTO t (x) VALUES (a.);").is_err());
}

#[test]
fn insert_err_dangling_period_in_compound() {
    // VALUES (a.b.c.) — trailing period fails.
    assert!(try_parse("INSERT INTO t (x) VALUES (a.b.c.);").is_err());
}
