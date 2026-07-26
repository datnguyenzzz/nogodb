//! Integration tests for the `UPDATE ... SET ... [WHERE ...]` statement.
//!
//! These tests exercise the public API from the outside, like a real
//! downstream user would. Run with `cargo test --test sqlparser_update`.
//!
//! Tests assert only the public AST. `Update` is currently a struct with
//! private fields, so we extract the values we want to compare through
//! the parser's `Debug` output and through the public `Assignment` fields.

use query_engine::sql_parser::{
    Parser,
    ast::{
        dml::Update,
        expr::{Expr, Ident, Value},
        operators::BinaryOperator,
        query::TableFactor,
        statements::Statement,
    },
    tokenizer::{Location, Span},
};

// AST helpers

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

fn id_expr(name: &str) -> Expr {
    Expr::Identifier(id(name))
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

/// Extract the table name `Ident` from a `Update`'s `TableFactor::Table`.
fn table_name_ident(u: &Update) -> Ident {
    match &u.table {
        TableFactor::Table { name, .. } => name.clone(),
    }
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

fn parse_update(sql: &str) -> Update {
    match parse_one(sql) {
        Statement::Update(u) => u,
        other => panic!("expected Statement::Update from `{sql}`, got {other:?}"),
    }
}

fn try_parse(sql: &str) -> Result<Vec<Statement>, String> {
    let mut parser = Parser::default();
    parser.parse_sql(sql).map_err(|e| format!("{e:?}"))
}

// A. Shape

#[test]
fn update_no_where() {
    let u = parse_update("UPDATE t SET a = 1;");
    assert_eq!(table_name_ident(&u), id("t"));
    assert_eq!(u.assignments.len(), 1);
    assert_eq!(u.assignments[0].target, id("a"));
    assert_eq!(u.assignments[0].value, num("1"));
    assert!(u.selection.is_none());
}

#[test]
fn update_with_simple_where() {
    let u = parse_update("UPDATE users SET active = TRUE WHERE id = 1;");
    assert_eq!(table_name_ident(&u), id("users"));
    assert_eq!(u.assignments.len(), 1);
    assert_eq!(u.assignments[0].target, id("active"));
    assert_eq!(u.assignments[0].value, boolean(true));
    assert!(u.selection.is_some());
}

#[test]
fn update_lowercase_keywords() {
    let u = parse_update("update t set a = 1;");
    assert_eq!(table_name_ident(&u), id("t"));
    assert_eq!(u.assignments.len(), 1);
}

#[test]
fn update_mixed_case_keywords() {
    let u = parse_update("UpDaTe t SeT a = 1 WhErE a = 2;");
    assert_eq!(table_name_ident(&u), id("t"));
    assert_eq!(u.assignments[0].target, id("a"));
    assert!(u.selection.is_some());
}

#[test]
fn update_trailing_semicolon_consumed() {
    let u = parse_update("UPDATE t SET a = 1;");
    assert_eq!(table_name_ident(&u), id("t"));
}

#[test]
fn update_table_alias_unimplemented() {
    // The parser does not currently accept an alias on UPDATE.
    let err = try_parse("UPDATE t AS x SET a = 1;");
    assert!(
        err.is_err(),
        "alias on UPDATE is not yet supported, got: {err:?}"
    );
}

// B. Assignments

#[test]
fn update_single_assignment() {
    let u = parse_update("UPDATE t SET a = 1;");
    assert_eq!(u.assignments.len(), 1);
    assert_eq!(u.assignments[0].target, id("a"));
    assert_eq!(u.assignments[0].value, num("1"));
}

#[test]
fn update_two_assignments() {
    let u = parse_update("UPDATE t SET a = 1, b = 2;");
    assert_eq!(u.assignments.len(), 2);
    assert_eq!(u.assignments[0].target, id("a"));
    assert_eq!(u.assignments[0].value, num("1"));
    assert_eq!(u.assignments[1].target, id("b"));
    assert_eq!(u.assignments[1].value, num("2"));
}

#[test]
fn update_many_assignments() {
    let u = parse_update("UPDATE users SET name = 'foo', age = 18, active = TRUE, score = 0;");
    assert_eq!(u.assignments.len(), 4);
    assert_eq!(u.assignments[0].target, id("name"));
    assert_eq!(u.assignments[0].value, sq("foo"));
    assert_eq!(u.assignments[1].target, id("age"));
    assert_eq!(u.assignments[1].value, num("18"));
    assert_eq!(u.assignments[2].target, id("active"));
    assert_eq!(u.assignments[2].value, boolean(true));
    assert_eq!(u.assignments[3].target, id("score"));
    assert_eq!(u.assignments[3].value, num("0"));
}

#[test]
fn update_assignment_rhs_is_identifier() {
    let u = parse_update("UPDATE t SET a = b;");
    assert_eq!(u.assignments[0].value, id_expr("b"));
}

#[test]
fn update_assignment_rhs_is_arithmetic() {
    let u = parse_update("UPDATE t SET a = 1 + 2;");
    let expected = Expr::BinaryOp {
        left: Box::new(num("1")),
        op: BinaryOperator::Plus,
        right: Box::new(num("2")),
    };
    assert_eq!(u.assignments[0].value, expected);
}

#[test]
fn update_assignment_rhs_is_string_concat() {
    let u = parse_update("UPDATE t SET a = 'foo' || 'bar';");
    let expected = Expr::BinaryOp {
        left: Box::new(sq("foo")),
        op: BinaryOperator::StringConcat,
        right: Box::new(sq("bar")),
    };
    assert_eq!(u.assignments[0].value, expected);
}

#[test]
fn update_assignment_rhs_is_unary_minus() {
    let u = parse_update("UPDATE t SET a = -42;");
    let expected = Expr::UnaryOp {
        op: query_engine::sql_parser::ast::operators::UnaryOperator::Minus,
        expr: Box::new(num("42")),
    };
    assert_eq!(u.assignments[0].value, expected);
}

#[test]
fn update_assignment_rhs_is_null() {
    let u = parse_update("UPDATE t SET a = NULL;");
    assert_eq!(u.assignments[0].value, Expr::Value(Value::Null));
}

// C. WHERE clause

#[test]
fn update_where_simple_equality() {
    let u = parse_update("UPDATE t SET a = 1 WHERE a = 2;");
    let sel = u.selection.expect("expected WHERE clause");
    let expected = Expr::BinaryOp {
        left: Box::new(id_expr("a")),
        op: BinaryOperator::Eq,
        right: Box::new(num("2")),
    };
    assert_eq!(sel, expected);
}

#[test]
fn update_where_and_of_comparisons() {
    let u = parse_update("UPDATE t SET a = 1 WHERE a > 0 AND b < 10;");
    let sel = u.selection.expect("expected WHERE clause");
    let expected = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(id_expr("a")),
            op: BinaryOperator::Gt,
            right: Box::new(num("0")),
        }),
        op: BinaryOperator::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(id_expr("b")),
            op: BinaryOperator::Lt,
            right: Box::new(num("10")),
        }),
    };
    assert_eq!(sel, expected);
}

#[test]
fn update_no_where_sets_everything() {
    let u = parse_update("UPDATE t SET a = 1;");
    assert!(u.selection.is_none());
}

// D. Multi-statement

#[test]
fn update_followed_by_insert() {
    let mut parser = Parser::default();
    let stmts = parser
        .parse_sql("UPDATE t SET a = 1; INSERT INTO t VALUES (2);")
        .unwrap();
    assert_eq!(stmts.len(), 2);
    assert!(matches!(stmts[0], Statement::Update(_)));
    assert!(matches!(stmts[1], Statement::Insert(_)));
}

#[test]
fn two_updates_in_one_sql() {
    let mut parser = Parser::default();
    let stmts = parser
        .parse_sql("UPDATE t SET a = 1; UPDATE t SET a = 2;")
        .unwrap();
    assert_eq!(stmts.len(), 2);
    assert!(matches!(stmts[0], Statement::Update(_)));
    assert!(matches!(stmts[1], Statement::Update(_)));
}

// E. Whitespace

#[test]
fn update_whitespace_newlines_and_tabs() {
    let u = parse_update("UPDATE\n\tt\nSET\n\ta\t=\t1\nWHERE\n\ta\t=\t2\n;");
    assert_eq!(table_name_ident(&u), id("t"));
    assert_eq!(u.assignments[0].target, id("a"));
    assert_eq!(u.assignments[0].value, num("1"));
    assert!(u.selection.is_some());
}

#[test]
fn update_no_spaces_at_all() {
    let u = parse_update("UPDATE t SET a=1,b=2 WHERE a=0;");
    assert_eq!(u.assignments.len(), 2);
    assert_eq!(u.assignments[0].target, id("a"));
    assert_eq!(u.assignments[0].value, num("1"));
    assert_eq!(u.assignments[1].target, id("b"));
    assert_eq!(u.assignments[1].value, num("2"));
    assert!(u.selection.is_some());
}

// F. Negative / error cases

#[test]
fn err_update_missing_table() {
    assert!(try_parse("UPDATE SET a = 1;").is_err());
}

#[test]
fn err_update_missing_set() {
    assert!(try_parse("UPDATE t a = 1;").is_err());
}

#[test]
fn err_update_missing_equals() {
    assert!(try_parse("UPDATE t SET a 1;").is_err());
}

#[test]
fn err_update_missing_rhs() {
    assert!(try_parse("UPDATE t SET a =;").is_err());
}

#[test]
fn err_update_dangling_where() {
    assert!(try_parse("UPDATE t SET a = 1 WHERE;").is_err());
}

#[test]
fn err_update_dangling_comma_in_assignments() {
    assert!(try_parse("UPDATE t SET a = 1,;").is_err());
}

#[test]
fn err_update_table_is_literal() {
    assert!(try_parse("UPDATE 123 SET a = 1;").is_err());
}

#[test]
fn err_update_garbage_input() {
    assert!(try_parse("UPDATE t SET a = !@#;").is_err());
}
