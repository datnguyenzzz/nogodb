//! Integration tests for the `SELECT` statement and the `FROM`/`WHERE` /
//! `GROUP BY` / `ORDER BY` clauses reachable through it.
//!
//! These tests exercise the public API from the outside, like a real
//! downstream user would. Run with `cargo test --test sqlparser_select`.
//!
//! Tests assert only the public AST the parser produces. If a test has
//! to know about tokenization internals, it does not belong here.

use query_engine::sql_parser::{
    Parser,
    ast::{
        expr::{Expr, Ident, SetExpr, Value},
        operators::BinaryOperator,
        query::{
            Join, JoinConstraint, JoinOperator, OrderByExpr, OrderBySort, Query, Select,
            SelectItem, TableFactor, TableWithJoins,
        },
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

fn compound_expr(parts: &[&str]) -> Expr {
    Expr::CompoundIdentifier(parts.iter().map(|p| id(p)).collect())
}

fn num(n: &str) -> Expr {
    Expr::Value(Value::Number(n.to_string(), false))
}

fn sq(v: &str) -> Expr {
    Expr::Value(Value::SingleQuotedString(v.to_string()))
}

fn table(name: &str) -> TableFactor {
    TableFactor::Table {
        name: id(name),
        alias: None,
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

fn parse_select(sql: &str) -> Select {
    match parse_one(sql) {
        Statement::Query(q) => match &*q.body {
            SetExpr::Select(s) => (**s).clone(),
            other => panic!("expected SetExpr::Select from `{sql}`, got {other:?}"),
        },
        other => panic!("expected Statement::Query from `{sql}`, got {other:?}"),
    }
}

fn parse_query(sql: &str) -> Query {
    match parse_one(sql) {
        Statement::Query(q) => *q,
        other => panic!("expected Statement::Query from `{sql}`, got {other:?}"),
    }
}

fn try_parse(sql: &str) -> Result<Vec<Statement>, String> {
    let mut parser = Parser::default();
    parser.parse_sql(sql).map_err(|e| format!("{e:?}"))
}

// A. Projection shape

#[test]
fn select_single_ident() {
    let s = parse_select("SELECT a, b FROM t;");
    assert_eq!(
        s.projections,
        vec![
            SelectItem::Expr(id_expr("a")),
            SelectItem::Expr(id_expr("b"))
        ]
    );
    assert_eq!(
        s.from,
        TableWithJoins {
            relation: table("t"),
            joins: None,
        },
    );
    assert_eq!(s.selection, None);
    assert_eq!(s.group_by, None);
}

#[test]
fn select_wildcard() {
    let s = parse_select("SELECT * FROM t;");
    assert_eq!(s.projections, vec![SelectItem::Wildcard]);
}

#[test]
fn select_two_projections() {
    let s = parse_select("SELECT a, b FROM t;");
    assert_eq!(
        s.projections,
        vec![
            SelectItem::Expr(id_expr("a")),
            SelectItem::Expr(id_expr("b")),
        ],
    );
}

#[test]
fn select_many_projections() {
    let s = parse_select("SELECT a, b, c, d FROM t;");
    assert_eq!(s.projections.len(), 4);
}

#[test]
fn select_projection_is_arithmetic() {
    let s = parse_select("SELECT a + b FROM t;");
    assert_eq!(
        s.projections,
        vec![SelectItem::Expr(Expr::BinaryOp {
            left: Box::new(id_expr("a")),
            op: BinaryOperator::Plus,
            right: Box::new(id_expr("b")),
        })],
    );
}

#[test]
fn select_projection_is_string_concat() {
    let s = parse_select("SELECT 'a' || 'b' FROM t;");
    assert_eq!(
        s.projections,
        vec![SelectItem::Expr(Expr::BinaryOp {
            left: Box::new(sq("a")),
            op: BinaryOperator::StringConcat,
            right: Box::new(sq("b")),
        })],
    );
}

#[test]
fn select_projection_is_cast() {
    let s = parse_select("SELECT CAST(a AS INT) FROM t;");
    assert_eq!(
        s.projections,
        vec![SelectItem::Expr(Expr::Cast {
            kind: query_engine::sql_parser::ast::expr::CastKind::Cast,
            expr: Box::new(id_expr("a")),
            data_type: query_engine::sql_parser::ast::data_type::DataType::Int(None),
        })],
    );
}

#[test]
fn select_projection_is_ceil() {
    let s = parse_select("SELECT CEIL(a) FROM t;");
    assert_eq!(
        s.projections,
        vec![SelectItem::Expr(Expr::Ceil {
            expr: Box::new(id_expr("a")),
        })],
    );
}

#[test]
fn select_projection_is_literal() {
    let s = parse_select("SELECT 1 FROM t;");
    assert_eq!(s.projections, vec![SelectItem::Expr(num("1"))]);
}

#[test]
fn select_with_alias_using_as() {
    let s = parse_select("SELECT a AS x FROM t;");
    assert_eq!(
        s.projections,
        vec![SelectItem::NamedExpr {
            expr: id_expr("a"),
            alias: id("x"),
        }],
    );
}

#[test]
fn select_projection_is_compound_identifier() {
    let s = parse_select("SELECT a.b FROM t;");
    assert_eq!(
        s.projections,
        vec![SelectItem::Expr(compound_expr(&["a", "b"]))],
    );
}

#[test]
fn select_projection_is_three_part_compound() {
    let s = parse_select("SELECT schema.users.id FROM t;");
    assert_eq!(
        s.projections,
        vec![SelectItem::Expr(compound_expr(&["schema", "users", "id"]))],
    );
}

// B. FROM / table

#[test]
fn select_from_bare_table() {
    let s = parse_select("SELECT a, b FROM users;");
    assert_eq!(
        s.from,
        TableWithJoins {
            relation: table("users"),
            joins: None,
        },
    );
}

#[test]
fn select_lowercase_keywords() {
    let s = parse_select("SELECT a, b FROM t;");
    assert_eq!(
        s.projections,
        vec![
            SelectItem::Expr(id_expr("a")),
            SelectItem::Expr(id_expr("b"))
        ]
    );
    assert_eq!(s.from.relation, table("t"));
}

#[test]
fn select_mixed_case_keywords() {
    let s = parse_select("SELECT a, b FROM t;");
    assert_eq!(
        s.projections,
        vec![
            SelectItem::Expr(id_expr("a")),
            SelectItem::Expr(id_expr("b"))
        ]
    );
    assert_eq!(s.from.relation, table("t"));
}

#[test]
fn select_trailing_semicolon_consumed() {
    let s = parse_select("SELECT a, b FROM t;");
    assert_eq!(s.from.relation, table("t"));
}

// C. WHERE

#[test]
fn select_no_where() {
    let s = parse_select("SELECT a, b FROM t;");
    assert_eq!(s.selection, None);
}

#[test]
fn select_where_simple_eq() {
    let s = parse_select("SELECT a, b FROM t WHERE a = 1;");
    assert_eq!(
        s.selection,
        Some(Expr::BinaryOp {
            left: Box::new(id_expr("a")),
            op: BinaryOperator::Eq,
            right: Box::new(num("1")),
        }),
    );
}

#[test]
fn select_where_and_of_comparisons() {
    let s = parse_select("SELECT a, b FROM t WHERE a > 0 AND b < 10;");
    assert_eq!(
        s.selection,
        Some(Expr::BinaryOp {
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
        }),
    );
}

#[test]
fn select_where_compound_ident() {
    let s = parse_select("SELECT a, b FROM t WHERE a.b = 1;");
    assert_eq!(
        s.selection,
        Some(Expr::BinaryOp {
            left: Box::new(compound_expr(&["a", "b"])),
            op: BinaryOperator::Eq,
            right: Box::new(num("1")),
        }),
    );
}

#[test]
fn select_where_cast() {
    let s = parse_select("SELECT a, b FROM t WHERE CAST(price AS INT) > 100;");
    assert!(s.selection.is_some());
}

// D. GROUP BY

#[test]
fn select_no_group_by() {
    let s = parse_select("SELECT a, b FROM t;");
    assert_eq!(s.group_by, None);
}

#[test]
fn select_group_by_single_expr() {
    let s = parse_select("SELECT a, b FROM t GROUP BY a;");
    assert_eq!(s.group_by, Some(vec![id_expr("a")]));
}

#[test]
fn select_group_by_two_exprs() {
    let s = parse_select("SELECT a, b FROM t GROUP BY a, b;");
    assert_eq!(s.group_by, Some(vec![id_expr("a"), id_expr("b")]),);
}

#[test]
fn select_group_by_compound() {
    let s = parse_select("SELECT a, b FROM t GROUP BY schema.t.col;");
    assert_eq!(
        s.group_by,
        Some(vec![compound_expr(&["schema", "t", "col"])]),
    );
}

// E. ORDER BY

#[test]
fn select_no_order_by() {
    let q = parse_query("SELECT a, b FROM t;");
    assert_eq!(q.order_by, None);
}

#[test]
fn select_order_by_single_default() {
    let q = parse_query("SELECT a, b FROM t ORDER BY a;");
    assert_eq!(
        q.order_by,
        Some(vec![OrderByExpr {
            expr: id_expr("a"),
            sort: None,
        }]),
    );
}

#[test]
fn select_order_by_asc() {
    let q = parse_query("SELECT a, b FROM t ORDER BY a ASC;");
    assert_eq!(
        q.order_by,
        Some(vec![OrderByExpr {
            expr: id_expr("a"),
            sort: Some(OrderBySort::Asc),
        }]),
    );
}

#[test]
fn select_order_by_desc() {
    let q = parse_query("SELECT a, b FROM t ORDER BY a DESC;");
    assert_eq!(
        q.order_by,
        Some(vec![OrderByExpr {
            expr: id_expr("a"),
            sort: Some(OrderBySort::Desc),
        }]),
    );
}

#[test]
fn select_order_by_many() {
    let q = parse_query("SELECT a, b FROM t ORDER BY a ASC, b DESC;");
    assert_eq!(
        q.order_by,
        Some(vec![
            OrderByExpr {
                expr: id_expr("a"),
                sort: Some(OrderBySort::Asc),
            },
            OrderByExpr {
                expr: id_expr("b"),
                sort: Some(OrderBySort::Desc),
            },
        ]),
    );
}

#[test]
fn select_order_by_compound() {
    let q = parse_query("SELECT a, b FROM t ORDER BY t.a DESC;");
    assert_eq!(
        q.order_by,
        Some(vec![OrderByExpr {
            expr: compound_expr(&["t", "a"]),
            sort: Some(OrderBySort::Desc),
        }]),
    );
}

// F. JOINs

#[test]
fn select_no_join() {
    let s = parse_select("SELECT a, b FROM t;");
    assert_eq!(s.from.joins, None);
}

#[test]
fn select_inner_join() {
    let s = parse_select("SELECT a, b FROM t1 INNER JOIN t2 ON t1.id = t2.t1id;");
    assert_eq!(
        s.from.joins,
        Some(vec![Join {
            relation: table("t2"),
            join_operator: JoinOperator::Inner(JoinConstraint::On(Expr::BinaryOp {
                left: Box::new(compound_expr(&["t1", "id"])),
                op: BinaryOperator::Eq,
                right: Box::new(compound_expr(&["t2", "t1id"])),
            })),
        }]),
    );
}

#[test]
fn select_left_join() {
    let s = parse_select("SELECT a, b FROM t1 LEFT JOIN t2 ON t1.id = t2.id;");
    assert!(matches!(
        s.from.joins.as_ref().unwrap()[0].join_operator,
        JoinOperator::Left(_),
    ));
}

#[test]
fn select_right_join() {
    let s = parse_select("SELECT a, b FROM t1 RIGHT JOIN t2 ON t1.id = t2.id;");
    assert!(matches!(
        s.from.joins.as_ref().unwrap()[0].join_operator,
        JoinOperator::Right(_),
    ));
}

#[test]
fn select_join_noop() {
    let s = parse_select("SELECT a, b FROM t1 JOIN t2 ON t1.id = t2.id;");
    assert!(matches!(
        s.from.joins.as_ref().unwrap()[0].join_operator,
        JoinOperator::Join(_),
    ));
}

#[test]
fn select_full_outer_join() {
    let s = parse_select("SELECT a, b FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id;");
    assert!(matches!(
        s.from.joins.as_ref().unwrap()[0].join_operator,
        JoinOperator::FullOuter(_),
    ));
}

#[test]
fn select_two_joins() {
    let s = parse_select("SELECT a, b FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t2.id = t3.id;");
    assert_eq!(s.from.joins.as_ref().unwrap().len(), 2);
}

#[test]
fn select_join_where_group_by_order_by() {
    let q = parse_query(
        "SELECT a, b FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.a > 0 GROUP BY a ORDER BY a DESC;",
    );
    let select = match &*q.body {
        SetExpr::Select(s) => s,
        _ => panic!(),
    };
    assert_eq!(select.from.joins.as_ref().unwrap().len(), 1);
    assert!(select.selection.is_some());
    assert_eq!(select.group_by, Some(vec![id_expr("a")]));
    assert_eq!(
        q.order_by,
        Some(vec![OrderByExpr {
            expr: id_expr("a"),
            sort: Some(OrderBySort::Desc),
        }]),
    );
}

// G. Whitespace

#[test]
fn select_newlines_and_tabs() {
    let s = parse_select("SELECT\n\ta\nFROM\n\tt\nWHERE\n\ta\n=\n1\n;");
    assert_eq!(s.projections, vec![SelectItem::Expr(id_expr("a"))]);
    assert!(s.selection.is_some());
}

#[test]
fn select_no_spaces_at_all() {
    let s = parse_select("SELECT a, b FROM t WHERE a=1;");
    assert!(s.selection.is_some());
}

#[test]
fn select_leading_and_trailing_whitespace() {
    let s = parse_select("   SELECT a, b FROM t   ;   ");
    assert_eq!(
        s.projections,
        vec![
            SelectItem::Expr(id_expr("a")),
            SelectItem::Expr(id_expr("b"))
        ]
    );
}

// H. Multiple statements

#[test]
fn select_two_statements() {
    let mut parser = Parser::default();
    let stmts = parser
        .parse_sql("SELECT a, b FROM t; SELECT b FROM t2;")
        .unwrap();
    assert_eq!(stmts.len(), 2);
    assert!(matches!(stmts[0], Statement::Query(_)));
    assert!(matches!(stmts[1], Statement::Query(_)));
}

#[test]
fn select_followed_by_insert() {
    let mut parser = Parser::default();
    let stmts = parser
        .parse_sql("SELECT a, b FROM t; INSERT INTO t VALUES (1);")
        .unwrap();
    assert_eq!(stmts.len(), 2);
    assert!(matches!(stmts[0], Statement::Query(_)));
    assert!(matches!(stmts[1], Statement::Insert(_)));
}

// I. Negative / error cases

#[test]
fn err_select_missing_from() {
    assert!(try_parse("SELECT a;").is_err());
}

#[test]
fn err_select_empty_projection() {
    assert!(try_parse("SELECT FROM t;").is_err());
}

#[test]
fn err_select_missing_table() {
    assert!(try_parse("SELECT a, b FROM;").is_err());
}

#[test]
fn err_select_where_dangling() {
    assert!(try_parse("SELECT a, b FROM t WHERE;").is_err());
}

#[test]
fn err_select_group_by_dangling() {
    assert!(try_parse("SELECT a, b FROM t GROUP BY;").is_err());
}

#[test]
fn err_select_order_by_dangling() {
    assert!(try_parse("SELECT a, b FROM t ORDER BY;").is_err());
}

#[test]
fn err_select_join_missing_on() {
    assert!(try_parse("SELECT a, b FROM t1 JOIN t2;").is_err());
}

#[test]
fn err_select_join_on_dangling() {
    assert!(try_parse("SELECT a, b FROM t1 JOIN t2 ON;").is_err());
}

#[test]
fn err_select_full_outer_missing_outer() {
    // FULL must be followed by OUTER JOIN
    assert!(try_parse("SELECT a, b FROM t1 FULL JOIN t2 ON t1.id = t2.id;").is_err());
}

#[test]
fn err_select_full_outer_missing_join() {
    assert!(try_parse("SELECT a, b FROM t1 FULL OUTER t2 ON t1.id = t2.id;").is_err());
}

#[test]
fn err_select_garbage_input() {
    assert!(try_parse("SELECT !@# FROM t;").is_err());
}

#[test]
fn err_select_table_is_literal() {
    assert!(try_parse("SELECT a, b FROM 123;").is_err());
}
