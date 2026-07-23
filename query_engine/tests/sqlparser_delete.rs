//! Integration tests for the `DELETE FROM ... [WHERE <expr>]` statement and
//! the expression grammar reachable through the WHERE clause.
//!
//! These tests exercise the public API from the outside, like a real
//! downstream user would. Run with `cargo test --test sqlparser_delete`.
//!
//! Tests are intentionally decoupled from the internal structure of the
//! parser: they only assert the public `Expr` / `Delete` / `Statement` AST
//! that the parser produces. If a test has to know about tokenization
//! internals, it does not belong here.

use query_engine::sql_parser::{
    Parser,
    ast::{
        data_type::DataType,
        dml::Delete,
        expr::{Expr, Ident, Value},
        operators::{BinaryOperator, UnaryOperator},
        query::TableFactor,
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

fn id_expr(name: &str) -> Expr {
    Expr::Identifier(id(name))
}

fn num(n: &str) -> Expr {
    Expr::Value(Value::Number(n.to_string(), false))
}

fn long(n: &str) -> Expr {
    Expr::Value(Value::Number(n.to_string(), true))
}

fn sq(v: &str) -> Expr {
    Expr::Value(Value::SingleQuotedString(v.to_string()))
}

fn dq(v: &str) -> Expr {
    Expr::Value(Value::DoubleQuotedString(v.to_string()))
}

fn boolean(b: bool) -> Expr {
    Expr::Value(Value::Boolean(b))
}

fn null() -> Expr {
    Expr::Value(Value::Null)
}

fn binop(l: Expr, op: BinaryOperator, r: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(l),
        op,
        right: Box::new(r),
    }
}

fn unop(op: UnaryOperator, e: Expr) -> Expr {
    Expr::UnaryOp {
        op,
        expr: Box::new(e),
    }
}

fn nested(e: Expr) -> Expr {
    Expr::Nested(Box::new(e))
}

fn table(name: &str) -> TableFactor {
    TableFactor::Table {
        name: id(name),
        alias: None,
    }
}

// Parse helpers

/// Parse a SQL string and return the first statement, panicking with a
/// descriptive message on failure.
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

/// Parse a `DELETE` statement and extract the `Delete` payload.
fn parse_delete(sql: &str) -> Delete {
    match parse_one(sql) {
        Statement::Delete(d) => d,
        other => panic!("expected Statement::Delete from `{sql}`, got {other:?}"),
    }
}

/// Parse a SQL string and return the parsed WHERE expression. The input must
/// be a `DELETE FROM t WHERE <expr>`; the expression is extracted and returned.
fn parse_where_expr(sql: &str) -> Expr {
    let delete = parse_delete(sql);
    delete
        .selection
        .unwrap_or_else(|| panic!("expected WHERE clause in `{sql}`"))
}

/// Try to parse a SQL string, returning the error if any.
fn try_parse(sql: &str) -> Result<Vec<Statement>, String> {
    let mut parser = Parser::default();
    parser.parse_sql(sql).map_err(|e| format!("{e:?}"))
}

// A. DELETE shape

#[test]
fn delete_no_where() {
    let d = parse_delete("DELETE FROM t;");
    assert_eq!(d.from, vec![table("t")]);
    assert_eq!(d.selection, None);
}

#[test]
fn delete_with_simple_where() {
    let d = parse_delete("DELETE FROM users WHERE id = 1;");
    assert_eq!(d.from, vec![table("users")]);
    assert_eq!(
        d.selection,
        Some(binop(id_expr("id"), BinaryOperator::Eq, num("1")))
    );
}

#[test]
fn delete_tautology_where() {
    let d = parse_delete("DELETE FROM t WHERE 1 = 1;");
    assert_eq!(
        d.selection,
        Some(binop(num("1"), BinaryOperator::Eq, num("1"))),
    );
}

#[test]
fn delete_trailing_semicolon_consumed() {
    let d = parse_delete("DELETE FROM t;");
    assert_eq!(d.from, vec![table("t")]);
}

#[test]
fn delete_lowercase_keywords() {
    let d = parse_delete("delete from t;");
    assert_eq!(d.from, vec![table("t")]);
    assert_eq!(d.selection, None);
}

#[test]
fn delete_mixed_case_keywords() {
    let d = parse_delete("DeLeTe FrOm t WhErE id = 1;");
    assert_eq!(d.from, vec![table("t")]);
    assert_eq!(
        d.selection,
        Some(binop(id_expr("id"), BinaryOperator::Eq, num("1")))
    );
}

#[test]
fn delete_with_table_alias_unimplemented() {
    // The parser does not currently accept an alias. This documents the gap:
    // until `DELETE FROM t AS x` is supported, this MUST fail to parse.
    let err = try_parse("DELETE FROM t AS x WHERE id = 1;");
    assert!(
        err.is_err(),
        "alias on DELETE FROM is not yet supported, got: {err:?}"
    );
}

// B. Expression atoms

#[test]
fn expr_bare_identifier() {
    assert_eq!(parse_where_expr("DELETE FROM t WHERE a;"), id_expr("a"));
}

#[test]
fn expr_integer_literal() {
    assert_eq!(parse_where_expr("DELETE FROM t WHERE 42;"), num("42"));
}

#[test]
fn expr_long_literal() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE 9999999999L;"),
        long("9999999999")
    );
}

#[test]
fn expr_string_literal_single_quoted() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE 'hello';"),
        sq("hello")
    );
}

#[test]
fn expr_string_literal_double_quoted() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE \"hello\";"),
        dq("hello")
    );
}

#[test]
fn expr_true_literal() {
    assert_eq!(parse_where_expr("DELETE FROM t WHERE TRUE;"), boolean(true));
}

#[test]
fn expr_false_literal() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE FALSE;"),
        boolean(false)
    );
}

#[test]
fn expr_null_literal() {
    assert_eq!(parse_where_expr("DELETE FROM t WHERE NULL;"), null());
}

#[test]
fn expr_paren_grouping() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE (1);"),
        nested(num("1"))
    );
}

#[test]
fn expr_paren_binary_inside() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE (1 + 2);"),
        nested(binop(num("1"), BinaryOperator::Plus, num("2"))),
    );
}

#[test]
fn expr_nested_paren_grouping() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE ((1 + 2));"),
        nested(nested(binop(num("1"), BinaryOperator::Plus, num("2")))),
    );
}

#[test]
fn expr_paren_resets_precedence() {
    // (1 + 2) * 3 must be *(+(1, 2), 3), not +(*(1, 2), 3)
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE (1 + 2) * 3;"),
        binop(
            nested(binop(num("1"), BinaryOperator::Plus, num("2"))),
            BinaryOperator::Multiply,
            num("3"),
        ),
    );
}

// C. Unary ops

#[test]
fn expr_unary_minus_literal() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE -42;"),
        unop(UnaryOperator::Minus, num("42")),
    );
}

#[test]
fn expr_unary_plus_literal() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE +42;"),
        unop(UnaryOperator::Plus, num("42")),
    );
}

#[test]
fn expr_double_unary_minus() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE --42;"),
        unop(UnaryOperator::Minus, unop(UnaryOperator::Minus, num("42"))),
    );
}

#[test]
fn expr_not_true() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE NOT TRUE;"),
        unop(UnaryOperator::Not, boolean(true)),
    );
}

#[test]
fn expr_not_false() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE NOT FALSE;"),
        unop(UnaryOperator::Not, boolean(false)),
    );
}

#[test]
fn expr_not_grouped_comparison() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE NOT (a = b);"),
        unop(
            UnaryOperator::Not,
            nested(binop(id_expr("a"), BinaryOperator::Eq, id_expr("b"))),
        ),
    );
}

#[test]
fn expr_unary_minus_on_identifier() {
    // -a: unary operand recurses at MulDivMod precedence, so the ident
    // is the operand.
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE -a;"),
        unop(UnaryOperator::Minus, id_expr("a")),
    );
}

#[test]
fn expr_unary_minus_in_sum() {
    // -a + b is +( (-a), b ) because the unary binds at MulDivMod(40),
    // tighter than PlusMinus(30).
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE -a + b;"),
        binop(
            unop(UnaryOperator::Minus, id_expr("a")),
            BinaryOperator::Plus,
            id_expr("b"),
        ),
    );
}

// D. Binary precedence

#[test]
fn prec_mul_binds_tighter_than_add() {
    // 1 + 2 * 3  =>  +(1, *(2, 3))
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE 1 + 2 * 3;"),
        binop(
            num("1"),
            BinaryOperator::Plus,
            binop(num("2"), BinaryOperator::Multiply, num("3")),
        ),
    );
}

#[test]
fn prec_add_left_associative() {
    // 1 + 2 + 3  =>  +(+(1, 2), 3)
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE 1 + 2 + 3;"),
        binop(
            binop(num("1"), BinaryOperator::Plus, num("2")),
            BinaryOperator::Plus,
            num("3"),
        ),
    );
}

#[test]
fn prec_sub_left_associative() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE 10 - 3 - 2;"),
        binop(
            binop(num("10"), BinaryOperator::Minus, num("3")),
            BinaryOperator::Minus,
            num("2"),
        ),
    );
}

#[test]
fn prec_div_left_associative() {
    // 100 / 10 / 2  =>  /(/(100, 10), 2) = 5, not /(100, /(10, 2)) = 20
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE 100 / 10 / 2;"),
        binop(
            binop(num("100"), BinaryOperator::Divide, num("10")),
            BinaryOperator::Divide,
            num("2"),
        ),
    );
}

#[test]
fn prec_and_binds_tighter_than_or() {
    // a OR b AND c  =>  OR(a, AND(b, c))
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE a OR b AND c;"),
        binop(
            id_expr("a"),
            BinaryOperator::Or,
            binop(id_expr("b"), BinaryOperator::And, id_expr("c")),
        ),
    );
}

#[test]
fn prec_or_left_associative() {
    // a OR b OR c  =>  OR(OR(a, b), c)
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE a OR b OR c;"),
        binop(
            binop(id_expr("a"), BinaryOperator::Or, id_expr("b")),
            BinaryOperator::Or,
            id_expr("c"),
        ),
    );
}

#[test]
fn prec_and_left_associative() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE a AND b AND c;"),
        binop(
            binop(id_expr("a"), BinaryOperator::And, id_expr("b")),
            BinaryOperator::And,
            id_expr("c"),
        ),
    );
}

#[test]
fn prec_and_or_combined_left_assoc() {
    // a AND b OR c AND d  =>  OR(AND(a, b), AND(c, d))
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE a AND b OR c AND d;"),
        binop(
            binop(id_expr("a"), BinaryOperator::And, id_expr("b")),
            BinaryOperator::Or,
            binop(id_expr("c"), BinaryOperator::And, id_expr("d")),
        ),
    );
}

#[test]
fn prec_eq_between_arith_and_logic() {
    // a = b OR c = d  =>  OR(=(a,b), =(c,d))  [Eq=20, Or=5]
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE a = b OR c = d;"),
        binop(
            binop(id_expr("a"), BinaryOperator::Eq, id_expr("b")),
            BinaryOperator::Or,
            binop(id_expr("c"), BinaryOperator::Eq, id_expr("d")),
        ),
    );
}

#[test]
fn prec_eq_left_associative() {
    // a = b = c  =>  =(=(a, b), c)  (degenerate but tests associativity)
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE a = b = c;"),
        binop(
            binop(id_expr("a"), BinaryOperator::Eq, id_expr("b")),
            BinaryOperator::Eq,
            id_expr("c"),
        ),
    );
}

#[test]
fn prec_not_binds_tighter_than_and() {
    // NOT a AND b  =>  AND(Not(a), b)  [UnaryNot=15 > And=10]
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE NOT a AND b;"),
        binop(
            unop(UnaryOperator::Not, id_expr("a")),
            BinaryOperator::And,
            id_expr("b"),
        ),
    );
}

#[test]
fn prec_compare_binds_looser_than_arith() {
    // a + b > c * d  =>  >(+(a, b), *(c, d))
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE a + b > c * d;"),
        binop(
            binop(id_expr("a"), BinaryOperator::Plus, id_expr("b")),
            BinaryOperator::Gt,
            binop(id_expr("c"), BinaryOperator::Multiply, id_expr("d")),
        ),
    );
}

#[test]
fn prec_not_binds_tighter_than_eq() {
    // NOT a = b  =>  =(Not(a), b)  [UnaryNot=15 < Eq=20, so NOT applies to `a` only]
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE NOT a = b;"),
        binop(
            unop(UnaryOperator::Not, id_expr("a")),
            BinaryOperator::Eq,
            id_expr("b"),
        ),
    );
}

#[test]
fn prec_modulo_with_arith() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE a % b + c;"),
        binop(
            binop(id_expr("a"), BinaryOperator::Modulo, id_expr("b")),
            BinaryOperator::Plus,
            id_expr("c"),
        ),
    );
}

#[test]
fn prec_string_concat_binds_like_mul() {
    // 'a' || 'b' || 'c'  =>  ||(||('a', 'b'), 'c')
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE 'a' || 'b' || 'c';"),
        binop(
            binop(sq("a"), BinaryOperator::StringConcat, sq("b")),
            BinaryOperator::StringConcat,
            sq("c"),
        ),
    );
}

// E. Binary op kinds

#[test]
fn ops_arithmetic_plus() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE 1 + 2;"),
        binop(num("1"), BinaryOperator::Plus, num("2")),
    );
}

#[test]
fn ops_arithmetic_minus() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE 5 - 3;"),
        binop(num("5"), BinaryOperator::Minus, num("3")),
    );
}

#[test]
fn ops_arithmetic_multiply() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE 4 * 3;"),
        binop(num("4"), BinaryOperator::Multiply, num("3")),
    );
}

#[test]
fn ops_arithmetic_divide() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE 10 / 2;"),
        binop(num("10"), BinaryOperator::Divide, num("2")),
    );
}

#[test]
fn ops_arithmetic_modulo() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE 10 % 3;"),
        binop(num("10"), BinaryOperator::Modulo, num("3")),
    );
}

#[test]
fn ops_string_concat() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE 'a' || 'b';"),
        binop(sq("a"), BinaryOperator::StringConcat, sq("b")),
    );
}

#[test]
fn ops_eq_and_double_eq_are_same() {
    // `=` and `==` both map to BinaryOperator::Eq
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE a = b;"),
        binop(id_expr("a"), BinaryOperator::Eq, id_expr("b")),
    );
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE a == b;"),
        binop(id_expr("a"), BinaryOperator::Eq, id_expr("b")),
    );
}

#[test]
fn ops_not_eq() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE a <> b;"),
        binop(id_expr("a"), BinaryOperator::NotEq, id_expr("b")),
    );
}

#[test]
fn ops_lt_le_gt_ge() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE a < b;"),
        binop(id_expr("a"), BinaryOperator::Lt, id_expr("b")),
    );
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE a <= b;"),
        binop(id_expr("a"), BinaryOperator::LtEq, id_expr("b")),
    );
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE a > b;"),
        binop(id_expr("a"), BinaryOperator::Gt, id_expr("b")),
    );
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE a >= b;"),
        binop(id_expr("a"), BinaryOperator::GtEq, id_expr("b")),
    );
}

#[test]
fn ops_logical_xor() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE a XOR b;"),
        binop(id_expr("a"), BinaryOperator::Xor, id_expr("b")),
    );
}

#[test]
fn ops_logical_and_or_case_insensitive() {
    assert_eq!(
        parse_where_expr("DELETE FROM t where a and b;"),
        binop(id_expr("a"), BinaryOperator::And, id_expr("b")),
    );
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE a oR b;"),
        binop(id_expr("a"), BinaryOperator::Or, id_expr("b")),
    );
}

// F. CAST / TRY_CAST / CEIL / FLOOR

#[test]
fn cast_basic_int() {
    let e = parse_where_expr("DELETE FROM t WHERE CAST(x AS INT);");
    assert_eq!(
        e,
        Expr::Cast {
            kind: query_engine::sql_parser::ast::expr::CastKind::Cast,
            expr: Box::new(id_expr("x")),
            data_type: DataType::Int(None),
        },
    );
}

#[test]
fn cast_with_precision() {
    let e = parse_where_expr("DELETE FROM t WHERE CAST(x AS INT(4));");
    assert_eq!(
        e,
        Expr::Cast {
            kind: query_engine::sql_parser::ast::expr::CastKind::Cast,
            expr: Box::new(id_expr("x")),
            data_type: DataType::Int(Some(4)),
        },
    );
}

#[test]
fn cast_to_varchar_with_length() {
    let e = parse_where_expr("DELETE FROM t WHERE CAST(x AS VARCHAR(10));");
    assert_eq!(
        e,
        Expr::Cast {
            kind: query_engine::sql_parser::ast::expr::CastKind::Cast,
            expr: Box::new(id_expr("x")),
            data_type: DataType::Varchar(Some(10)),
        },
    );
}

#[test]
fn cast_to_bigint() {
    let e = parse_where_expr("DELETE FROM t WHERE CAST(x AS BIGINT);");
    assert_eq!(
        e,
        Expr::Cast {
            kind: query_engine::sql_parser::ast::expr::CastKind::Cast,
            expr: Box::new(id_expr("x")),
            data_type: DataType::BigInt(None),
        },
    );
}

#[test]
fn cast_to_boolean() {
    let e = parse_where_expr("DELETE FROM t WHERE CAST(x AS BOOLEAN);");
    assert_eq!(
        e,
        Expr::Cast {
            kind: query_engine::sql_parser::ast::expr::CastKind::Cast,
            expr: Box::new(id_expr("x")),
            data_type: DataType::Boolean,
        },
    );
}

#[test]
fn cast_to_double_precision() {
    let e = parse_where_expr("DELETE FROM t WHERE CAST(x AS DOUBLE PRECISION);");
    assert_eq!(
        e,
        Expr::Cast {
            kind: query_engine::sql_parser::ast::expr::CastKind::Cast,
            expr: Box::new(id_expr("x")),
            data_type: DataType::DoublePrecision,
        },
    );
}

#[test]
fn try_cast_basic() {
    let e = parse_where_expr("DELETE FROM t WHERE TRY_CAST(x AS FLOAT);");
    assert_eq!(
        e,
        Expr::Cast {
            kind: query_engine::sql_parser::ast::expr::CastKind::TryCast,
            expr: Box::new(id_expr("x")),
            data_type: DataType::Float(
                query_engine::sql_parser::ast::data_type::ExactNumberInfo::None,
            ),
        },
    );
}

#[test]
fn cast_of_arithmetic() {
    // CAST(1 + 1 AS INT)  — the inner expression is a full sub-expression
    let e = parse_where_expr("DELETE FROM t WHERE CAST(1 + 1 AS INT);");
    assert_eq!(
        e,
        Expr::Cast {
            kind: query_engine::sql_parser::ast::expr::CastKind::Cast,
            expr: Box::new(binop(num("1"), BinaryOperator::Plus, num("1"))),
            data_type: DataType::Int(None),
        },
    );
}

#[test]
fn ceil_of_identifier() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE CEIL(x);"),
        Expr::Ceil {
            expr: Box::new(id_expr("x"))
        },
    );
}

#[test]
fn floor_of_identifier() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE FLOOR(x);"),
        Expr::Floor {
            expr: Box::new(id_expr("x"))
        },
    );
}

#[test]
fn ceil_of_literal() {
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE CEIL(1.5);"),
        Expr::Ceil {
            expr: Box::new(Expr::Value(Value::Number("1.5".to_string(), false)))
        },
    );
}

#[test]
fn cast_of_ceil() {
    let e = parse_where_expr("DELETE FROM t WHERE CAST(CEIL(x) AS INT);");
    assert_eq!(
        e,
        Expr::Cast {
            kind: query_engine::sql_parser::ast::expr::CastKind::Cast,
            expr: Box::new(Expr::Ceil {
                expr: Box::new(id_expr("x"))
            }),
            data_type: DataType::Int(None),
        },
    );
}

#[test]
fn cast_inside_comparison() {
    // end-to-end: CAST(price AS INT) > 10
    let e = parse_where_expr("DELETE FROM t WHERE CAST(price AS INT) > 10;");
    assert_eq!(
        e,
        binop(
            Expr::Cast {
                kind: query_engine::sql_parser::ast::expr::CastKind::Cast,
                expr: Box::new(id_expr("price")),
                data_type: DataType::Int(None),
            },
            BinaryOperator::Gt,
            num("10"),
        ),
    );
}

#[test]
fn ceil_used_in_arithmetic() {
    // CEIL(x) + 1
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE CEIL(x) + 1;"),
        binop(
            Expr::Ceil {
                expr: Box::new(id_expr("x"))
            },
            BinaryOperator::Plus,
            num("1"),
        ),
    );
}

// G. Realistic DELETE scenarios

#[test]
fn realistic_and_of_comparisons() {
    // a > 1 AND b < 2
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE a > 1 AND b < 2;"),
        binop(
            binop(id_expr("a"), BinaryOperator::Gt, num("1")),
            BinaryOperator::And,
            binop(id_expr("b"), BinaryOperator::Lt, num("2")),
        ),
    );
}

#[test]
fn realistic_grouped_arith_in_compare() {
    // (a + b) * c = 10
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE (a + b) * c = 10;"),
        binop(
            binop(
                nested(binop(id_expr("a"), BinaryOperator::Plus, id_expr("b"))),
                BinaryOperator::Multiply,
                id_expr("c"),
            ),
            BinaryOperator::Eq,
            num("10"),
        ),
    );
}

#[test]
fn realistic_not_with_keyword_ident() {
    // `active` is a non-reserved word used as a column
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE NOT active;"),
        unop(UnaryOperator::Not, id_expr("active")),
    );
}

#[test]
fn realistic_string_eq_and_age_compare() {
    // name = 'foo' AND age > 18
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE name = 'foo' AND age > 18;"),
        binop(
            binop(id_expr("name"), BinaryOperator::Eq, sq("foo")),
            BinaryOperator::And,
            binop(id_expr("age"), BinaryOperator::Gt, num("18")),
        ),
    );
}

#[test]
fn realistic_or_of_neq() {
    // status <> 'active' OR status <> 'pending'
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE status <> 'active' OR status <> 'pending';"),
        binop(
            binop(id_expr("status"), BinaryOperator::NotEq, sq("active")),
            BinaryOperator::Or,
            binop(id_expr("status"), BinaryOperator::NotEq, sq("pending")),
        ),
    );
}

#[test]
fn realistic_nested_not_and_or() {
    // NOT (a = 1) AND (b = 2 OR c = 3)
    assert_eq!(
        parse_where_expr("DELETE FROM t WHERE NOT (a = 1) AND (b = 2 OR c = 3);"),
        binop(
            unop(
                UnaryOperator::Not,
                nested(binop(id_expr("a"), BinaryOperator::Eq, num("1"))),
            ),
            BinaryOperator::And,
            nested(binop(
                binop(id_expr("b"), BinaryOperator::Eq, num("2")),
                BinaryOperator::Or,
                binop(id_expr("c"), BinaryOperator::Eq, num("3")),
            )),
        ),
    );
}

#[test]
fn realistic_cast_in_where() {
    // CAST(price AS INT) > 100 AND active = TRUE
    let e = parse_where_expr("DELETE FROM t WHERE CAST(price AS INT) > 100 AND active = TRUE;");
    assert_eq!(
        e,
        binop(
            binop(
                Expr::Cast {
                    kind: query_engine::sql_parser::ast::expr::CastKind::Cast,
                    expr: Box::new(id_expr("price")),
                    data_type: DataType::Int(None),
                },
                BinaryOperator::Gt,
                num("100"),
            ),
            BinaryOperator::And,
            binop(id_expr("active"), BinaryOperator::Eq, boolean(true)),
        ),
    );
}

// H. Negative / error cases

#[test]
fn err_delete_missing_table() {
    assert!(try_parse("DELETE FROM;").is_err());
}

#[test]
fn err_delete_missing_from() {
    assert!(try_parse("DELETE t;").is_err());
}

#[test]
fn err_delete_dangling_where() {
    assert!(try_parse("DELETE FROM t WHERE;").is_err());
}

#[test]
fn err_unclosed_paren_in_where() {
    assert!(try_parse("DELETE FROM t WHERE ((1 + 2);").is_err());
}

#[test]
fn err_unclosed_paren_in_cast() {
    assert!(try_parse("DELETE FROM t WHERE CAST(x AS INT;").is_err());
}

#[test]
fn err_table_is_literal() {
    assert!(try_parse("DELETE FROM 123;").is_err());
}

#[test]
fn err_dangling_binary_op() {
    assert!(try_parse("DELETE FROM t WHERE 1 +;").is_err());
}

#[test]
fn err_dangling_unary_op() {
    assert!(try_parse("DELETE FROM t WHERE +;").is_err());
}

#[test]
fn err_garbage_input() {
    assert!(try_parse("DELETE FROM t WHERE !@#;").is_err());
}

#[test]
fn err_garbage_keyword_as_prefix() {
    // SELECT is not a valid expression prefix.
    assert!(try_parse("DELETE FROM t WHERE SELECT 1;").is_err());
}

#[test]
fn err_empty_where() {
    // `WHERE` followed by EOF
    assert!(try_parse("DELETE FROM t WHERE").is_err());
}

// I. Whitespace handling

#[test]
fn ws_leading_and_trailing() {
    let d = parse_delete("   DELETE FROM t   ;   ");
    assert_eq!(d.from, vec![table("t")]);
}

#[test]
fn ws_newlines_and_tabs() {
    let d = parse_delete("DELETE\n\tFROM\tt\nWHERE\n\tid\t=\t1\n;");
    assert_eq!(d.from, vec![table("t")]);
    assert_eq!(
        d.selection,
        Some(binop(id_expr("id"), BinaryOperator::Eq, num("1"))),
    );
}

#[test]
fn ws_no_spaces_at_all() {
    // The parser must still tokenize correctly when keywords run into idents.
    let d = parse_delete("DELETE FROM t WHERE a=1;");
    assert_eq!(
        d.selection,
        Some(binop(id_expr("a"), BinaryOperator::Eq, num("1"))),
    );
}
