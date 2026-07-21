use crate::sql_parser::{
    ast::expr::{Expr, Ident, SetExpr},
    tokenizer::TokenWithSpan,
};

/// Represents how two tables are constrained in a join: `ON`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JoinConstraint {
    /// `ON <expr>` join condition.
    On(Expr),
    // TODO: Support CROSS Join
}

/// The operator used for joining two tables, e.g. `INNER`, `LEFT`, etc.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JoinOperator {
    /// Generic `JOIN` with an optional constraint.
    Join(JoinConstraint),
    /// `INNER JOIN` with an optional constraint.
    Inner(JoinConstraint),
    /// `LEFT JOIN` with an optional constraint.
    Left(JoinConstraint),
    /// `LEFT OUTER JOIN` with an optional constraint.
    LeftOuter(JoinConstraint),
    /// `RIGHT JOIN` with an optional constraint.
    Right(JoinConstraint),
    /// `RIGHT OUTER JOIN` with an optional constraint.
    RightOuter(JoinConstraint),
    /// `FULL OUTER JOIN` with an optional constraint.
    FullOuter(JoinConstraint),
    // TODO: Support CROSS Join
}

/// A single `JOIN` clause including relation and join operator/options.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Join {
    /// The joined table factor (table reference or derived table).
    relation: TableFactor,
    /// The join operator and its constraint (INNER/LEFT/RIGHT/CROSS/ASOF/etc.).
    join_operator: JoinOperator,
}

/// A table name or a parenthesized subquery with an optional alias
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableFactor {
    Table {
        /// Table name
        name: Ident,
        /// Optional alias for the table
        alias: Option<Ident>,
    },
}

/// A left table followed by zero or more joins.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableWithJoins {
    /// The starting table factor (left side) of the join chain.
    relation: TableFactor,
    // The sequence of joins applied to the relation.
    joins: Vec<Join>,
}

/// One item of the comma-separated list following `SELECT`
/// https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#select%20list
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SelectItem {
    /// Any expression, not followed by `[ AS ] alias`
    Expr(Expr),
    /// An expression, followed by `[ AS ] alias`
    NamedExpr {
        /// The expression being projected.
        expr: Expr,
        /// The alias for the expression.
        alias: Ident,
    },
    /// An unqualified `*`
    Wildcard(TokenWithSpan),
}

/// `SELECT` (without CTEs/`ORDER BY`), which may appear either as the
/// only body item of a `Query`, or as an operand to a set operation like
/// `UNION`
/// https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#query%20specification
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Select {
    /// Token for the `SELECT` keyword
    select_token: TokenWithSpan,
    // TODO: support DISTINCT
    /// Projection expressions
    projections: Vec<SelectItem>,
    /// FROM
    from: Vec<TableWithJoins>,
    /// WHERE
    selection: Option<Expr>,
    /// GROUP BY (<exprs>,...)
    group_by: Vec<Expr>,
}

/// The sort order for an `ORDER BY` expression.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OrderBySort {
    /// `ASC`
    Asc,
    /// `DESC`
    Desc,
}

/// An `ORDER BY` expression
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderByExpr {
    /// The expression to order by.
    expr: Expr,
    /// Ordering options such as `ASC`/`DESC`.
    sort: Option<OrderBySort>,
}

/// Represents the different syntactic forms of `LIMIT` clauses.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LimitClause {
    /// Standard SQL `LIMIT` syntax (optionally `BY` and `OFFSET`).
    ///
    /// `LIMIT <limit> [OFFSET <offset>]`
    LimitOffset {
        /// `LIMIT { <N> | ALL }` expression.
        limit: Option<Expr>,
        /// Optional `OFFSET` expression.
        offset: Option<Expr>,
    },
}

/// A variant of `SELECT` query expression, optionally including `WITH`,
/// `UNION` / other set operations, and `ORDER BY`.
/// https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#query%20expression
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Query {
    /// TODO: Support CTE (WITH ...)
    /// SELECT or UNION / EXCEPT / INTERSECT
    body: Box<SetExpr>,
    /// ORDER BY
    order_by: Option<Vec<OrderByExpr>>,
    /// `LIMIT ... OFFSET ... | LIMIT <offset>, <limit>`
    limit_clause: Option<LimitClause>,
}
