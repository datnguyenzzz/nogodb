use crate::sql_parser::ast::expr::{Expr, Ident, SetExpr};

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
    /// `RIGHT JOIN` with an optional constraint.
    Right(JoinConstraint),
    /// `FULL OUTER JOIN` with an optional constraint.
    FullOuter(JoinConstraint),
    // TODO: Support CROSS Join
}

/// A single `JOIN` clause including relation and join operator/options.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Join {
    /// The joined table factor (table reference or derived table).
    pub relation: TableFactor,
    /// The join operator and its constraint (INNER/LEFT/RIGHT/CROSS/ASOF/etc.).
    pub join_operator: JoinOperator,
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
    pub relation: TableFactor,
    // The sequence of joins applied to the relation.
    pub joins: Option<Vec<Join>>,
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
    /// An `*`
    Wildcard,
}

/// `SELECT` (without CTEs/`ORDER BY`), which may appear either as the
/// only body item of a `Query`, or as an operand to a set operation like
/// `UNION`
/// https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#query%20specification
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Select {
    // TODO: support DISTINCT
    /// Projection expressions
    pub projections: Vec<SelectItem>,
    /// FROM
    pub from: TableWithJoins,
    /// WHERE
    pub selection: Option<Expr>,
    /// GROUP BY (<exprs>,...)
    pub group_by: Option<Vec<Expr>>,
    // TODO: Having
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
    pub expr: Expr,
    /// Ordering options such as `ASC`/`DESC`.
    pub sort: Option<OrderBySort>,
}

/// A variant of `SELECT` query expression, optionally including `WITH`,
/// `UNION` / other set operations, and `ORDER BY`.
/// https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#query%20expression
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Query {
    /// TODO: Support CTE (WITH ...)
    /// SELECT or UNION / EXCEPT / INTERSECT
    pub body: Box<SetExpr>,
    /// ORDER BY
    pub order_by: Option<Vec<OrderByExpr>>,
    /// LIMIT <number>
    pub limit: Option<usize>,
    /// OFFSET <number>
    pub offset: Option<usize>,
}
