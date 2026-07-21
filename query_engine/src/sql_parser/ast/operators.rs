use std::fmt::{Display, Formatter, Result};

/// Binary operators
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BinaryOperator {
    /// Plus, e.g. `a + b`
    Plus,
    /// Minus, e.g. `a - b`
    Minus,
    /// Multiply, e.g. `a * b`
    Multiply,
    /// Divide, e.g. `a / b`
    Divide,
    /// Modulo, e.g. `a % b`
    Modulo,
    /// String/Array Concat operator, e.g. `a || b`
    StringConcat,
    /// Greater than, e.g. `a > b`
    Gt,
    /// Less than, e.g. `a < b`
    Lt,
    /// Greater equal, e.g. `a >= b`
    GtEq,
    /// Less equal, e.g. `a <= b`
    LtEq,
    /// Equal, e.g. `a = b`
    Eq,
    /// Not equal, e.g. `a <> b`
    NotEq,
    /// And, e.g. `a AND b`
    And,
    /// Or, e.g. `a OR b`
    Or,
    /// XOR, e.g. `a XOR b`
    Xor,
    /// Bitwise or, e.g. `a | b`
    BitwiseOr,
    /// Bitwise and, e.g. `a & b`
    BitwiseAnd,
    /// Bitwise XOR, e.g. `a ^ b`
    BitwiseXor,
}

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter) -> Result {
        match self {
            BinaryOperator::Plus => f.write_str("+"),
            BinaryOperator::Minus => f.write_str("-"),
            BinaryOperator::Multiply => f.write_str("*"),
            BinaryOperator::Divide => f.write_str("/"),
            BinaryOperator::Modulo => f.write_str("%"),
            BinaryOperator::StringConcat => f.write_str("||"),
            BinaryOperator::Gt => f.write_str(">"),
            BinaryOperator::Lt => f.write_str("<"),
            BinaryOperator::GtEq => f.write_str(">="),
            BinaryOperator::LtEq => f.write_str("<="),
            BinaryOperator::Eq => f.write_str("="),
            BinaryOperator::NotEq => f.write_str("<>"),
            BinaryOperator::And => f.write_str("AND"),
            BinaryOperator::Or => f.write_str("OR"),
            BinaryOperator::Xor => f.write_str("XOR"),
            BinaryOperator::BitwiseOr => f.write_str("|"),
            BinaryOperator::BitwiseAnd => f.write_str("&"),
            BinaryOperator::BitwiseXor => f.write_str("^"),
        }
    }
}

/// Unary operators
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum UnaryOperator {
    /// Plus, e.g. `+9`
    Plus,
    /// Minus, e.g. `-9`
    Minus,
    /// Not, e.g. `NOT(true)`
    Not,
}

impl Display for UnaryOperator {
    fn fmt(&self, f: &mut Formatter) -> Result {
        f.write_str(match self {
            UnaryOperator::Minus => "-",
            UnaryOperator::Not => "NOT",
            UnaryOperator::Plus => "+",
        })
    }
}
