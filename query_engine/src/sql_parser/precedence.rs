/// Operators for which precedence must be defined.
///
/// Higher number -> higher precedence.
/// Use https://www.postgresql.org/docs/7.0/operators.htm#AEN2026 as a reference
#[derive(Debug, Clone, Copy)]
pub enum Precedence {
    /// Member access operator `.` (highest precedence).
    Period,
    /// Type cast `::`.
    DoubleColon,
    /// Multiplication / Division / Modulo operators (`*`, `/`, `%`).
    MulDivMod,
    /// Addition / Subtraction (`+`, `-`).
    PlusMinus,
    /// Bitwise `XOR` operator (`^`).
    Xor,
    /// Bitwise `AND` operator (`&`).
    Ampersand,
    /// Bitwise `OR` / pipe operator (`|`).
    Pipe,
    /// `BETWEEN` operator.
    Between,
    /// Equality operator (`=`).
    Eq,
    /// Pattern matching (`LIKE`).
    Like,
    /// `IS` operator (e.g. `IS NULL`).
    Is,
    /// Unary `NOT`.
    UnaryNot,
    /// Logical `AND`.
    And,
    /// Logical `OR` (lowest precedence).
    Or,
}

pub fn prec_value(prec: Precedence) -> u8 {
    match prec {
        Precedence::Period => 100,
        Precedence::DoubleColon => 50,
        Precedence::MulDivMod => 40,
        Precedence::PlusMinus => 30,
        Precedence::Xor => 24,
        Precedence::Ampersand => 23,
        Precedence::Pipe => 21,
        Precedence::Between => 20,
        Precedence::Eq => 20,
        Precedence::Like => 19,
        Precedence::Is => 17,
        Precedence::UnaryNot => 15,
        Precedence::And => 10,
        Precedence::Or => 5,
    }
}

pub fn prec_unknown() -> u8 {
    0
}
