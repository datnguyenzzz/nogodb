/// Timestamp and Time data types information about TimeZone formatting.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TimezoneInfo {
    /// No information about time zone, e.g. TIMESTAMP
    None,
    /// Temporal type 'WITH TIME ZONE', e.g. TIMESTAMP WITH TIME ZONE
    WithTimeZone,
    /// Temporal type 'WITHOUT TIME ZONE', e.g. TIME WITHOUT TIME ZONE
    WithoutTimeZone,
}

/// Additional information for `NUMERIC`, `DECIMAL`, and `DEC` data types
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExactNumberInfo {
    /// No additional information, e.g. `DECIMAL`.
    None,
    /// Only precision information, e.g. `DECIMAL(10)`.
    Precision(u64),
    /// Precision and scale information, e.g. `DECIMAL(10,2)`.
    PrecisionAndScale(u64, i64),
}

/// SQL data types
/// https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#data%20type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    /// Fixed-length char type, e.g. CHAR(10).
    Char(Option<u64>),
    /// Variable-length character type, e.g. VARCHAR(10).
    Varchar(Option<u64>),
    /// Uuid type.
    Uuid,
    /// Floating point with optional precision and scale, e.g. FLOAT, FLOAT(8), or FLOAT(8,2).
    Float(ExactNumberInfo),
    /// Int with optional display width, e.g. INT or INT(11).
    Int(Option<u64>),
    /// Big integer with optional display width, e.g. BIGINT or BIGINT(20).
    BigInt(Option<u64>),
    /// Integer with optional display width, e.g. INTEGER or INTEGER(11).
    Integer(Option<u64>),
    /// Double
    Double(ExactNumberInfo),
    /// Double Precision
    DoublePrecision,
    /// unsigned double precision
    DoublePrecisionUnsigned,
    /// Boolean type.
    Boolean,
    /// Date type.
    Date,
    /// Time with optional time precision and time zone information,
    Time(Option<u64>, TimezoneInfo),
    /// Timestamp with optional time precision and time zone information
    Timestamp(Option<u64>, TimezoneInfo),
    // TODO: Support JSON, JSONB
    /// String with optional length.
    String(Option<u64>),
}
