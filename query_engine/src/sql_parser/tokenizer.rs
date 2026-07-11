use super::keywords::Keyword;

/// A keyword (like SELECT) or an optionally quoted SQL identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Word {
    /// If the word matched one of the known keywords, this will have one of 
    /// the values from keywords::Keyword, otherwise empty
    pub keyword: Keyword,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Whitespace {
    /// A single space character.
    Space,
    /// A newline character.
    Newline,
    /// A tab character.
    Tab,
    /// A single-line comment (e.g. `-- comment` or `# comment`).
    /// The `comment` field contains the text, and `prefix` contains the comment prefix.
    SingleLineComment {
        /// The content of the comment (without the prefix).
        comment: String,
        /// The prefix used for the comment (for example `--` or `#`).
        prefix: String,
    },

    /// A multi-line comment (without the `/* ... */` delimiters).
    MultiLineComment(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Token {
    /// An end-of-file marker, not a real token
    EOF,
    /// A keyword (like SELECT) or an optionally quoted SQL identifier
    Word(Word),
    /// An unsigned numeric literal
    Number(String, bool),
    /// A character that could not be tokenized
    Char(char),
    /// Single quoted string: i.e: 'string'
    SingleQuotedString(String),
    /// Double quoted string: i.e: "string"
    DoubleQuotedString(String),
    /// Comma
    Comma,
    /// Whitespace (space, tab, etc)
    Whitespace(Whitespace),
    /// Double equals sign `==`
    DoubleEq,
    /// Equality operator `=`
    Eq,
    /// Not Equals operator `<>` (or `!=` in some dialects)
    Neq,
    /// Less Than operator `<`
    Lt,
    /// Greater Than operator `>`
    Gt,
    /// Less Than Or Equals operator `<=`
    LtEq,
    /// Greater Than Or Equals operator `>=`
    GtEq,
    /// Spaceship operator <=>
    Spaceship,
    /// Plus operator `+`
    Plus,
    /// Minus operator `-`
    Minus,
    /// Multiplication operator `*`
    Mul,
    /// Division operator `/`
    Div,
    /// Integer division operator `//` in DuckDB
    DuckIntDiv,
    /// Modulo Operator `%`
    Mod,
    /// String concatenation `||`
    StringConcat,
    /// Left parenthesis `(`
    LParen,
    /// Right parenthesis `)`
    RParen,
    /// Period (used for compound identifiers or projections into nested types)
    Period,
    /// Colon `:`
    Colon,
    /// DoubleColon `::` (used for casting in PostgreSQL)
    DoubleColon,
    /// Assignment `:=` (used for keyword argument in DuckDB macros and some functions, and for variable declarations in DuckDB and Snowflake)
    Assignment,
    /// SemiColon `;` used as separator for COPY and payload
    SemiColon,
    /// Left bracket `[`
    LBracket,
    /// Right bracket `]`
    RBracket,
    /// Left brace `{`
    LBrace,
    /// Right brace `}`
    RBrace,
    /// Right Arrow `=>`
    RArrow,
}

/// learning: 'a is lifetime, Tokenizer borrows &str from other
/// Any Tokenizer is created must not outlive the string slice 
/// it borrowed, and it borrows &str for some lifetime 'a
pub struct Tokenizer<'a> {
    query: &'a str, 
}

impl <'a> Tokenizer <'a>{
    pub fn new(query: &'a str) -> Self {
        Self { query }
    }

    pub fn tokenize(&self) {}
}