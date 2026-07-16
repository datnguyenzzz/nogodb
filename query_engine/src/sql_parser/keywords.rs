use core::fmt;

/// https://en.wikipedia.org/wiki/List_of_SQL_reserved_words
macro_rules! define_keywords {
    // learning:
    //   < $(...),* >: repeat the pattern inside zero or more times, separated by commas.
    ($(
        $keyword:ident
    ),*) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[allow(non_camel_case_types)]
        /// An enumeration of SQL keywords recognized by the parser.
        pub enum Keyword{
            NoKeyWord,
            $(
                #[doc = concat!("The `", stringify!($keyword), "` SQL keyword.")]
                $keyword
            ),*
        }

        impl Keyword {
            /// Array of all keywords as string constants.
            const ALL: &'static [&str] = &[
                $(stringify!($keyword)),*
            ];
            /// Array of all enum values in declaration order.
            const ALL_KEYWORDS: &'static [Keyword] = &[
                $(Keyword::$keyword),*
            ];
        }
    };
}

define_keywords!(
    ABS, ALTER, ANALYZE, AND, AS, ASC, BETWEEN, BIGDECIMAL, BIGINT, BOOLEAN, CASE, CAST, CHAR,
    COALESCE, COLLATE, COUNT, CREATE, DATE, DEFAULT, DOUBLE, DROP, ELSE, ELSEIF, EMPTY, FLOAT,
    FLOAT32, FLOAT4, FLOAT64, FLOAT8, FLOOR, GROUP, GROUPING, HASH, HASHES, HAVING, ID, IF, INNER,
    INSERT, INT, INT128, INT16, INT2, INT256, INT32, INT4, INT64, INT8, INTEGER, INTERSECT, JOIN,
    LEFT, LIKE, LIMIT, MOD, NOT, NOTNULL, OFFSET, OUTER, QUERY, REGEXP, UINT128, UINT16, UINT256,
    UINT32, UINT64, UINT8, VARCHAR, WHERE
);

/// A keyword (like SELECT) or an optionally quoted SQL identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Word {
    /// If the word matched one of the known keywords, this will have one of
    /// the values from keywords::Keyword, otherwise empty
    pub keyword: Keyword,
    pub value: String,
    pub quote: Option<char>,
}

impl fmt::Display for Word {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.quote {
            Some(s) if s == '"' || s == '`' => {
                write!(f, "{}{}{}", s, self.value, s)
            }
            None => f.write_str(&self.value),
            _ => panic!("Unexpected quote_style!"),
        }
    }
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

impl fmt::Display for Whitespace {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Whitespace::Space => f.write_str(" "),
            Whitespace::Newline => f.write_str("\n"),
            Whitespace::Tab => f.write_str("\t"),
            Whitespace::SingleLineComment { prefix, comment } => writeln!(f, "{prefix}{comment}"),
            Whitespace::MultiLineComment(s) => write!(f, "/*{s}*/"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Token {
    /// An end-of-file marker, not a real token
    EOF,
    /// A keyword (like SELECT) or an optionally quoted SQL identifier
    Word(Word),
    /// An unsigned numeric literal. `bool` indicates a number if Long
    /// or not. Such as 123456789L
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
    /// Not Equals operator `<>`
    Neq,
    /// Less Than operator `<`
    Lt,
    /// Greater Than operator `>`
    Gt,
    /// Less Than Or Equals operator `<=`
    LtEq,
    /// Greater Than Or Equals operator `>=`
    GtEq,
    /// Plus operator `+`
    Plus,
    /// Minus operator `-`
    Minus,
    /// Multiplication operator `*`
    Mul,
    /// Division operator `/`
    Div,
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
    /// Left bracket `[`
    LBracket,
    /// Right bracket `]`
    RBracket,
    /// Left brace `{`
    LBrace,
    /// Right brace `}`
    RBrace,
    /// SemiColon `;` used as separator for COPY and payload
    SemiColon,
}

impl Token {
    pub fn make_word(word: &str, quote: Option<char>) -> Self {
        Token::Word(Word {
            keyword: search_keyword(word),
            value: word.to_string(),
            quote,
        })
    }
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::EOF => f.write_str("EOF"),
            Token::Word(w) => write!(f, "{w}"),
            Token::Number(n, l) => write!(f, "{}{long}", n, long = if *l { "L" } else { "" }),
            Token::Char(c) => write!(f, "{c}"),
            Token::SingleQuotedString(s) => write!(f, "'{s}'"),
            Token::DoubleQuotedString(s) => write!(f, "\"{s}\""),
            Token::Comma => f.write_str(","),
            Token::Whitespace(ws) => write!(f, "{ws}"),
            Token::DoubleEq => f.write_str("=="),
            Token::Eq => f.write_str("="),
            Token::Neq => f.write_str("<>"),
            Token::Lt => f.write_str("<"),
            Token::Gt => f.write_str(">"),
            Token::LtEq => f.write_str("<="),
            Token::GtEq => f.write_str(">="),
            Token::Plus => f.write_str("+"),
            Token::Minus => f.write_str("-"),
            Token::Mul => f.write_str("*"),
            Token::Div => f.write_str("/"),
            Token::StringConcat => f.write_str("||"),
            Token::Mod => f.write_str("%"),
            Token::LParen => f.write_str("("),
            Token::RParen => f.write_str(")"),
            Token::Period => f.write_str("."),
            Token::Colon => f.write_str(":"),
            Token::LBracket => f.write_str("["),
            Token::RBracket => f.write_str("]"),
            Token::LBrace => f.write_str("{"),
            Token::RBrace => f.write_str("}"),
            Token::SemiColon => f.write_str(";"),
        }
    }
}

/// Case-insensitive keyword lookup using binary search over [`ALL_KEYWORDS`].
pub fn search_keyword(word: &str) -> Keyword {
    Keyword::ALL
        .binary_search_by(|probe| {
            let probe = probe.as_bytes();
            let word = word.as_bytes();
            for (p, w) in probe.iter().zip(word.iter()) {
                let cmp = p.cmp(&w.to_ascii_uppercase());
                if cmp != core::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            probe.len().cmp(&word.len())
        })
        .map_or_else(|_| Keyword::NoKeyWord, |i| Keyword::ALL_KEYWORDS[i])
}
