use core::{fmt, iter::Peekable};
use std::str::Chars;

use super::keywords::{Token, Whitespace};

/// An error reported by the tokenizer, with a human-readable `message` and a `location`.
#[derive(Debug, PartialEq, Eq)]
pub struct TokenizerError {
    /// A descriptive error message.
    pub message: String,
    /// The `Location` where the error was detected.
    pub location: Location,
}

impl fmt::Display for TokenizerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.message, self.location)
    }
}

impl core::error::Error for TokenizerError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Location {
    /// Line number, starting from 1.
    pub line: u32,
    /// Line column, starting from 1.
    pub column: u32,
}

impl Location {
    fn to_span(self, end: Self) -> Span {
        Span { start: self, end }
    }
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.line == 0 {
            return Ok(());
        }

        write!(f, "at Line: {}, Column: {}", self.line, self.column)
    }
}

/// A span represents a linear portion of the input string [start, end]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Span {
    /// Start `Location` (inclusive).
    pub start: Location,
    /// End `Location` (inclusive).
    pub end: Location,
}

/// A `Token` together with its `Span`, location in the source
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TokenWithSpan {
    pub token: Token,
    pub span: Span,
}

struct Cursor<'a> {
    line: u32,
    col: u32,
    iter: Peekable<Chars<'a>>,
}

impl<'a> Cursor<'a> {
    fn next(&mut self) -> Option<char> {
        match self.iter.next() {
            None => None,
            Some(s) => {
                if s == '\n' {
                    self.line += 1;
                    self.col = 1
                } else {
                    self.col += 1
                }

                Some(s)
            }
        }
    }

    fn peek(&mut self) -> Option<&char> {
        self.iter.peek()
    }

    fn loc(&self) -> Location {
        Location {
            line: self.line,
            column: self.col,
        }
    }
}

// learning: 'a is lifetime, Tokenizer borrows &str from other Any Tokenizer
// is created must not outlive the string slice it borrowed, and it borrows
// &str for some lifetime 'a
// In Rust, the compiler guarantees that references will never
// be dangling references: If you have a reference to some data, the compiler
// will ensure that the data will not go out of scope before the reference to
// the data does.
pub struct Tokenizer<'a> {
    query: &'a str,
}

fn is_identifier_start(ch: char) -> bool {
    ch.is_ascii_lowercase() || ch.is_ascii_uppercase()
}

fn is_identifier_part(ch: char) -> bool {
    ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_'
}

fn is_delimited_identifier_start(ch: char) -> bool {
    ch == '"' || ch == '`'
}

impl<'a> Tokenizer<'a> {
    pub fn new(query: &'a str) -> Self {
        Self { query }
    }

    fn tokenizer_error<R>(
        &self,
        loc: Location,
        msg: impl Into<String>,
    ) -> Result<R, TokenizerError> {
        Err(TokenizerError {
            message: msg.into(),
            location: loc,
        })
    }

    fn consume_and_next(
        &self,
        cursor: &mut Cursor,
        t: Token,
    ) -> Result<Option<Token>, TokenizerError> {
        cursor.next();
        Ok(Some(t))
    }

    /// peek_and_take_while peeks and takes the current cursor
    /// while the predicate still return True
    fn peek_and_take_while(
        &self,
        cursor: &mut Cursor,
        mut predicate: impl FnMut(char) -> bool,
    ) -> String {
        let mut s = String::new();
        while let Some(&ch) = cursor.peek() {
            if !predicate(ch) {
                break;
            }

            s.push(ch);
            cursor.next();
        }
        s
    }

    /// next_multi_line_comment tokenize multi-line comment, e.g /* .... */
    /// and returns Token::MultiLineComment
    fn next_multi_line_comment(
        &self,
        cursor: &mut Cursor,
    ) -> Result<Option<Token>, TokenizerError> {
        let mut s = String::new();
        loop {
            match cursor.next() {
                Some('*') if matches!(cursor.peek(), Some('/')) => {
                    cursor.next();
                    break Ok(Some(Token::Whitespace(Whitespace::MultiLineComment(s))));
                }
                Some(ch) => {
                    s.push(ch);
                }
                None => {
                    break self.tokenizer_error(
                        cursor.loc(),
                        "Unexpected EOF while in a multi-line comment",
                    );
                }
            }
        }
    }

    fn take_word(&self, cursor: &mut Cursor) -> String {
        self.peek_and_take_while(cursor, |ch: char| is_identifier_part(ch))
    }

    /// next get the next token or None
    fn next(&self, cursor: &mut Cursor) -> Result<Option<Token>, TokenizerError> {
        match cursor.next() {
            None => Ok(None),
            Some(ch) => match ch {
                // Whitespaces
                ' ' => self.consume_and_next(cursor, Token::Whitespace(Whitespace::Space)),
                '\t' => self.consume_and_next(cursor, Token::Whitespace(Whitespace::Tab)),
                '\n' => self.consume_and_next(cursor, Token::Whitespace(Whitespace::Newline)),
                ch if ch.is_whitespace() => {
                    self.consume_and_next(cursor, Token::Whitespace(Whitespace::Space))
                }
                // Punctuations
                '(' => self.consume_and_next(cursor, Token::LParen),
                ')' => self.consume_and_next(cursor, Token::RParen),
                '[' => self.consume_and_next(cursor, Token::LBracket),
                ']' => self.consume_and_next(cursor, Token::RBracket),
                ',' => self.consume_and_next(cursor, Token::Comma),
                // Operators
                '=' => {
                    cursor.next();
                    match cursor.peek() {
                        Some('=') => self.consume_and_next(cursor, Token::DoubleEq),
                        _ => Ok(Some(Token::Eq)),
                    }
                }
                '<' => {
                    cursor.next();
                    match cursor.peek() {
                        Some('>') => self.consume_and_next(cursor, Token::Neq),
                        Some('=') => self.consume_and_next(cursor, Token::LtEq),
                        _ => Ok(Some(Token::Lt)),
                    }
                }
                '>' => {
                    cursor.next();
                    match cursor.peek() {
                        Some('=') => self.consume_and_next(cursor, Token::GtEq),
                        _ => Ok(Some(Token::Gt)),
                    }
                }
                '+' => self.consume_and_next(cursor, Token::Plus),
                '-' => self.consume_and_next(cursor, Token::Minus),
                '*' => self.consume_and_next(cursor, Token::Mul),
                '/' => {
                    cursor.next();
                    match cursor.peek() {
                        Some('*') => {
                            // multi line comment
                            cursor.next();
                            self.next_multi_line_comment(cursor)
                        }
                        _ => Ok(Some(Token::Div)),
                    }
                }
                '%' => self.consume_and_next(cursor, Token::Mod),
                '|' => {
                    cursor.next();
                    if matches!(cursor.peek(), Some('|')) {
                        Ok(Some(Token::StringConcat))
                    } else {
                        self.tokenizer_error(cursor.loc(), "Unrecognised token")
                    }
                }
                // Single quoted string
                '\'' => {
                    cursor.next();
                    let is_not_single_quote = |ch: char| matches!(ch, '\'');

                    let s = self.peek_and_take_while(cursor, is_not_single_quote);
                    cursor.next();

                    Ok(Some(Token::SingleQuotedString(s)))
                }
                // Numbers
                '0'..='9' => {
                    // TODO: support heximal, decimal string literal
                    let mut s = self.peek_and_take_while(cursor, |ch: char| ch.is_ascii_digit());

                    if cursor.peek() == Some(&'.') {
                        s.push('.');
                        cursor.next();
                    }

                    // consume the fractional digits
                    s += &self.peek_and_take_while(cursor, |ch: char| ch.is_ascii_digit());

                    let is_long = if cursor.peek() == Some(&'L') {
                        cursor.next();
                        true
                    } else {
                        false
                    };

                    Ok(Some(Token::Number(s, is_long)))
                }
                // Identifiers or Keywords
                ch if is_identifier_start(ch) => {
                    let s = self.take_word(cursor);
                    Ok(Some(Token::make_word(&s, None)))
                }
                // Delimited (quoted) identifiers
                quote_start if is_delimited_identifier_start(quote_start) => {
                    cursor.next();
                    let s = self.take_word(cursor);
                    if matches!(cursor.peek(), Some(&_quote_start)) {
                        cursor.next();
                        Ok(Some(Token::make_word(&s, Some(quote_start))))
                    } else {
                        Err(TokenizerError {
                            message: format!(
                                "Expected close delimiter '{quote_start}' before EOF."
                            ),
                            location: cursor.loc(),
                        })
                    }
                }

                other => self.consume_and_next(cursor, Token::Char(other)),
            },
        }
    }

    pub fn tokenize(&self) -> Result<Vec<TokenWithSpan>, TokenizerError> {
        let mut tokens: Vec<TokenWithSpan> = vec![];
        let mut cursor = Cursor {
            line: 1,
            col: 1,
            iter: self.query.chars().peekable(),
        };

        let mut loc = cursor.loc();
        while let Some(token) = self.next(&mut cursor)? {
            let span = loc.to_span(cursor.loc());
            tokens.push(TokenWithSpan { token, span });
            loc = cursor.loc();
        }

        Ok(tokens)
    }
}
