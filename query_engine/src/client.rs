use crate::sql_parser::Parser;

pub struct Client {}

impl Client {
    pub fn init() -> Self {
        Self {}
    }

    /// TODO: update to execute(ENUM request). We'd like to support several request type
    /// not just SQL query execution
    pub fn execute(&self, statement: &str) {
        let _ = Parser::default().parse_sql(statement);
    }
}
