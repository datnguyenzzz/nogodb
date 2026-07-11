// Learning: use crate::... in main.rs looks at the binary, not the library.
//  Use the library's name (query_engine::...) to reach into it.
use query_engine::Client; 
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name="nogodb cli", version, about)]
pub struct Args {
    #[arg()]
    statement: String,

    #[arg(short = 'H', long, default_value="0.0.0.0")]
    host: String,

    #[arg(short = 'P', long, default_value="9601")]
    port: u16, 
}

impl Args {
    fn run(&self, client: &Client) {
        // Learning: execute(self.statement) will fail to compile here
        // `String` in Rust is heap allocated. &self has an ownership 
        // of `statement`. When we are doing ...(self.statement), it 
        // means we are trying to take ownership from &self which is 
        // prohibited. There are 2 options:
        //. 1. Use the reference &str
        //. 2. Clone self.statement.clone()
        client.execute(&self.statement);
    }
}


fn main() {
    env_logger::init();
    let args = Args::parse();
    let client = Client::init();
    args.run(&client);
}

