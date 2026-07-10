use clap::Parser;

#[derive(Parser, Debug)]
#[command(name="nogodb cli", version, about)]
struct Args {
    #[arg()]
    statement: String,

    #[arg(short = 'H', long, default_value="0.0.0.0")]
    host: String,

    #[arg(short = 'P', long, default_value="9601")]
    port: u16, 
}

impl Args {
    fn run(&self) {
        println!("Query to be run: {}", self.statement)
    }
}

fn main() {
    let args = Args::parse();
    args.run();
}

