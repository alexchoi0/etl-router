use clap::ValueEnum;

#[derive(Clone, ValueEnum, Default)]
pub enum OutputFormat {
    #[default]
    Table,
    Yaml,
    Json,
}
