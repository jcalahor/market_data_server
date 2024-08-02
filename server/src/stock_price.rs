use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct StockPrice {
    pub symbol: String,
    pub price: f64,
    pub change: f64
}
