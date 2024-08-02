
use axum::{
    routing::get,
    Router,    
};
use tokio::task;
use std::{collections::HashMap, net::SocketAddr};
mod stock_price;
use stock_price::StockPrice;
use std::str;
use axum::{
    response::IntoResponse,
    Json,
};
use tower_http::cors::{CorsLayer, Any};
use rdkafka::{
    consumer::{BaseConsumer, Consumer, StreamConsumer},
    ClientConfig, ClientContext, Message,
};
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use std::sync::Arc;

async fn consume_kafka_messages(
    tx: mpsc::Sender<StockPrice>,
    kafka_brokers: &str,
    kafka_topic: &str,
) {
    let consumer: BaseConsumer = ClientConfig::new()
    .set("bootstrap.servers", kafka_brokers)
    .set("group.id", "andean-group")
    .create()
    .expect("invalid consumer config");


    if let Err(e) = consumer.subscribe(&[kafka_topic]) {
        println!("Failed to subscribe to topic {}: {:?}", kafka_topic, e);
        return;
    }

    println!("Listening");
    loop {
        println!("consuming...");
        for msg_result in consumer.iter() {
                println!("data");
                let msg = msg_result.unwrap();
                //let key: &str = msg.key_view().unwrap().unwrap();
                println!("data");
                let value = msg.payload().unwrap();
                let buff = str::from_utf8(&value).unwrap();
                    
                let stock_price: StockPrice = serde_json::from_slice(&value).expect("failed to deser JSON to Tick");
                println!(
                    "{:?} value {:?} in offset {:?} from partition {}",
                     stock_price,
                     buff,
                     msg.offset(),
                     msg.partition()
                );
                    
                if let Err(_) = tx.send(stock_price).await {
                    println!("receiver dropped when sending for insertion {:?}", buff);                         
                }
            }
        }
}



async fn get_stock_prices() -> impl IntoResponse {
    let stock_prices = vec![
        StockPrice {
            symbol: "AAPL".to_string(),
            price: 180.57,
            change: 0.11,
        },
        StockPrice {
            symbol: "GOOGL".to_string(),
            price: 2801.12,
            change: 0.12
        },
        StockPrice {
            symbol: "MSFT".to_string(),
            price: 345.67,
            change: 0.22
        },
    ];
    println!("requested data");
    Json(stock_prices)
}

type StockPrices = Arc<RwLock<HashMap<String, StockPrice>>>;

#[tokio::main]
async fn main() {
    let stock_prices: StockPrices = Arc::new(RwLock::new(HashMap::new()));
    let (tx, mut rx) = mpsc::channel(100);

    let kafka_brokers = "kafka:9092";
    let kafka_topic = "stock_prices";
    
    tokio::spawn(async move {
        consume_kafka_messages(tx, kafka_brokers, kafka_topic).await;
    });

    let stock_prices_clone: Arc<RwLock<HashMap<String, StockPrice>>> = stock_prices.clone();
    tokio::spawn(async move {
        while let Some(price) = rx.recv().await {
            //let mut prices = stock_prices_clone.write().await;
            //prices.push(price);
            println!("Received and ready to process{:?}", price)
       }
    });

    let cors = CorsLayer::new()
      .allow_origin(Any) // Allow requests from any origin
      .allow_methods(Any) // Allow any method
      .allow_headers(Any); // Allow any header

    let app = Router::new().route("/stock-prices", get(get_stock_prices))
       .layer(cors);

     let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    println!("Listening on {}", addr);
     axum::Server::bind(&addr)
       .serve(app.into_make_service())
        .await
       .unwrap();
}