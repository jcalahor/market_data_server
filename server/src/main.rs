
use axum::{
    routing::get,
    Router,    
    extract::State,
    extract::ws::{WebSocket, WebSocketUpgrade},
};
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
    consumer::{BaseConsumer, Consumer},
    ClientConfig, Message
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
                let msg = msg_result.unwrap();
                //let key: &str = msg.key_view().unwrap().unwrap();
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

type StockPrices = Arc<RwLock<HashMap<String, StockPrice>>>;

async fn handler(ws: WebSocketUpgrade, state: State<StockPrices>) -> impl IntoResponse  {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: State<StockPrices>) {
    while let Some(msg) = socket.recv().await {
        let msg = if let Ok(msg) = msg {
            msg
        } else {
            // client disconnected
            return;
        };

        if socket.send(msg).await.is_err() {
            // client disconnected
            return;
        }
    }
}

async fn get_stock_prices(state: State<StockPrices>) -> Json<Vec<StockPrice>> {
    let prices: HashMap<String, StockPrice> = state.read().await.clone();

    let price_list = prices.into_iter().map(|(symbol, inner_map)| {
        // Assume 'price' is always present in the inner_map
        let price = inner_map.price;
        let change =inner_map.change;
        StockPrice { symbol, price, change }
    }).collect();

    Json(price_list)
    
}


#[tokio::main]
async fn main() {
    let stock_prices: StockPrices = Arc::new(RwLock::new(HashMap::new()));
    let (tx, mut rx) = mpsc::channel(1000);

    let kafka_brokers = "kafka:9092";
    let kafka_topic = "stock_prices";
    
    tokio::spawn(async move {
        consume_kafka_messages(tx, kafka_brokers, kafka_topic).await;
    });

    let stock_prices_clone: Arc<RwLock<HashMap<String, StockPrice>>> = stock_prices.clone();
    tokio::spawn(async move {
        while let Some(stock_price) = rx.recv().await {
            let mut stock_prices = stock_prices_clone.write().await;
            println! ("updated");
            stock_prices.insert(stock_price.symbol.clone(), StockPrice {
                symbol: stock_price.symbol.clone(),
                price: stock_price.price,
                change: stock_price.change
            });
            println!("{:?}", stock_prices);
       }
    });

    let cors = CorsLayer::new()
      .allow_origin(Any) // Allow requests from any origin
      .allow_methods(Any) // Allow any method
      .allow_headers(Any); // Allow any header

    let app = Router::new().route("/stock-prices", get(get_stock_prices))
       .route("/ws", get(handler))
       .with_state(stock_prices.clone())
       .layer(cors);

     let addr = SocketAddr::from(([0, 0, 0, 0], 3000));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Listening on {}", addr);
    axum::serve(listener, app).await.unwrap();
}