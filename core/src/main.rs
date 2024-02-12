mod logs;

use logs::create;

use std::env;

use actix_web::{ get, post, web::{self, Data}, App, HttpResponse, HttpServer, Responder};

use redis::{from_redis_value, streams::{StreamReadOptions, StreamReadReply}, AsyncCommands, FromRedisValue, Value};
use tokio;

#[derive(Debug)]
struct RV  {
    name: String
}

impl FromRedisValue for RV {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<RV> {
        match v {
            redis::Value::Data(data) => {
                let s = String::from_utf8_lossy(data);
                Ok(RV { name: s.to_string() })
            }
            redis::Value::Bulk(_data) => {
                Ok(RV { name: "test".to_string() })
            }
            _ => {
                Err(redis::RedisError::from((redis::ErrorKind::TypeError, "Not a string")))
            },
        }
    }
}

// Add a new item to the Redis stream
#[post("/add-to-redis/{name}")]
async fn add_to_redis(con: web::Data<redis::Client>, name: web::Path<String> ) -> impl Responder {
    let mut con = con.get_tokio_connection().await.expect("Failed to get Redis connection");
    con.xadd::<&str, &str, &str, &str, RV>("test", "*", &[("name", &name)])
        .await
        .expect("Bad xadd");
    HttpResponse::Created().body("Added to Redis stream")
}

#[get("/")]
async fn health() -> impl Responder {
    HttpResponse::Ok().body("OK")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let client = redis::Client::open("redis://yaroslav:aaaaaA1=@redis-17009.c311.eu-central-1-1.ec2.cloud.redislabs.com:17009").expect("Oh my");
    let tokio_redis_client = client.clone();

    // Spawn a new task to read from the Redis stream
    tokio::spawn(
        async move {

            let mut con = tokio_redis_client.get_tokio_connection().await.expect("Bad Redis connection");
            let options = StreamReadOptions::default().count(1).block(0);

            loop{
                let result: Option<StreamReadReply> = con.xread_options::<&str, &str, Option<StreamReadReply>>(&["test"], &["$"], &options).await.expect("Bad xread");
                if let Some(reply) = result {
                    for stream_key in reply.keys {
                        for stream_id in stream_key.ids {
                            let value = stream_id.map.get("name").unwrap();
                            match from_redis_value(value).expect("Bad from_redis_value") {
                                Value::Data(data) => {
                                    let s: String = String::from_utf8(data).expect("Bad utf8");
                                    create(&format!("{}", &stream_id.id), &s).unwrap();
                                }
                                _ => {
                                    println!("Not a data");
                                }
                            }
                        }
                    }

                }
            }
        }
    );

    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(client.clone())) // Share the cloned Redis client across handlers
            .service(add_to_redis)
            .service(health)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}