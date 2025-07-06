use log::debug;
use tackboardlib::connection_manager::{ConnectionManager, Connects};

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();

    //  Start some random topics
    

    let mut connection_manager = ConnectionManager::create_listener("127.0.0.1:5621".to_string()).await.unwrap();

 
    tokio::spawn(async move {
        // Process jobs
        loop {
         
        }
    }); 

    loop {
    
        connection_manager.accept_connection(move |x| {
          
            async move {
               
            }}).await;

    }
}