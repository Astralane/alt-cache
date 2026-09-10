pub mod api;
pub mod config;
pub mod feed;
pub mod logging;
pub mod store;

pub mod proto {
    tonic::include_proto!("astralane.alt.v1");
}
