pub mod common {
    tonic::include_proto!("conveyor.common");
}

pub mod source {
    tonic::include_proto!("conveyor.source");
}

pub mod transform {
    tonic::include_proto!("conveyor.transform");
}

pub mod lookup {
    tonic::include_proto!("conveyor.lookup");
}

pub mod sink {
    tonic::include_proto!("conveyor.sink");

    impl WriteOptions {
        pub fn at_least_once() -> Self {
            Self {
                require_ack: true,
                timeout_ms: 30000,
                guarantee: DeliveryGuarantee::AtLeastOnce as i32,
            }
        }

        pub fn exactly_once() -> Self {
            Self {
                require_ack: true,
                timeout_ms: 60000,
                guarantee: DeliveryGuarantee::ExactlyOnce as i32,
            }
        }
    }
}

pub mod registry {
    tonic::include_proto!("conveyor.registry");
}

pub mod checkpoint {
    tonic::include_proto!("conveyor.checkpoint");
}

pub mod router {
    tonic::include_proto!("conveyor.router");
}

pub mod raft {
    tonic::include_proto!("raft");
}

pub mod backup {
    tonic::include_proto!("conveyor.backup");
}

pub mod sidecar {
    tonic::include_proto!("conveyor.sidecar");
}
