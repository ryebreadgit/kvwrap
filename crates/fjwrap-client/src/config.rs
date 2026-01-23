use std::time::Duration;

#[derive(Clone, Debug)]
pub struct RemoteConfig {
    pub endpoint: String,
    pub connect_lazy: bool,
    pub connect_timeout: Option<Duration>,
    pub request_timeout: Option<Duration>,
}

impl RemoteConfig {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            connect_lazy: false,
            connect_timeout: Some(Duration::from_secs(5)),
            request_timeout: Some(Duration::from_secs(30)),
        }
    }

    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    pub fn with_connect_lazy(mut self, connect_lazy: bool) -> Self {
        self.connect_lazy = connect_lazy;
        self
    }
}
