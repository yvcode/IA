from savant.client import SourceBuilder


class SavantPublisher:
    def __init__(self, savant_uri, jaeger_provider):
        self.savant_source =  (
            SourceBuilder()
            .with_log_provider(jaeger_provider)
            .with_socket(savant_uri)
            .build()
        )

    def publish(self, frame_source):
        self.savant_source(frame_source, send_eos=False)
