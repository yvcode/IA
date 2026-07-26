from savant.client import SinkBuilder



class ResultProvider:
    def __init__(self, savant_uri, source_id, jaeger_provider, callback):
        self.provider = (
            SinkBuilder()
            .with_socket(savant_uri)
            .with_source_id(source_id)
            .with_idle_timeout(800)
            .with_log_provider(jaeger_provider)
            .build()
        )
        self.callback = callback
    def provide_loop(self):
        for result in self.provider:
            self.callback(result)
