package no.nav.tms.kafka.reader

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Counter
import io.prometheus.client.Histogram

object Metrics {
    private val registry = CollectorRegistry.defaultRegistry

    val onMessageHistorgram = Histogram.build()
        .name("on_kafka_message_seconds")
        .help("Hvor lang det tar for subscriber å prosessere melding i sekunder")
        .labelNames("subscriber", "event_name")
        .register(registry)

    val onMessageCounter = Counter.build()
        .name("kafka_message_counter")
        .help("Hvor mange meldinger som er lest inn")
        .labelNames("rapid", "river", "validated", "event_name")
        .register(registry)
}
