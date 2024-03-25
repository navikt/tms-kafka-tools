package no.nav.tms.kafka.application

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Counter
import io.prometheus.client.Histogram

object Metrics {
    private val registry = CollectorRegistry.defaultRegistry

    val onMessageHistorgram = Histogram.build()
        .name("on_kafka_message_seconds")
        .help("Hvor lang det tar for subscriber å prosessere melding i sekunder")
        .labelNames("subscriber", "event_name", "result")
        .register(registry)

    val onMessageCounter = Counter.build()
        .name("kafka_message_counter")
        .help("Hvor mange meldinger som er akseptert eller ignorert")
        .labelNames("subscriber", "event_name", "result")
        .register(registry)
}
