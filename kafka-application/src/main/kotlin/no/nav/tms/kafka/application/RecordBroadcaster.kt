package no.nav.tms.kafka.application

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Counter
import io.prometheus.client.Histogram
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import kotlin.time.DurationUnit
import kotlin.time.measureTimedValue

internal class RecordBroadcaster(
    private val subscribers: List<Subscriber>
) {
    suspend fun broadcastRecord(record: ConsumerRecord<String, String>) {
        val jsonMessage = JsonMessage.fromRecord(record)

        subscribers.forEach { subscriber ->
            measureTimedValue {
                subscriber.onMessage(jsonMessage)
            }.let { (result, duration) ->
                Metrics.onMessageCounter.labels(subscriber.name(), jsonMessage.eventName, result.toString())
                    .inc()

                Metrics.onMessageHistorgram.labels(subscriber.name(), jsonMessage.eventName, result.toString())
                    .observe(duration.toDouble(DurationUnit.SECONDS))
            }
        }
    }
}

// For use in tests in dependent projects
class MessageBroadcaster(
    private val subscribers: List<Subscriber>
) {
    fun broadcastRecord(record: ConsumerRecord<String, String>) {
        broadcastMessage(JsonMessage.fromRecord(record))
    }

    fun broadcastJson(jsonString: String, metadata: EventMetadata? = null) {
        broadcastMessage(JsonMessage.fromJson(jsonString, metadata))
    }

    private fun broadcastMessage(jsonMessage: JsonMessage) {
        subscribers.forEach { subscriber ->
            runBlocking {
                subscriber.onMessage(jsonMessage)
            }
        }
    }
}

class MessageException(message: String): IllegalArgumentException(message)

private object Metrics {
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
