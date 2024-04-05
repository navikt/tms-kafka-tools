package no.nav.tms.kafka.application

import io.github.oshai.kotlinlogging.KotlinLogging
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
    private val log = KotlinLogging.logger {}
    private val secureLog = KotlinLogging.logger("secureLog")

    suspend fun broadcastRecord(record: ConsumerRecord<String, String>) {
        try {
            JsonMessage.fromRecord(record).let {
                broadcastMessage(it)
                Metrics.onValidEventCounter.labels(record.topic(), it.eventName)
            }
        } catch (e: JsonException) {
            Metrics.onInvalidEventCounter.labels(record.topic(), "invalid_json")
            log.warn { "ignoring record with offset ${record.offset()} in partition ${record.partition()} because value is not valid json" }
            secureLog.warn(e) { "ignoring record with offset ${record.offset()} in partition ${record.partition()} because value is not valid json" }
        } catch (e: MessageFormatException) {
            Metrics.onValidEventCounter.labels(record.topic(), "missing_name")
            log.warn { "ignoring record with offset ${record.offset()} in partition ${record.partition()} because it does not contain field '@event_name'" }
            secureLog.warn(e) { "ignoring record with offset ${record.offset()} in partition ${record.partition()} because it does not contain field '@event_name'" }
        }
    }

    private suspend fun broadcastMessage(jsonMessage: JsonMessage) {
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
        .name("kafka_message_processing_time_s")
        .help("Hvor lang det tar for en subscriber å prosessere melding i sekunder")
        .labelNames("subscriber", "event_name", "result")
        .register(registry)

    val onMessageCounter = Counter.build()
        .name("kafka_message_counter")
        .help("Hvor mange ganger en subscriber har akseptert eller ignorert en melding.")
        .labelNames("subscriber", "event_name", "result")
        .register(registry)

    val onValidEventCounter = Counter.build()
        .name("kafka_valid_event_counter")
        .help("Hvor mange gyldige eventer som er lest fra kafka")
        .labelNames("topic", "event_name")
        .register(registry)

    val onInvalidEventCounter = Counter.build()
        .name("kafka_invalid_event_counter")
        .help("Hvor mange ugyldige eventer som er lest fra kafka")
        .labelNames("topic", "reason")
        .register(registry)
}
