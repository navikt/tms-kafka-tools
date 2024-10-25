package no.nav.tms.kafka.application

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.server.application.*
import io.ktor.util.*
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Counter
import io.prometheus.client.Histogram
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import kotlin.time.DurationUnit
import kotlin.time.measureTimedValue

class RecordBroadcaster internal constructor(
    private val subscribers: List<Subscriber>,
    eventNameFields: List<String>
) {
    private val log = KotlinLogging.logger {}
    private val secureLog = KotlinLogging.logger("secureLog")

    private val messageBuilder = JsonMessageBuilder(eventNameFields)

    suspend fun broadcastRecord(record: ConsumerRecord<String, String>) {
        try {
            messageBuilder.fromRecord(record).let {
                broadcastMessage(it)
                Metrics.onValidEventCounter.labels(record.topic(), it.eventName).inc()
            }
        } catch (e: JsonException) {
            Metrics.onInvalidEventCounter.labels(record.topic(), "invalid_json").inc()
            log.warn { "ignoring record with offset [${record.offset()}] in partition [${record.partition()}] because value is not valid json" }
            secureLog.warn(e) { "ignoring record with offset [${record.offset()}] in partition [${record.partition()}] because value is not valid json" }
        } catch (e: MessageFormatException) {
            Metrics.onInvalidEventCounter.labels(record.topic(), "missing_name").inc()
            log.warn { "ignoring record with offset [${record.offset()}] in partition [${record.partition()}] because it does not contain field '@event_name' or its alternative" }
            secureLog.warn(e) { "ignoring record with offset [${record.offset()}] in partition [${record.partition()}] because it does not contain field '@event_name' or its alternative" }
        }  catch (nullpointer: NullPointerException){
            Metrics.onInvalidEventCounter.labels(record.topic(), "nullpointer").inc()
            log.warn { "ignoring record with offset [${record.offset()}] in partition [${record.partition()}] because a value is null'" }
            secureLog.warn(nullpointer) { "ignoring record with offset [${record.offset()}] in partition [${record.partition()}] because a value is null'" }
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
    private val subscribers: List<Subscriber>,
    eventNameFields: List<String> = listOf(JsonMessage.DEFAULT_EVENT_NAME)
) {
    constructor(vararg subscriber: Subscriber, eventNameFields: List<String> = listOf(JsonMessage.DEFAULT_EVENT_NAME)) : this(subscriber.toList(), eventNameFields)

    private val messageBuilder: JsonMessageBuilder = JsonMessageBuilder(eventNameFields)

    fun broadcastRecord(record: ConsumerRecord<String, String>) {
        broadcastMessage(messageBuilder.fromRecord(record))
    }

    fun broadcastJson(jsonString: String, metadata: EventMetadata? = null) {
        broadcastMessage(messageBuilder.fromJson(jsonString, metadata))
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
