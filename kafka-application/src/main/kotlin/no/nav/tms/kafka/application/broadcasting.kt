package no.nav.tms.kafka.application

import io.github.oshai.kotlinlogging.KotlinLogging
import io.prometheus.metrics.core.metrics.Counter
import io.prometheus.metrics.core.metrics.Histogram
import io.prometheus.metrics.model.registry.PrometheusRegistry
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import kotlin.time.DurationUnit
import kotlin.time.measureTimedValue

open class MessageException(message: String): IllegalArgumentException(message)

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
                Metrics.onValidEventCounter.labelValues(record.topic(), it.eventName).inc()
            }
        } catch (e: JsonException) {
            Metrics.onInvalidEventCounter.labelValues(record.topic(), "invalid_json").inc()
            log.warn { "ignoring record with offset [${record.offset()}] in partition [${record.partition()}] because value is not valid json" }
            secureLog.warn(e) { "ignoring record with offset [${record.offset()}] in partition [${record.partition()}] because value is not valid json" }
        } catch (e: MessageFormatException) {
            Metrics.onInvalidEventCounter.labelValues(record.topic(), "missing_name").inc()
            log.warn { "ignoring record with offset [${record.offset()}] in partition [${record.partition()}] because it does not contain field '@event_name' or its alternative" }
            secureLog.warn(e) { "ignoring record with offset [${record.offset()}] in partition [${record.partition()}] because it does not contain field '@event_name' or its alternative" }
        }  catch (nullpointer: NullPointerException){
            Metrics.onInvalidEventCounter.labelValues(record.topic(), "nullpointer").inc()
            log.warn { "ignoring record with offset [${record.offset()}] in partition [${record.partition()}] because a value is null'" }
            secureLog.warn(nullpointer) { "ignoring record with offset [${record.offset()}] in partition [${record.partition()}] because a value is null'" }
        }
    }

    private suspend fun broadcastMessage(jsonMessage: JsonMessage) {
        subscribers.forEach { subscriber ->
            measureTimedValue {
                subscriber.onMessage(jsonMessage)
            }.let { (outcome, duration) ->
                Metrics.onMessageCounter.labelValues(subscriber.name(), jsonMessage.eventName, outcome.status.name)
                    .inc()

                Metrics.onMessageHistorgram.labelValues(subscriber.name(), jsonMessage.eventName, outcome.status.name)
                    .observe(duration.toDouble(DurationUnit.SECONDS))
            }
        }
    }
}

// For use in tests in dependent projects
class MessageBroadcaster(
    private val subscribers: List<Subscriber>,
    eventNameFields: List<String> = listOf(JsonMessage.DEFAULT_EVENT_NAME),
    enableTracking: Boolean = false
) {
    constructor(vararg subscriber: Subscriber, eventNameFields: List<String> =
        listOf(JsonMessage.DEFAULT_EVENT_NAME), enableTracking: Boolean = false) :
        this(subscriber.toList(), eventNameFields, enableTracking)

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
                subscriber.onMessage(jsonMessage).let {
                    tracker?.track(subscriber, jsonMessage, it)
                }
            }
        }
    }

    fun history(): MessageTracker {
        return tracker ?: throw IllegalStateException("Must enable tracking to access message history")
    }

    fun clearHistory() {
        tracker?.reset() ?: throw IllegalStateException("Must enable tracking to clear history")
    }

    private val tracker: MessageTracker? = if (enableTracking) {
        MessageTracker()
    } else {
        null
    }
}

private object Metrics {
    private val registry = PrometheusRegistry.defaultRegistry

    val onMessageHistorgram = Histogram.builder()
        .name("kafka_message_processing_time_s")
        .help("Hvor lang det tar for en subscriber å prosessere melding i sekunder")
        .labelNames("subscriber", "event_name", "result")
        .register(registry)

    val onMessageCounter = Counter.builder()
        .name("kafka_message_counter")
        .help("Hvor mange ganger en subscriber har akseptert eller ignorert en melding.")
        .labelNames("subscriber", "event_name", "result")
        .register(registry)

    val onValidEventCounter = Counter.builder()
        .name("kafka_valid_event_counter")
        .help("Hvor mange gyldige eventer som er lest fra kafka")
        .labelNames("topic", "event_name")
        .register(registry)

    val onInvalidEventCounter = Counter.builder()
        .name("kafka_invalid_event_counter")
        .help("Hvor mange ugyldige eventer som er lest fra kafka")
        .labelNames("topic", "reason")
        .register(registry)
}
