package no.nav.tms.kafka.application

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.contains
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.record.TimestampType
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime

class JsonMessage internal constructor(
    val eventName: String,
    val json: JsonNode,
    val metadata: EventMetadata
) {
    operator fun get(fieldName: String): JsonNode = json.get(fieldName)!!

    fun getOrNull(fieldName: String): JsonNode? = json.get(fieldName)

    internal fun withFields(fields: Collection<String>) = JsonMessage(
        eventName = eventName,
        json = json.keepFields(fields),
        metadata = metadata
    )

    companion object {
        const val DEFAULT_EVENT_NAME = "@event_name"

        private val objectMapper = jacksonObjectMapper()

        private fun JsonNode.keepFields(fields: Collection<String>): JsonNode {
            val objectNode = objectMapper.createObjectNode()

            fields.forEach { field ->
                get(field)
                    .takeUnless { it.isMissingOrNull() }
                    ?.let { objectNode.replace(field, it) }
            }

            return objectNode
        }
    }
}

internal class JsonMessageBuilder(private val primaryNameField: String) {

    private val objectMapper = jacksonObjectMapper()

    fun fromRecord(consumerRecord: ConsumerRecord<String, String>): JsonMessage {
        val json = try {
            objectMapper.readTree(consumerRecord.value())
        } catch (e: Exception) {
            throw JsonException(e.message!!)
        }

        if (!json.isContainerNode) {
            throw JsonException("Root-level json object must be container node")
        }

        if (!json.contains(primaryNameField)) {
            throw MessageFormatException("Field '$primaryNameField' must be present at top level of json object")
        }

        return JsonMessage(
            eventName = json[primaryNameField].asText(),
            json = json,
            metadata = EventMetadata(
                topic = consumerRecord.topic(),
                kafkaEvent = KafkaEvent(key = consumerRecord.key(), value = consumerRecord.value()),
                createdAt = consumerRecord.timestampZ(),
                readAt = nowAtUtc()
            )
        )
    }

    fun fromJson(jsonString: String, eventMetadata: EventMetadata?): JsonMessage {
        val json = try {
            objectMapper.readTree(jsonString)
        } catch (e: Exception) {
            throw JsonException(e.message!!)
        }

        if (!json.isContainerNode) {
            throw JsonException("Root-level json object must be container node")
        }

        if (!json.contains(primaryNameField)) {
            throw MessageFormatException("Field '$primaryNameField' must be present at top level of json object")
        }

        val metadata = eventMetadata ?: EventMetadata(
            "unknown_topic",
            kafkaEvent = KafkaEvent(key = "unknown_key", value = jsonString),
            createdAt = nowAtUtc(),
            readAt = nowAtUtc()
        )

        return JsonMessage(
            eventName = json[primaryNameField].asText(),
            json = json,
            metadata = metadata
        )
    }

    private fun ConsumerRecord<String, String>.timestampZ() = when (timestampType()) {
        TimestampType.LOG_APPEND_TIME, TimestampType.CREATE_TIME -> {
            Instant.ofEpochMilli(timestamp()).let { ZonedDateTime.ofInstant(it, ZoneId.of("Z")) }
        }
        else ->  null
    }
}

private fun nowAtUtc() = ZonedDateTime.now(ZoneId.of("Z"))

fun JsonNode?.isMissingOrNull() = this == null || isMissingNode || isNull

class JsonException(message: String): IllegalArgumentException(message)
class MessageFormatException(message: String): IllegalArgumentException(message)

data class EventMetadata(
    val topic: String,
    val kafkaEvent: KafkaEvent,
    val createdAt: ZonedDateTime?,
    val readAt: ZonedDateTime
)

data class KafkaEvent(
    val key: String?,
    val value: String
)
