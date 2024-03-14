package no.nav.helse.rapids_rivers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.record.TimestampType
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime

data class NewJsonMessage internal constructor(
    val json: JsonNode,
    val metadata: EventMetadata?
) {
    operator fun get(fieldName: String): JsonNode = json.get(fieldName)!!

    fun getOrNull(fieldName: String): JsonNode? = json.get(fieldName)

    companion object {
        private val objectMapper = jacksonObjectMapper()

        fun initMessage(consumerRecord: ConsumerRecord<String, String>): NewJsonMessage {
            val json = try {
                objectMapper.readTree(consumerRecord.value())
            } catch (e: Exception) {
                throw JsonException(e.message!!)
            }

            if (!json.isContainerNode) {
                throw JsonException("Top-level json object must be container node")
            }

            return NewJsonMessage(
                json = json,
                metadata = EventMetadata(
                    topic = consumerRecord.topic(),
                    kafkaEvent = KafkaEvent(key = consumerRecord.key(), value = consumerRecord.value()),
                    opprettet = consumerRecord.timestampZ(),
                    lest = ZonedDateTime.now(ZoneId.of("Z"))
                )
            )
        }

        private fun ConsumerRecord<String, String>.timestampZ() = when (timestampType()) {
            TimestampType.LOG_APPEND_TIME, TimestampType.CREATE_TIME -> {
                Instant.ofEpochMilli(timestamp()).let { ZonedDateTime.ofInstant(it, ZoneId.of("Z")) }
            }
            else ->  null
        }

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

    internal fun withFields(fields: Collection<String>): NewJsonMessage {
        return copy(json = json.keepFields(fields))
    }
}

fun JsonNode?.isMissingOrNull() = this == null || isMissingNode || isNull

class JsonException(message: String): IllegalArgumentException(message)

data class EventMetadata(
    val topic: String,
    val kafkaEvent: KafkaEvent,
    val opprettet: ZonedDateTime?,
    val lest: ZonedDateTime
)

data class KafkaEvent(
    val key: String,
    val value: String
)
