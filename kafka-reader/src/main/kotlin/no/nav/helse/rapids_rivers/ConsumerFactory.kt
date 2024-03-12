package no.nav.helse.rapids_rivers

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.*

class ConsumerFactory(private val config: KafkaConfig) {
    private val stringDeserializer = StringDeserializer()

    internal fun createConsumer(groupId: String, properties: Properties = Properties()): KafkaConsumer<String, String> {
        return KafkaConsumer(config.consumerConfig(groupId, properties), stringDeserializer, stringDeserializer)
    }
}
