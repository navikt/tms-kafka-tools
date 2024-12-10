package no.nav.tms.kafka.application

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.KafkaAdminClient
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.serialization.StringSerializer
import org.testcontainers.kafka.ConfluentKafkaContainer
import java.util.*

class KafkaTestFactory(kafkaContainer: ConfluentKafkaContainer) {
    private val stringSerializer = StringSerializer()

    private val connectionProperties = localProperties(kafkaContainer)

    private val producerProperties = copy(connectionProperties).apply {
        put(ProducerConfig.ACKS_CONFIG, "all")
        put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
        put(ProducerConfig.LINGER_MS_CONFIG, "0")
        put(ProducerConfig.RETRIES_CONFIG, "0")
    }

    fun createProducer(): KafkaProducer<String, String> {
        return KafkaProducer(producerProperties, stringSerializer, stringSerializer)
    }

    fun createAdminClient(): AdminClient = KafkaAdminClient.create(connectionProperties)

    private fun localProperties(kafkaContainer: ConfluentKafkaContainer) = Properties().apply {
        put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.bootstrapServers)
        put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT")
        put(SaslConfigs.SASL_MECHANISM, "PLAIN")
    }

    private fun copy(properties: Properties) = Properties().apply { putAll(properties) }
}
