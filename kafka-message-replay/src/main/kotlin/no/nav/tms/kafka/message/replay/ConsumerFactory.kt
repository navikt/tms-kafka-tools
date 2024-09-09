package no.nav.tms.kafka.message.replay

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer
import java.net.InetAddress
import java.util.*

internal class ConsumerFactory private constructor(
    private val baseProperties: Properties
) {
    private val stringDeserializer = StringDeserializer()

    fun createConsumer(records: Int): KafkaConsumer<String, String> {

        val withMaxPoll = Properties().apply {
            putAll(baseProperties)
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, records)
        }

        return KafkaConsumer(withMaxPoll, stringDeserializer, stringDeserializer)
    }

    companion object {
        fun init(enableSsl: Boolean, env: Map<String, String>): ConsumerFactory = Properties().apply {

            val clientId = generateClientId(env)

            configureBrokers(env)
            configureSecurity(enableSsl, env)

            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
            put(ConsumerConfig.CLIENT_ID_CONFIG, clientId)
            put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, clientId)
            put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000)

        }.let { ConsumerFactory(it) }

        private fun Properties.configureBrokers(env: Map<String, String>) {
            val brokers = env.getValue("KAFKA_BROKERS")
                .split(',')
                .map(String::trim)

            require(brokers.isNotEmpty()) { "Kafka brokers must not be empty" }

            put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers)
        }

        private fun Properties.configureSecurity(enableSsl: Boolean, env: Map<String, String>) {
            if (enableSsl) {
                put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name)
                put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "")
                put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "jks")
                put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12")
                put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, env.getValue("KAFKA_TRUSTSTORE_PATH"))
                put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, env.getValue("KAFKA_CREDSTORE_PASSWORD"))
                put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, env.getValue("KAFKA_KEYSTORE_PATH"))
                put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, env.getValue("KAFKA_CREDSTORE_PASSWORD"))
            } else {
                put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name)
                put(SaslConfigs.SASL_MECHANISM, "PLAIN")
            }
        }

        private fun generateClientId(env: Map<String, String>): String {
            return if (env.containsKey("NAIS_APP_NAME")) {
                InetAddress.getLocalHost().hostName
            } else {
                UUID.randomUUID().toString()
            }
        }
    }
}
