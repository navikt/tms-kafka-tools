package no.nav.tms.kafka.reader

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import java.util.*

class KafkaConfig private constructor(
    private val brokers: List<String>,
    private val sslConfig: SslConfig?
) {
    companion object {
        fun fromEnv(enableSsl: Boolean, env: Map<String, String>) = KafkaConfig(
            brokers = env.getValue("KAFKA_BROKERS").split(',').map(String::trim),
            sslConfig = if(enableSsl) {
                SslConfig(
                    truststorePath = env.getValue("KAFKA_TRUSTSTORE_PATH"),
                    truststorePw = env.getValue("KAFKA_CREDSTORE_PASSWORD"),
                    keystorePath = env.getValue("KAFKA_KEYSTORE_PATH"),
                    keystorePw = env.getValue("KAFKA_CREDSTORE_PASSWORD")
                )
            } else {
                null
            }

        )
    }

    init {
        require(brokers.isNotEmpty()) { "Kafka brokers must not be empty" }
    }

    fun consumerConfig(groupId: String, properties: Properties) = Properties().apply {
        put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers)
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
        put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        putAll(properties)

        if (sslConfig != null) {
            put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name)
            put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "")
            put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "jks")
            put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12")
            put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslConfig.truststorePath)
            put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslConfig.truststorePw)
            put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslConfig.keystorePath)
            put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslConfig.keystorePw)
        } else {
            put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name)
            put(SaslConfigs.SASL_MECHANISM, "PLAIN")
        }
    }

    private data class SslConfig(
        val truststorePath: String,
        val truststorePw: String,
        val keystorePath: String,
        val keystorePw: String
    )
}
