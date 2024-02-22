package no.nav.helse.rapids_rivers

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import java.util.*

class KafkaConfig(
    private val brokers: List<String>,
    private val truststorePath: String,
    private val truststorePw: String,
    private val keystorePath: String,
    private val keystorePw: String
) {
    companion object {
        val default: KafkaConfig get() {
            val env = System.getenv()
            return KafkaConfig(
                brokers = env.getValue("KAFKA_BROKERS").split(',').map(String::trim),
                truststorePath = env.getValue("KAFKA_TRUSTSTORE_PATH"),
                truststorePw = env.getValue("KAFKA_CREDSTORE_PASSWORD"),
                keystorePath = env.getValue("KAFKA_KEYSTORE_PATH"),
                keystorePw = env.getValue("KAFKA_CREDSTORE_PASSWORD")
            )
        }
    }

    init {
        check(brokers.isNotEmpty())
    }

    fun consumerConfig(groupId: String, properties: Properties) = Properties().apply {
        putAll(kafkaBaseConfig())
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
        putAll(properties)
        put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    }

    fun adminConfig(properties: Properties) = Properties().apply {
        putAll(kafkaBaseConfig())
        putAll(properties)
    }

    private fun kafkaBaseConfig() = Properties().apply {
        put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers)
        put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name)
        put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "")
        put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "jks")
        put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12")
        put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststorePath)
        put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePw)
        put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystorePath)
        put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePw)
    }
}
