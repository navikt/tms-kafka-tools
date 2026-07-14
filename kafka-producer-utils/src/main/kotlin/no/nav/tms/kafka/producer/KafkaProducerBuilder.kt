package no.nav.tms.kafka.producer

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringSerializer
import java.lang.IllegalStateException
import java.util.Properties
import kotlin.reflect.KClass

object KafkaProducerBuilder {
    fun stringProducer(
        customKafkaConfig: KafkaConfig? = null,
        propertiesOverride: Properties.() -> Unit = {}
    ): KafkaProducer<String, String> {
        val kafkaConfig = customKafkaConfig ?: KafkaConfig(
            clientId = getEnvVar("NAIS_POD_NAME"),
            brokers = getEnvVar("KAFKA_BROKERS"),
            truststorePath = getEnvVar("KAFKA_TRUSTSTORE_PATH"),
            keystorePath = getEnvVar("KAFKA_KEYSTORE_PATH"),
            credstorePassword = getEnvVar("KAFKA_CREDSTORE_PASSWORD"),
        )

        val producerProperties = initProperties(kafkaConfig)

        // Configure serializers
        producerProperties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        producerProperties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java

        producerProperties.also(propertiesOverride)

        return KafkaProducer(producerProperties)
    }

    fun <K, V> producer(
        keySerializer: KClass<out Serializer<*>>,
        valueSerializer: KClass<out Serializer<*>>,
        customKafkaConfig: KafkaConfig? = null,
        propertiesOverride: Properties.() -> Unit = {}
    ): KafkaProducer<K, V> {

        val kafkaConfig = customKafkaConfig ?: KafkaConfig(
            clientId = getEnvVar("NAIS_POD_NAME"),
            brokers = getEnvVar("KAFKA_BROKERS"),
            truststorePath = getEnvVar("KAFKA_TRUSTSTORE_PATH"),
            keystorePath = getEnvVar("KAFKA_KEYSTORE_PATH"),
            credstorePassword = getEnvVar("KAFKA_CREDSTORE_PASSWORD"),
        )

        val producerProperties = initProperties(kafkaConfig)

        // Configure serializers
        producerProperties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = keySerializer.java
        producerProperties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = valueSerializer.java

        producerProperties.also(propertiesOverride)

        return KafkaProducer(producerProperties)
    }


    private fun initProperties(kafkaConfig: KafkaConfig): Properties {
        return Properties().apply {
            // Base connectivity config
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.brokers)
            put(ProducerConfig.CLIENT_ID_CONFIG, kafkaConfig.clientId)
            put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 40000)
            put(ProducerConfig.ACKS_CONFIG, "all")
            put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

            // Secure connection config
            put(SaslConfigs.SASL_MECHANISM, "PLAIN")
            put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
            put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "jks")
            put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12")
            put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, kafkaConfig.truststorePath)
            put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, kafkaConfig.credstorePassword)
            put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, kafkaConfig.keystorePath)
            put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, kafkaConfig.credstorePassword)
            put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, kafkaConfig.credstorePassword)
            put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "")
        }
    }

    fun setEnvOverride(override: Map<String, String>) {
        envOverride.clear()
        envOverride.putAll(override)
    }
    fun resetEnvOverride() {
        envOverride.clear()
    }

    private val envOverride = mutableMapOf<String, String>()

    private fun getEnvVar(name: String): String {
        return envOverride[name]
            ?: System.getenv(name)
            ?: throw IllegalStateException("Fant ikke miljøvariabel '$name'. Påse at nais-yaml er satt opp riktig, eller suppler variabel som override.")
    }

    class KafkaConfig(
        val clientId: String,
        val brokers: String,
        val truststorePath: String,
        val keystorePath: String,
        val credstorePassword: String,
    )
}
