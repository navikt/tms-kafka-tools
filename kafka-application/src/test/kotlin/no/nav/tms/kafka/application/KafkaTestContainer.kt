package no.nav.tms.kafka.application

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.KafkaAdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.serialization.StringSerializer
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.utility.DockerImageName
import java.util.Properties
import java.util.UUID

object KafkaTestContainer {
    val instance: ConfluentKafkaContainer = ConfluentKafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.8.0")
    ).apply { start() }

    const val TEST_TOPIC = "test-topic"
    val factory = KafkaTestFactory(instance)
    private val testProducer = factory.createProducer()
    val applicationKafkaEnv = mapOf("KAFKA_BROKERS" to instance.bootstrapServers)

    init {
        val admin =
            AdminClient.create(mapOf("bootstrap.servers" to instance.bootstrapServers))
        admin.createTopics(listOf(NewTopic(TEST_TOPIC, 1, 1))).all().get()
        admin.close()
    }

    fun sendMessage(body: String) {
        println("[TEST] Sending message to Kafka: $body")
        testProducer.send(
            ProducerRecord(TEST_TOPIC, UUID.randomUUID().toString(), body)
        )
        testProducer.flush() // Ensure the message is delivered before Awaitility waits
        println("[TEST] Message sent and flushed.")
    }

    fun sendMessageWithoutKey(body: String) {
        testProducer.send(
            ProducerRecord(TEST_TOPIC, null, body)
        )
    }

    fun cleanTopic() {
        val admin = AdminClient.create(mapOf("bootstrap.servers" to instance.bootstrapServers))
        try {
            val topics = admin.listTopics().names().get()
            println("[KafkaTestContainer] Existing topics before clean: $topics")
            if (topics.contains(TEST_TOPIC)) {
                println("[KafkaTestContainer] Deleting topic: $TEST_TOPIC")
                admin.deleteTopics(listOf(TEST_TOPIC)).all().get()
                println("[KafkaTestContainer] Topic deleted: $TEST_TOPIC. Recreating...")
                admin.createTopics(listOf(NewTopic(TEST_TOPIC, 1, 1))).all().get()
                println("[KafkaTestContainer] Topic recreated: $TEST_TOPIC")
            } else {
                println("[KafkaTestContainer] Topic $TEST_TOPIC does not exist. Creating...")
                admin.createTopics(listOf(NewTopic(TEST_TOPIC, 1, 1))).all().get()
                println("[KafkaTestContainer] Topic created: $TEST_TOPIC")
            }
            val topicsAfter = admin.listTopics().names().get()
            println("[KafkaTestContainer] Existing topics after clean: $topicsAfter")
        } finally {
            admin.close()
        }
    }
}

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
