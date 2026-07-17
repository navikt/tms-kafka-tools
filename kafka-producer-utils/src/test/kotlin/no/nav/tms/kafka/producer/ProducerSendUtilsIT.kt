package no.nav.tms.kafka.producer

import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import no.nav.tms.kafka.producer.ProducerSendUtils.batched
import no.nav.tms.kafka.producer.ProducerSendUtils.sendSynchronized
import no.nav.tms.kafka.producer.ProducerSendUtils.transactional
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.KafkaAdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.Properties
import java.util.UUID

class ProducerSendUtilsIT {

    private val testProducer = KafkaTestContainer.createProducer()

    @AfterEach
    fun cleanUp() {
        KafkaTestContainer.resetTopics()
    }

    @Test
    fun `sender kafka-records synkront`() {

        val data = """{"data": "abc-123"}"""

        val event = outputRecord(data)

        testProducer.sendSynchronized(event)

        val output = KafkaTestContainer.getOutput()

        output.count() shouldBe 1
        output.first { it.key() == event.key() }
            .let { it.value() shouldBe data }
    }

    @Test
    fun `sender kafka-records i synkron batch og kjører onSuccess`() {

        val numRecords = 100

        var successfulRecords = 0

        testProducer.batched {
            (0 until numRecords).forEach { i ->
                val data = """{"data": "abc-$i"}"""

                val event = outputRecord(data)

                sendInBatch(event) {
                    successfulRecords += 1
                }
            }
        }

        successfulRecords shouldBe numRecords

        val output = KafkaTestContainer.getOutput()

        output.count() shouldBe 100
    }

    @Test
    fun `sender kafka-records transaksjonelt`() {
        val groupid = "TransactionalSendTest"
        val numRecords = 100

        val txConsumer = KafkaTestContainer.createConsumer(groupid)
        txConsumer.subscribe(listOf(KafkaTestContainer.INPUT_TOPIC))

        (0 until numRecords).forEach { i ->
            KafkaTestContainer.sendInput("input-$i", "$i")
        }

        val txProducer = KafkaTestContainer.createProducer(transactional = true)
        txProducer.initTransactions()

        txConsumer.poll(Duration.ofSeconds(5)).let { records ->
            txProducer.transactional(records, txConsumer) {

                records.forEach { record ->
                    val value = """{"data": "abc-${record.value()}"}"""

                    sendInTransaction(outputRecord(value))
                }
            }
        }

        KafkaTestContainer.getOutput().count() shouldBe numRecords

        val committed = KafkaTestContainer.getCommittedOffsets(groupid)

        committed.shouldNotBeNull()
        committed.offset() shouldBe numRecords.toLong()
    }

    private fun outputRecord(data: String): ProducerRecord<String, String> =
        ProducerRecord(KafkaTestContainer.OUTPUT_TOPIC, UUID.randomUUID().toString(), data)
}

private object KafkaTestContainer {

    const val INPUT_TOPIC = "input_topic"
    const val OUTPUT_TOPIC = "output_topic"

    private val instance: ConfluentKafkaContainer = ConfluentKafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.8.0")
    ).apply { start() }

    private val stringSerializer = StringSerializer()
    private val stringDeserializer = StringDeserializer()

    private fun connectionProperties() = Properties().apply {
        put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, instance.bootstrapServers)
        put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT")
        put(SaslConfigs.SASL_MECHANISM, "PLAIN")
    }

    private fun producerProperties(transactional: Boolean) = connectionProperties().apply {
        put(ProducerConfig.ACKS_CONFIG, "all")
        put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")

        if (transactional) {
            put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
            put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test-tx")
        } else {
            put(ProducerConfig.RETRIES_CONFIG, "0")
            put(ProducerConfig.LINGER_MS_CONFIG, "0")
        }

    }

    private fun consumerProperties(groupId: String? = null) = connectionProperties().apply {
        val clientId = UUID.randomUUID().toString()

        if (groupId != null) {
            put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        }

        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, clientId)
        put(ConsumerConfig.CLIENT_ID_CONFIG, clientId)
        put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000)
        put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 200)
    }

    private val testProducer = createProducer()
    private val adminClient = createAdminClient()

    init {
        createTopics()
    }

    fun sendInput(key: String, value: String) {
        println("[TEST] Sending message to Kafka: [key: $key, value: $value]")
        testProducer.send(
            ProducerRecord(INPUT_TOPIC, key, value)
        )
        testProducer.flush() // Ensure the message is delivered before Awaitility waits
        println("[TEST] Message sent and flushed.")
    }

    // Recorsd committed to output topic
    fun getOutput(): ConsumerRecords<String, String> {
        val consumer = createConsumer()

        consumer.assign(listOf(TopicPartition(OUTPUT_TOPIC, 0)))

        return consumer.poll(Duration.ofSeconds(1))
    }

    // Offsets committed to input topic
    fun getCommittedOffsets(groupId: String): OffsetAndMetadata? {
        return adminClient.listConsumerGroupOffsets(groupId)
            .all()
            .get()
            .get(groupId)
            ?.get(TopicPartition(INPUT_TOPIC, 0))
    }

    private fun createTopics() {
        println("[KafkaTestContainer] Creating topics: [$INPUT_TOPIC, $OUTPUT_TOPIC]")
        val admin =
            AdminClient.create(mapOf("bootstrap.servers" to instance.bootstrapServers))
        admin.createTopics(listOf(
            NewTopic(INPUT_TOPIC, 1, 1),
            NewTopic(OUTPUT_TOPIC, 1, 1)
        )).all().get()
        admin.close()
    }

    private fun deleteTopics() {
        println("[KafkaTestContainer] Deleting topics: [$INPUT_TOPIC, $OUTPUT_TOPIC]")
        val admin =
            AdminClient.create(mapOf("bootstrap.servers" to instance.bootstrapServers))
        admin.deleteTopics(listOf(INPUT_TOPIC, OUTPUT_TOPIC))
            .all()
            .get()
        admin.close()
    }

    fun resetTopics() {
        println("[KafkaTestContainer] Resetting topics...")
        deleteTopics()
        createTopics()
        println("[KafkaTestContainer] Topics reset.")
    }

    fun createProducer(transactional: Boolean = false): KafkaProducer<String, String> {
        return KafkaProducer(producerProperties(transactional), stringSerializer, stringSerializer)
    }

    fun createConsumer(groupId: String? = null): KafkaConsumer<String, String> {
        return KafkaConsumer<String, String>(
            consumerProperties(groupId),
            stringDeserializer,
            stringDeserializer
        )
    }

    private fun createAdminClient(): AdminClient  = KafkaAdminClient.create(connectionProperties())
}

