package no.nav.helse.rapids_rivers

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.kotest.assertions.throwables.shouldNotThrowAny
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.coroutines.*
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.awaitility.Awaitility
import org.junit.jupiter.api.*
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.shaded.org.awaitility.Awaitility.await
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.time.LocalDateTime
import java.util.*
import java.util.concurrent.TimeUnit

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaReaderTest {
    private val objectMapper = jacksonObjectMapper()
        .registerModule(JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    private val groupId = "test-app"

    private val testTopic = "test-topic"

    private lateinit var consumerFactory: ConsumerFactory

    private val kafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.1"))

    private lateinit var producer: KafkaProducer<String, String>

    private lateinit var adminClient: AdminClient

    private lateinit var kafkaReader: KafkaReader
    private lateinit var readerJob: Job

    private val consumerProperties = Properties().apply { put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") }


    @BeforeAll
    fun setup() {
        kafkaContainer.start()

        consumerFactory = ConsumerFactory(kafkaTestConfig(kafkaContainer))

        val testFactory = KafkaTestFactory(kafkaContainer)
        producer = testFactory.createProducer()
        adminClient = testFactory.createAdminClient()
    }

    @AfterAll
    fun teardown() {
        producer.close()
        kafkaContainer.stop()
    }

    @AfterEach
    fun stopReader() {
        if (this::kafkaReader.isInitialized) {
            kafkaReader.stop()
            await("wait until the kafkaReader stops")
                .atMost(10, TimeUnit.SECONDS)
                .until { !kafkaReader.isRunning() }
        }

        if (this::readerJob.isInitialized) {
            runBlocking { readerJob.cancelAndJoin() }
        }
    }

    @Test
    fun `no effect calling start multiple times`() {
        startTestReader()

        shouldNotThrowAny { kafkaReader.start() }
        kafkaReader.isRunning() shouldBe true
    }

    @Test
    fun `can stop`() {
        startTestReader()

        kafkaReader.stop()
        kafkaReader.isRunning() shouldBe false
        shouldNotThrowAny { kafkaReader.stop() }
    }

    @Test
    fun `should stop on errors`() {
        val failingSubscriber = object : Subscriber() {
            override fun subscribe() = Subscription.forEvent("test")

            override fun receive(jsonMessage: NewJsonMessage) {
                println("Hello!")
                throw Exception("Generic error")
            }
        }

        startTestReader(subscribers = listOf(failingSubscriber))

        producer.send(
            ProducerRecord(testTopic, UUID.randomUUID().toString(), """{ "@event_name": "test" }""")
        )

        await("wait until the kafkaReader stops")
            .atMost(10, TimeUnit.SECONDS)
            .until {
                !kafkaReader.isRunning()
            }
    }

    @Test
    fun `in case of exception, the offset committed is the erroneous record`() {
        val offsets = (0..100).map {
            producer.send(ProducerRecord(
                testTopic,
                UUID.randomUUID().toString(),
                """{"@event_name": "offset-test", "index": $it}""")
            )
                .get()
                .offset()
        }

        val failOnMessage = 50
        val expectedOffset = offsets[failOnMessage]
        var readFailedMessage = false

        val failingSubscriber = object : Subscriber() {
            override fun subscribe() = Subscription.forEvent("offset-test")
                .requireFields("index")

            override fun receive(jsonMessage: NewJsonMessage) {
                if (jsonMessage["index"].asInt() == failOnMessage) {
                    readFailedMessage = true
                    throw RuntimeException("an unexpected error happened")
                }
            }
        }

        startTestReader(subscribers = listOf(failingSubscriber))

        await("wait until the failed message has been read")
            .atMost(20, TimeUnit.SECONDS)
            .until { readFailedMessage }
        await("wait until the kafkaReader stops")
            .atMost(20, TimeUnit.SECONDS)
            .until { !kafkaReader.isRunning() }

        val actualOffset = adminClient
            .listConsumerGroupOffsets(groupId)
            ?.partitionsToOffsetAndMetadata()
            ?.get()
            ?.getValue(TopicPartition(testTopic, 0))
            ?: fail { "was not able to fetch committed offset for consumer $groupId" }

        expectedOffset shouldBe actualOffset.offset()
    }
//
//    @Test
//    fun `ignore tombstone messages`() {
//        val serviceId = "my-service"
//        val eventName = "heartbeat"
//
//        testRiver(eventName, serviceId)
//        val recordMetadata = waitForReply(testTopic, serviceId, eventName, null)
//
//        val offsets = kafkaAdmin
//            ?.listConsumerGroupOffsets(groupId)
//            ?.partitionsToOffsetAndMetadata()
//            ?.get()
//            ?: fail { "was not able to fetch committed offset for consumer $groupId" }
//        val actualOffset = offsets.getValue(TopicPartition(recordMetadata.topic(), recordMetadata.partition()))
//        val metadata = actualOffset.metadata() ?: fail { "expected metadata to be present in OffsetAndMetadata" }
//        Assertions.assertTrue(actualOffset.offset() >= recordMetadata.offset())
//        Assertions.assertTrue(objectMapper.readTree(metadata).has("groupInstanceId"))
//        Assertions.assertDoesNotThrow { LocalDateTime.parse(objectMapper.readTree(metadata).path("time").asText()) }
//    }
//
//    @Test
//    fun `read and produce message`() {
//        val serviceId = "my-service"
//        val eventName = "heartbeat"
//        val value = "{ \"@event\": \"$eventName\" }"
//
//        testRiver(eventName, serviceId)
//        val recordMetadata = waitForReply(testTopic, serviceId, eventName, value)
//
//        val offsets = kafkaAdmin
//            ?.listConsumerGroupOffsets(groupId)
//            ?.partitionsToOffsetAndMetadata()
//            ?.get()
//            ?: fail { "was not able to fetch committed offset for consumer $groupId" }
//        val actualOffset = offsets.getValue(TopicPartition(recordMetadata.topic(), recordMetadata.partition()))
//        val metadata = actualOffset.metadata() ?: fail { "expected metadata to be present in OffsetAndMetadata" }
//        Assertions.assertTrue(actualOffset.offset() >= recordMetadata.offset())
//        Assertions.assertTrue(objectMapper.readTree(metadata).has("groupInstanceId"))
//        Assertions.assertDoesNotThrow { LocalDateTime.parse(objectMapper.readTree(metadata).path("time").asText()) }
//    }
//
//    @DelicateCoroutinesApi
//    @Test
//    fun `seek to beginning`() {
//        val readMessages = mutableListOf<JsonMessage>()
//        River(kafkaReader).onSuccess { packet: JsonMessage, _: MessageContext -> readMessages.add(packet) }
//
//        var producedMessages = 0
//        await("wait until the kafkaReader has read the test message")
//            .atMost(5, TimeUnit.SECONDS)
//            .until {
//                kafkaReader.publish("{\"foo\": \"bar\"}")
//                producedMessages += 1
//                readMessages.size >= 1
//            }
//
//        kafkaReader.stop()
//        runBlocking { rapidJob.cancelAndJoin() }
//
//        readMessages.clear()
//
//        kafkaReader = createTestRapid()
//        River(kafkaReader).onSuccess { packet: JsonMessage, _: MessageContext -> readMessages.add(packet) }
//        kafkaReader.seekToBeginning()
//        kafkaReader.startNonBlocking()
//
//        await("wait until the kafkaReader has read more than one message")
//            .atMost(20, TimeUnit.SECONDS)
//            .until { readMessages.size >= producedMessages }
//    }
//
//    @Test
//    fun `read from others topics and produce to kafkaReader topic`() {
//        val serviceId = "my-service"
//        val eventName = "heartbeat"
//        val value = "{ \"@event\": \"$eventName\" }"
//
//        testRiver(eventName, serviceId)
//        waitForReply(anotherTestTopic, serviceId, eventName, value)
//    }
//
    private fun startTestReader(waitUpToSeconds: Long = 10, subscribers: List<Subscriber> = emptyList()) {
        val reader = KafkaReader(consumerFactory, groupId, listOf(testTopic), consumerProperties)

        subscribers.forEach {
            reader.register(it)
        }

        readerJob = CoroutineScope(Dispatchers.IO)
            .launch {
                try {
                    reader.start()
                } catch (err: Exception) {
                    println(err)
                }
            }

        if (waitUpToSeconds > 0) {
            Awaitility.await("wait until reader has started")
                .atMost(waitUpToSeconds, TimeUnit.SECONDS)
                .until(reader::isRunning)
        }

        kafkaReader = reader
    }
//
//    private fun testRiver(eventName: String, serviceId: String) {
//        River(kafkaReader).apply {
//            validate { it.requireValue("@event", eventName) }
//            validate { it.forbid("service_id") }
//            register(object : River.PacketListener {
//                override fun onPacket(packet: JsonMessage, context: MessageContext) {
//                    packet["service_id"] = serviceId
//                    context.publish(packet.toJson())
//                }
//
//                override fun onError(problems: MessageProblems, context: MessageContext) {}
//            })
//        }
//    }
//
//    private fun waitForReply(topic: String, serviceId: String, eventName: String, event: String?): RecordMetadata {
//        val sentMessages = mutableListOf<String>()
//        val key = UUID.randomUUID().toString()
//        val recordMetadata = kafkaProducer.send(ProducerRecord(topic, key, event)).get(5000, TimeUnit.SECONDS)
//        sentMessages.add(key)
//        await("wait until we get a reply")
//            .atMost(20, TimeUnit.SECONDS)
//            .until {
//                kafkaConsumer.poll(Duration.ZERO).forEach {
//                    if (!sentMessages.contains(it.key())) return@forEach
//                    if (it.key() != key) return@forEach
//                    return@until true
//                }
//                return@until false
//            }
//        return recordMetadata
//    }

    private fun kafkaTestConfig(container: KafkaContainer) = KafkaConfig.fromEnv(
        enableSsl = false,
        env = mapOf("KAFKA_BROKERS" to container.bootstrapServers)
    )
}
