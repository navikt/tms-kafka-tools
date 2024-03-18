package no.nav.helse.rapids_rivers

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.kotest.assertions.throwables.shouldNotThrowAny
import io.kotest.matchers.collections.shouldContainOnly
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.*
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.awaitility.Awaitility
import org.junit.jupiter.api.*
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.shaded.org.awaitility.Awaitility.await
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaReaderTest {
    private val groupId = "test-app"

    private val testTopic = "test-topic"

    private lateinit var consumerFactory: ConsumerFactory

    private val kafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.1"))

    private lateinit var producer: KafkaProducer<String, String>
    private lateinit var adminClient: AdminClient

    private val kafkaReaders: MutableList<KafkaReader> = mutableListOf()

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
    fun stopReaders() {
        kafkaReaders.forEach(KafkaReader::stop)
        kafkaReaders.clear()
    }

    @Test
    fun `no effect calling start multiple times`() {
        val reader = startTestReader()

        shouldNotThrowAny { reader.start(wait = false) }
        reader.isRunning() shouldBe true
    }

    @Test
    fun `can stop`() {
        val reader = startTestReader()

        reader.stop()
        reader.isRunning() shouldBe false
        shouldNotThrowAny { reader.stop() }
    }

    @Test
    fun `should stop on errors`() {
        val failingSubscriber = object : Subscriber() {
            override fun subscribe() = Subscription.forEvent("test")

            override fun receive(jsonMessage: NewJsonMessage) {
                throw Exception("Generic error")
            }
        }

        val reader = startTestReader(subscribers = listOf(failingSubscriber))

        producer.send(
            ProducerRecord(testTopic, UUID.randomUUID().toString(), """{ "@event_name": "test" }""")
        )

        await("wait until the kafkaReader stops")
            .atMost(10, TimeUnit.SECONDS)
            .until {
                !reader.isRunning()
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

        val reader = startTestReader(waitUpToSeconds = 0, subscribers = listOf(failingSubscriber))

        await("wait until the failed message has been read")
            .atMost(20, TimeUnit.SECONDS)
            .until { readFailedMessage }
        await("wait until the kafkaReader stops")
            .atMost(20, TimeUnit.SECONDS)
            .until { !reader.isRunning() }

        val actualOffset = adminClient
            .listConsumerGroupOffsets(groupId)
            ?.partitionsToOffsetAndMetadata()
            ?.get()
            ?.getValue(TopicPartition(testTopic, 0))
            ?: fail { "was not able to fetch committed offset for consumer $groupId" }

        expectedOffset shouldBe actualOffset.offset()
    }

    @Test
    fun `ignore tombstone messages`() {
        val recordMetadata = sendAndAwait(testTopic, null)

        startTestReader()

        val offsets = adminClient
            .listConsumerGroupOffsets(groupId)
            ?.partitionsToOffsetAndMetadata()
            ?.get()
            ?: fail { "was not able to fetch committed offset for consumer $groupId" }

        val actualOffset = offsets.getValue(TopicPartition(recordMetadata.topic(), recordMetadata.partition()))
        Assertions.assertTrue(actualOffset.offset() >= recordMetadata.offset())
    }

    @Test
    fun `can read message and pass on to subscribers`() {
        var appleOrders = 0
        var appleCount = 0

        val appleCounter = object : Subscriber() {
            override fun subscribe() = Subscription.forEvent("order_placed")
                .requireFields("count")
                .requireValue("name", "apples")

            override fun receive(jsonMessage: NewJsonMessage) {
                appleOrders += 1
                appleCount += jsonMessage["count"].asInt()
            }
        }

        val itemsForCategories = mutableMapOf<String, Set<String>>()

        val categoryChecker = object : Subscriber() {
            override fun subscribe() = Subscription.forEvent("order_placed")
                .requireFields("category", "name", "count")

            override fun receive(jsonMessage: NewJsonMessage) {
                itemsForCategories.compute(jsonMessage["category"].asText()) { _, existing ->
                    val name = jsonMessage["name"].asText()

                    existing?.plus(name) ?: setOf(name)
                }
            }
        }

        val breakChecker = object : Subscriber() {
            var breakSignalled = false

            override fun subscribe() = Subscription.forEvent("break")

            override fun receive(jsonMessage: NewJsonMessage) { breakSignalled = true }
        }

        listOf(
            orderJson("food", "apples", 10),
            orderJson("food", "bananas", 15),
            orderJson("food", "apples", 6),
            orderJson("food", "apples", 1),
            orderJson("drink", "juice", 5),
            orderJson("drink", "sparkling", 18),
            """{ "@event_name": "break" }"""
        ).forEach(::sendTestMessage)

        startTestReader(subscribers = listOf(categoryChecker, appleCounter, breakChecker))

        await("Wait until we have read messages up until break")
            .atMost(10, TimeUnit.SECONDS)
            .until(breakChecker::breakSignalled)

        appleOrders shouldBe 3
        appleCount shouldBe 17

        itemsForCategories["food"] shouldContainOnly listOf("apples", "bananas")
        itemsForCategories["drink"] shouldContainOnly listOf("juice", "sparkling")
    }

    private fun orderJson(category: String, name: String, count: Int) = """
    {
        "@event_name": "order_placed",
        "category": "$category",
        "name": "$name",
        "count": $count
    }
    """

    private fun sendTestMessage(value: String) =
        producer.send(ProducerRecord(testTopic, UUID.randomUUID().toString(), value))


    private fun startTestReader(waitUpToSeconds: Long = 10, subscribers: List<Subscriber> = emptyList()): KafkaReader {
        val reader = KafkaReader(consumerFactory, groupId, listOf(testTopic), consumerProperties)

        subscribers.forEach {
            reader.register(it)
        }

        kafkaReaders.add(reader)

        reader.start(wait = false)

        if (waitUpToSeconds > 0) {
            Awaitility.await("wait until reader has started")
                .atMost(waitUpToSeconds, TimeUnit.SECONDS)
                .until(reader::isRunning)
        }

        return reader
    }

    private fun sendAndAwait(topic: String, event: String?): RecordMetadata {
        val key = UUID.randomUUID().toString()
        val recordMetadata = producer.send(ProducerRecord(topic, key, event)).get(5000, TimeUnit.SECONDS)
        val consumer = consumerFactory.createConsumer(key).apply { subscribe(listOf(topic)) }
        await("wait until we get a reply")
            .atMost(20, TimeUnit.SECONDS)
            .until {
                consumer.poll(Duration.ZERO).forEach {
                    if (it.key() != key) return@forEach
                    return@until true
                }
                return@until false
            }
        return recordMetadata
    }

    private fun kafkaTestConfig(container: KafkaContainer) = KafkaConfig.fromEnv(
        enableSsl = false,
        env = mapOf("KAFKA_BROKERS" to container.bootstrapServers)
    )
}
