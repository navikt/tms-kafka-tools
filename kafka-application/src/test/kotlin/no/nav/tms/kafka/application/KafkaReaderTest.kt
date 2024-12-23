package no.nav.tms.kafka.application

import io.kotest.assertions.throwables.shouldNotThrowAny
import io.kotest.matchers.collections.shouldContainOnly
import io.kotest.matchers.longs.shouldBeGreaterThan
import io.kotest.matchers.longs.shouldBeGreaterThanOrEqual
import io.kotest.matchers.longs.shouldBeLessThanOrEqual
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.*
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.awaitility.Awaitility
import org.junit.jupiter.api.*
import org.testcontainers.kafka.ConfluentKafkaContainer
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

    private val kafkaContainer = ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.8.0"))

    private lateinit var producer: KafkaProducer<String, String>
    private lateinit var adminClient: AdminClient

    private val kafkaReaders: MutableList<KafkaReader> = mutableListOf()

    @BeforeAll
    fun setup() {
        kafkaContainer.start()

        consumerFactory = ConsumerFactory.init(
            clientId = "test-app",
            enableSsl = false,
            env = mapOf("KAFKA_BROKERS" to kafkaContainer.bootstrapServers)
        )

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
        kafkaReaders.forEach { it.stop() }
        kafkaReaders.clear()
    }

    @Test
    fun `no effect calling start multiple times`() = runBlocking<Unit> {
        val reader = runTestReader()

        shouldNotThrowAny {
            reader.start()
        }
        reader.isRunning() shouldBe true
    }

    @Test
    fun `can stop`() {
        val reader = runTestReader()

        reader.stop()
        reader.isRunning() shouldBe false
        shouldNotThrowAny { reader.stop() }
    }

    @Test
    fun `should stop on errors`() {
        val failingSubscriber = object : Subscriber() {
            override fun subscribe() = Subscription.forEvent("test")

            override suspend fun receive(jsonMessage: JsonMessage) {
                throw Exception("Generic error")
            }
        }

        val reader = runTestReader(subscribers = listOf(failingSubscriber))

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
    fun `in case of unexpected exception, the offset committed is the erroneous record`() {
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
                .withFields("index")

            override suspend fun receive(jsonMessage: JsonMessage) {
                if (jsonMessage["index"].asInt() == failOnMessage) {
                    readFailedMessage = true
                    throw RuntimeException("an unexpected error happened")
                }
            }
        }

        val reader = runTestReader(waitUpToSeconds = 0, subscribers = listOf(failingSubscriber))

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
    fun `in case of MessageException, processing continues `() {
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
        val failingOffset = offsets[50]
        var readFailedMessage = false
        var readFinalMessage = false

        val failingSubscriber = object : Subscriber() {
            override fun subscribe() = Subscription.forEvent("offset-test")
                .withFields("index")

            override suspend fun receive(jsonMessage: JsonMessage) {
                val index = jsonMessage["index"].asInt()

                if (index == failOnMessage) {
                    readFailedMessage = true
                    throw MessageException("a known exception occurred")
                } else if (index == 100) {
                    readFinalMessage = true
                }
            }
        }

        runTestReader(waitUpToSeconds = 0, subscribers = listOf(failingSubscriber))

        await("wait until the failed message has been read")
            .atMost(20, TimeUnit.SECONDS)
            .until { readFailedMessage }
        await("wait until the kafkaReader stops")
            .atMost(20, TimeUnit.SECONDS)
            .until { readFinalMessage }

        val actualOffset = adminClient
            .listConsumerGroupOffsets(groupId)
            ?.partitionsToOffsetAndMetadata()
            ?.get()
            ?.getValue(TopicPartition(testTopic, 0))
            ?: fail { "was not able to fetch committed offset for consumer $groupId" }

        actualOffset.offset() shouldBeGreaterThan failingOffset
    }

    @Test
    fun `ignore tombstone messages`() {
        val recordMetadata = sendAndAwait(testTopic, null)

        runTestReader()

        val offsets = adminClient
            .listConsumerGroupOffsets(groupId)
            ?.partitionsToOffsetAndMetadata()
            ?.get()
            ?: fail { "was not able to fetch committed offset for consumer $groupId" }

        val actualOffset = offsets.getValue(TopicPartition(recordMetadata.topic(), recordMetadata.partition()))
        actualOffset.offset() shouldBeGreaterThanOrEqual recordMetadata.offset()
    }

    @Test
    fun `can read message and pass on to subscribers`() {
        var appleOrders = 0
        var appleCount = 0

        val appleCounter = object : Subscriber() {
            override fun subscribe() = Subscription.forEvent("order_placed")
                .withFields("count")
                .withValue("name", "apples")

            override suspend fun receive(jsonMessage: JsonMessage) {
                appleOrders += 1
                appleCount += jsonMessage["count"].asInt()
            }
        }

        val itemsForCategories = mutableMapOf<String, Set<String>>()

        val categoryChecker = object : Subscriber() {
            override fun subscribe() = Subscription.forEvent("order_placed")
                .withFields("category", "name", "count")

            override suspend fun receive(jsonMessage: JsonMessage) {
                itemsForCategories.compute(jsonMessage["category"].asText()) { _, existing ->
                    val name = jsonMessage["name"].asText()

                    existing?.plus(name) ?: setOf(name)
                }
            }
        }

        val breakChecker = object : Subscriber() {
            var breakSignalled = false

            override fun subscribe() = Subscription.forEvent("break")

            override suspend fun receive(jsonMessage: JsonMessage) { breakSignalled = true }
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

        runTestReader(subscribers = listOf(categoryChecker, appleCounter, breakChecker))

        await("Wait until we have read messages up until break")
            .atMost(10, TimeUnit.SECONDS)
            .until(breakChecker::breakSignalled)

        appleOrders shouldBe 3
        appleCount shouldBe 17

        itemsForCategories["food"] shouldContainOnly listOf("apples", "bananas")
        itemsForCategories["drink"] shouldContainOnly listOf("juice", "sparkling")
    }

    @Test
    fun `tolerates bad or broken json messages`() {
        val breakChecker = object : Subscriber() {
            var breakSignalled = false

            override fun subscribe() = Subscription.forEvent("break")

            override suspend fun receive(jsonMessage: JsonMessage) { breakSignalled = true }
        }

        listOf(
            """{completely invalid json""",
            """123""",
            """"json":"fragment"""",
            """{"@event_name": "break"}"""
        ).forEach(::sendTestMessage)

        val reader = runTestReader(subscribers = listOf(breakChecker))

        await("Wait until break has been signalled")
            .atMost(10, TimeUnit.SECONDS)
            .until(breakChecker::breakSignalled)

        reader.isRunning() shouldBe true
    }

    @Test
    fun `allows for alternative event-name fields`() {
        val crudReader = object : Subscriber() {
            var someValue = "something"

            override fun subscribe() = Subscription.forEvent("update")
                .withFields("newValue")

            override suspend fun receive(jsonMessage: JsonMessage) {
                jsonMessage["newValue"].asText().let { someValue = it }
            }
        }

        """
        {
            "@action": "update",
            "newValue": "something else"
        } 
        """.let(::sendTestMessage)

        runTestReader(eventNameFields = listOf("@action"), subscribers = listOf(crudReader))

        await("Wait until break has been signalled")
            .atMost(10, TimeUnit.SECONDS)
            .until { crudReader.someValue != "something" }

        crudReader.someValue shouldBe "something else"
    }

    @Test
    fun `allows for specifying different event name fields in prioritized order`() {
        val counterOne = object : Subscriber() {
            var counter = 0

            override fun subscribe() = Subscription.forEvent("one")

            override suspend fun receive(jsonMessage: JsonMessage) {
                counter++
            }
        }

        val counterTwo = object : Subscriber() {
            var counter = 0

            override fun subscribe() = Subscription.forEvent("two")

            override suspend fun receive(jsonMessage: JsonMessage) {
                counter++
            }
        }

        """
        {
            "@some": "one"
        } 
        """.let(::sendTestMessage)

        """
        {
            "@some": "one",
            "@other": "two"
        } 
        """.let(::sendTestMessage)

        """
        {
            "@other": "two"
        } 
        """.let(::sendTestMessage)

        runTestReader(eventNameFields = listOf("@some", "@other"), subscribers = listOf(counterOne, counterTwo))

        await("Wait until break has been signalled")
            .atMost(5, TimeUnit.SECONDS)
            .until { counterOne.counter + counterTwo.counter == 3 }

        counterOne.counter shouldBe 2
        counterTwo.counter shouldBe 1
    }

    @Test
    fun `tolerates nullpointer`() {

        var didRead = false

        val nullThrower = object : Subscriber() {
            override fun subscribe() = Subscription.forEvent("nully")

            override suspend fun receive(jsonMessage: JsonMessage) {
                didRead = true
                jsonMessage["nonexistant"]
            }
        }

        listOf(
            """{ "@event_name": "nully" }"""
        ).forEach(::sendTestMessage)

        val reader = runTestReader(subscribers = listOf(nullThrower))

        await("Wait until break has been signalled")
            .atMost(10, TimeUnit.SECONDS)
            .until { didRead }

        reader.isRunning() shouldBe true
    }

    @Test
    fun `excludes event name field from json object unless explicitly included`() {

        val missing = object : Subscriber() {
            override fun subscribe() = Subscription.forEvent("name")

            var didRead = false
            var npe = false

            override suspend fun receive(jsonMessage: JsonMessage) {
                try {
                    didRead = true
                    jsonMessage[JsonMessage.DEFAULT_EVENT_NAME]
                } catch (e: NullPointerException) {
                    npe = true
                }
            }
        }

        val present = object : Subscriber() {
            override fun subscribe() = Subscription.forEvent("name")
                .withFields("@event_name")

            var didRead = false
            var npe = false

            override suspend fun receive(jsonMessage: JsonMessage) {
                try {
                    didRead = true
                    jsonMessage[JsonMessage.DEFAULT_EVENT_NAME]
                } catch (e: NullPointerException) {
                    npe = true
                }
            }
        }

        listOf(
            """{ "@event_name": "name" }"""
        ).forEach(::sendTestMessage)

        val reader = runTestReader(subscribers = listOf(missing, present))

        await("Wait until break has been signalled")
            .atMost(10, TimeUnit.SECONDS)
            .until { missing.didRead && present.didRead }

        missing.npe shouldBe true
        present.npe shouldBe false

        reader.isRunning() shouldBe true
    }

    @Test
    fun `should rollback on reader job being cancelled`() {
        val slowscriber = object : Subscriber() {
            override fun subscribe() = Subscription.forEvent("event")

            var didRead = false

            override suspend fun receive(jsonMessage: JsonMessage) {
                delay(1000)
                didRead = true
            }
        }

        sendAndAwait(testTopic, """{ "@event_name": "unrelated" }""")

        val reader = runTestReader(subscribers = listOf(slowscriber))

        reader.stop()

        val recordMetadata = sendAndAwait(testTopic, """{ "@event_name": "event" }""")

        slowscriber.didRead shouldBe false

        val offsets = adminClient
            .listConsumerGroupOffsets(groupId)
            ?.partitionsToOffsetAndMetadata()
            ?.get()
            ?: fail { "was not able to fetch committed offset for consumer $groupId" }

        val actualOffset = offsets.getValue(TopicPartition(recordMetadata.topic(), recordMetadata.partition()))

        actualOffset.offset() shouldBeLessThanOrEqual recordMetadata.offset()
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


    private fun runTestReader(
        eventNameFields: List<String> = listOf(JsonMessage.DEFAULT_EVENT_NAME),
        waitUpToSeconds: Long = 10,
        subscribers: List<Subscriber> = emptyList()
    ): KafkaReader {

        val reader = KafkaReader(consumerFactory, groupId, listOf(testTopic), RecordBroadcaster(subscribers, eventNameFields))

        kafkaReaders.add(reader)

        reader.start()

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
}
