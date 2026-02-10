package no.nav.tms.kafka.application

import io.kotest.matchers.shouldBe
import io.ktor.server.routing.*
import kotlinx.coroutines.*
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.slf4j.MDC
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.concurrent.TimeUnit
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

@Execution(ExecutionMode.SAME_THREAD)
class MinSideMDCTest {

    companion object {
        private val testTopic = "test-topic"
        private val kafkaContainer = org.testcontainers.kafka.ConfluentKafkaContainer(org.testcontainers.utility.DockerImageName.parse("confluentinc/cp-kafka:7.8.0")).apply { start() }
        init {
            // Explicitly create the topic before any producer/consumer is created
            val admin = org.apache.kafka.clients.admin.AdminClient.create(mapOf("bootstrap.servers" to kafkaContainer.bootstrapServers))
            admin.createTopics(listOf(org.apache.kafka.clients.admin.NewTopic(testTopic, 1, 1))).all().get()
            admin.close()
        }
        private val producer: KafkaProducer<String, String> = KafkaTestFactory(kafkaContainer).createProducer()
    }

    @Test
    fun `Disabler min side MDC og legger til default felt event`() = minSideMdcTest {
        subscribeToEvent = "test_event"
        messageContent = emptyMap()
        message = buildMessage()
        testConfig = {
            disable = true
        }
        mdcAssertions = {
            kafkaValues().size shouldBe 7
            nonKafkaValues()["event"] shouldBe "test_event"
        }
    }

    @Test
    fun `kaster exception hvis required field mangler`() {
        assertThrows<IllegalArgumentException> {
            minSideMdcTest {
                testConfig = {
                    producedByFieldName = "producer"
                    idFieldName = "id"
                }
            }
        }
        assertThrows<IllegalArgumentException> {
            minSideMdcTest {
                testConfig = {
                    idFieldName = "id"
                    domain = Domain.microfrontend
                }
            }
        }
        assertThrows<IllegalArgumentException> {
            minSideMdcTest {
                testConfig = {
                    domain = Domain.microfrontend
                    producedByFieldName = "producer"
                }
            }
        }
    }

    @Test
    fun `kaster excpetion hvis required fields ikke er en del av en Subscribers subscription`() {
        assertThrows<IllegalArgumentException> {
            minSideMdcTest {
                subscribeTofields = listOf("producer", "id")
                testConfig = {
                    domain = Domain.microfrontend
                    producedByFieldName = "produced_by"
                    idFieldName = "id"
                }
            }
        }
        assertThrows<IllegalArgumentException> {
            minSideMdcTest {
                subscribeTofields = listOf("producer", "@event_name")
                testConfig = {
                    domain = Domain.microfrontend
                    producedByFieldName = "producer"
                    idFieldName = ""
                }
            }
        }
        assertThrows<IllegalArgumentException> {
            minSideMdcTest {
                subscribeTofields = listOf("id", "@event_name")
                testConfig = {
                    domain = Domain.microfrontend
                    producedByFieldName = "producer"
                    idFieldName = "id"
                }
            }
        }
    }

    @Test
    fun `Legger til mdc felter`() {
        minSideMdcTest {
            subscribeToEvent = "mdc_event"
            messageContent = mapOf(
                "producer" to "test-producer",
                "id" to "test-id"
            )
            message = buildMessage()
            subscribeTofields = listOf("producer", "id", "@event_name")
            testConfig = {
                domain = Domain.microfrontend
                producedByFieldName = "producer"
                idFieldName = "id"
            }
            mdcAssertions = {
                kafkaValues().size shouldBe 7
                val minSideValues = nonKafkaValues()
                minSideValues.size shouldBe 4
                minSideValues["domain"] shouldBe "microfrontend"
                minSideValues["produced_by"] shouldBe "test-producer"
                minSideValues["minside_id"] shouldBe "test-id"
                minSideValues["event"] shouldBe "mdc_event"
            }
        }
    }

    private open class MdcSubscriber(
        val builder: MinSideMdcTestBuilder,
        val messages: AtomicInteger = AtomicInteger(0)
    ) : Subscriber() {
        var mdcValues: Map<String, String> = emptyMap()
        override fun subscribe() = Subscription
            .forEvent(builder.subscribeToEvent)
            .withFields(*builder.subscribeTofields.toTypedArray())

        override suspend fun receive(jsonMessage: JsonMessage) {
            messages.incrementAndGet()
            mdcValues = MDC.getCopyOfContextMap() ?: emptyMap()
            println("[TEST] Message received by subscriber. messages=${messages.get()}")
        }

        fun kafkaValues() = mdcValues.filter {
            it.key.startsWith("kafka")
        }

        fun nonKafkaValues() = mdcValues.filter {
            !it.key.startsWith("kafka")
        }
    }

    private class MinSideMdcTestBuilder() {
        var subscribeToEvent: String = "test_event"
        var messageContent: Map<String, Any?> = emptyMap()
        var message: String = buildMessage()
        var groupId: String = UUID.randomUUID().toString()
        var testConfig: MinSideMdcConfig.() -> Unit = {}
        var mdcAssertions: (MdcSubscriber.() -> Unit)? = null
        var subscribeTofields: List<String> = emptyList()

        fun buildMessage(): String {
            val mapWithEvent = messageContent.toMutableMap().apply { put("@event_name", subscribeToEvent) }
            return mapWithEvent.entries.joinToString(prefix = "{", postfix = "}") { (k, v) ->
                val value = when (v) {
                    is String -> "\"$v\""
                    null -> "null"
                    else -> v.toString()
                }
                "\"$k\": $value"
            }
        }
    }

    private fun minSideMdcTest(
        builderAction: MinSideMdcTestBuilder.() -> Unit
    ) {
        val builder = MinSideMdcTestBuilder().apply(builderAction)
        runBlocking {
            val initJob = MockInitialization()
            val testStateHolder = TestStateHolder()
            val mdcSubscriber = MdcSubscriber(builder)
            val application = setupApplication(
                testStateHolder,
                subscriber = mdcSubscriber,
                ktorModule = { routing { get("dummy") { } } },
                minSideMdcConfig = builder.testConfig,
                initializatinJob = initJob,
                builder = builder
            )
            val exceptionHandler = CoroutineExceptionHandler { _, throwable ->
                throw throwable
            }
            val scope = CoroutineScope(Dispatchers.IO + SupervisorJob() + exceptionHandler)
            var testJob: Job? = null
            try {
                testJob = scope.launch(Dispatchers.IO) {
                    application.start()
                }
                initJob.complete()
                await("Wait for app to start")
                    .atMost(10, TimeUnit.SECONDS)
                    .until(application::isRunning)
                sendMessage(builder.message)
                await("wait for messages to be processed")
                    .atMost(10, TimeUnit.SECONDS)
                    .until { mdcSubscriber.messages.get() > 0 }

                builder.mdcAssertions!!.invoke(mdcSubscriber)
            } finally {
                application.stop()
                await("Wait for app to stop")
                    .atMost(5, TimeUnit.SECONDS)
                    .until { !application.isRunning() }
                testJob?.cancelAndJoin()
            }
        }
    }

    private fun sendMessage(body: String) {
        println("[TEST] Sending message to Kafka: $body")
        producer.send(
            ProducerRecord(testTopic, UUID.randomUUID().toString(), body)
        )
        producer.flush() // Ensure the message is delivered before Awaitility waits
        println("[TEST] Message sent and flushed.")
    }

    private fun setupApplication(
        stateHolder: TestStateHolder,
        subscriber: Subscriber,
        ktorModule: io.ktor.server.application.Application.() -> Unit,
        minSideMdcConfig: MinSideMdcConfig.() -> Unit = { disable = true },
        initializatinJob: MockInitialization,
        builder: MinSideMdcTestBuilder
    ) = KafkaApplication.build {
        val kafkaEnv = mapOf("KAFKA_BROKERS" to kafkaContainer.bootstrapServers)
        println("[TEST] Using Kafka brokers: ${kafkaEnv}")
        kafkaConfig {
            readTopic(testTopic)
            groupId = builder.groupId // Use unique groupId per test
            environment = kafkaEnv
            enableSSL = false
        }
        httpPort = 0 // Use random port or configure as needed
        onStartup {
            stateHolder.state = TestState.Starting
            initializatinJob.start()
        }
        onReady {
            stateHolder.state = TestState.Running
        }
        onShutdown {
            stateHolder.state = TestState.Stopped
        }
        subscriber {
            subscriber
        }
        minSideMdc {
            minSideMdcConfig()
        }
        healthCheck {
            if (stateHolder.healthy) AppHealth.Healthy else AppHealth.Unhealthy
        }
        ktorModule(ktorModule)
    }

    private class MockInitialization {
        var started = false
        private var isDone = false
        private val failAfter = java.time.Duration.ofSeconds(5)
        fun start() = runBlocking {
            started = true
            val start = java.time.Instant.now()
            while (!isDone) {
                if (java.time.Instant.now() > start + failAfter) {
                    throw java.util.concurrent.TimeoutException("Failed to complete within $failAfter seconds.")
                }
                delay(50)
            }
        }
        fun complete() {
            isDone = true
        }
    }

    private data class TestStateHolder(
        var healthy: Boolean = true,
        var state: TestState = TestState.Waiting,
    )

    private enum class TestState {
        Waiting, Starting, Running, Stopped
    }
}
