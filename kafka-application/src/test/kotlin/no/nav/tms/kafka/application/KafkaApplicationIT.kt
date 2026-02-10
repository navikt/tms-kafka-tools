package no.nav.tms.kafka.application

import io.kotest.matchers.maps.shouldBeEmpty
import io.kotest.matchers.shouldBe
import io.ktor.client.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.slf4j.MDC
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.utility.DockerImageName
import java.net.ServerSocket
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaApplicationIT {

    private val testGroupId = "test-app"
    private val testTopic = "test-topic"

    private val kafkaEnv: Map<String, String> by lazy {
        mapOf("KAFKA_BROKERS" to kafkaContainer.bootstrapServers)
    }
    private val kafkaContainer = ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.8.0"))
    private lateinit var producer: KafkaProducer<String, String>

    private val testClient = TestClient()

    @BeforeAll
    fun setup() {
        kafkaContainer.start()

        producer = KafkaTestFactory(kafkaContainer).createProducer()
    }

    @Test
    fun `check lifecycle for app that counts beads from a kafka topic`() = runBlocking<Unit> {

        // Setup application with desired configuration

        val stateHolder = TestStateHolder()

        val greenBeadCounter = object : Subscriber() {
            override fun subscribe() = Subscription.forEvent("beads_counted")
                .withFields("amount")
                .withValue("color", "green")

            override suspend fun receive(jsonMessage: JsonMessage) {
                stateHolder.greenBeads += jsonMessage["amount"].asInt()
            }
        }

        val ktorApi: Application.() -> Unit = {
            routing {
                get("/count") {
                    call.respondText(stateHolder.greenBeads.toString())
                }
            }
        }

        // Mock some init job (flyway migration etc..)
        val initializatinJob = MockInitialization()

        val application = setupApplication(stateHolder, greenBeadCounter, ktorApi, { disable = true }, initializatinJob)

        // Start application and verify startup hook

        stateHolder.state shouldBe TestState.Waiting

        launch(Dispatchers.IO) {
            application.start()
        }

        await("Wait for init job to start")
            .atMost(5, TimeUnit.SECONDS)
            .until(initializatinJob::started)

        stateHolder.state shouldBe TestState.Starting
        application.isRunning() shouldBe false

        // Complete initialization step and verify onReady hook

        initializatinJob.complete()

        await("Wait for app to start")
            .atMost(5, TimeUnit.SECONDS)
            .until(application::isRunning)

        stateHolder.state shouldBe TestState.Running

        // Verify standard endpoints

        testClient.get("/isalive").status shouldBe HttpStatusCode.OK
        testClient.get("/isready").status shouldBe HttpStatusCode.OK
        testClient.get("/metrics").status shouldBe HttpStatusCode.OK

        // Send kafka events and verify that the correct messages were passed on to subscriber

        val brokenEvent = """{invalid json"""
        val incompleteEvent = """"naked":"node""""
        val sixGreenBeads = """{ "@event_name": "beads_counted", "color": "green", "amount": 6 }"""
        val nineGreenBeads = """{ "@event_name": "beads_counted", "color": "green", "amount": 9 }"""
        val yellowBeads = """{ "@event_name": "beads_counted", "color": "yellow", "amount": 10 }"""

        listOf(brokenEvent, incompleteEvent, sixGreenBeads, nineGreenBeads, yellowBeads)
            .forEach(::sendMessage)

        val eventMissingKey = """{ "@event_name": "beads_counted", "color": "green", "amount": 5 }"""

        listOf(eventMissingKey)
            .forEach(::sendMessageWithoutKey)

        await("wait for green beads to be counted")
            .atMost(10, TimeUnit.SECONDS)
            .until { stateHolder.greenBeads == 20 }

        stateHolder.greenBeads shouldBe 20

        // Test custom module

        testClient.get("/count").bodyAsText() shouldBe "20"

        // Test health

        testClient.get("/isalive").status shouldBe HttpStatusCode.OK

        stateHolder.healthy = false

        testClient.get("/isalive").status shouldBe HttpStatusCode.ServiceUnavailable

        // Stop application and verify shutdown

        application.stop()

        await("Wait for app to stop")
            .atMost(5, TimeUnit.SECONDS)
            .until { !application.isRunning() }

        stateHolder.state shouldBe TestState.Stopped
    }


    private fun setupApplication(
        stateHolder: TestStateHolder,
        subscriber: Subscriber,
        ktorModule: Application.() -> Unit,
        minSideMdcConfig: MinSideMdcConfig.() -> Unit = {
            disable = true
        },
        initializatinJob: MockInitialization
    ) = KafkaApplication.build {

        kafkaConfig {
            readTopic(testTopic)
            groupId = testGroupId
            environment = kafkaEnv
            enableSSL = false
        }

        httpPort = testClient.port

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
            if (stateHolder.healthy) {
                AppHealth.Healthy
            } else {
                AppHealth.Unhealthy
            }
        }

        ktorModule(ktorModule)
    }

    private fun sendMessage(body: String) {
        producer.send(
            ProducerRecord(testTopic, UUID.randomUUID().toString(), body)
        )
    }

    private fun sendMessageWithoutKey(body: String) {
        producer.send(
            ProducerRecord(testTopic, null, body)
        )
    }

    @Nested
    inner class MinSideMDCTest {

        @Test
        fun `Disabler min side MDC og legger til default felt event`() = minSideMdcTest {
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
                message = """{ "@event_name": "test_event", "producer": "test-producer", "id": "test-id" }"""
                subscribeTofields = listOf("producer", "id", "@event_name")
                testConfig = {
                    domain = Domain.microfrontend
                    producedByFieldName = "producer"
                    idFieldName = "id"
                }
                mdcAssertions = {
                    kafkaValues().size shouldBe 7
                    val minSideValues = kafkaValues()
                    minSideValues["domain"] shouldBe "microfrontend"
                    minSideValues["produced_by"] shouldBe "test-producer"
                    minSideValues["minside_id"] shouldBe "test-id"
                    minSideValues["event"] shouldBe "test_event"
                }
            }
        }


        private open inner class MdcSubscriber(
            val subscribeTofields: List<String>,
            @Volatile var messages: Int = 0
        ) : Subscriber() {
            lateinit var mdcValues: Map<String, String>
            override fun subscribe() = Subscription
                .forEvent("test_event")
                .withFields(*subscribeTofields.toTypedArray())

            override suspend fun receive(jsonMessage: JsonMessage) {
                messages++
                mdcValues = MDC.getCopyOfContextMap() ?: emptyMap()

            }

            fun kafkaValues() = mdcValues.filter {
                it.key.startsWith("kafka")
            }

            fun nonKafkaValues() = mdcValues.filter {
                !it.key.startsWith("kafka")
            }
        }


        private inner class MinSideMdcTestBuilder() {
            var message: String = """{ "@event_name": "test_event" }"""
            var testConfig: MinSideMdcConfig.() -> Unit = {}
            var mdcAssertions: (MdcSubscriber.() -> Unit)? = null
            var subscribeTofields: List<String> = emptyList()
        }

        private fun minSideMdcTest(
            builderAction: MinSideMdcTestBuilder.() -> Unit
        ) {
            val builder = MinSideMdcTestBuilder().apply(builderAction)

            runBlocking {
                val initJob = MockInitialization()
                val testStateHolder = TestStateHolder()
                val mdcSubscriber = MdcSubscriber(builder.subscribeTofields)


                val application = setupApplication(
                    testStateHolder,
                    subscriber = mdcSubscriber,
                    ktorModule = { routing { get("dummy") { } } },
                    minSideMdcConfig = builder.testConfig,
                    initializatinJob = initJob
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
                        .atMost(5, TimeUnit.SECONDS)
                        .until(application::isRunning)

                    sendMessage(builder.message)

                    await("wait for messages to be processed")
                        .atMost(5, TimeUnit.SECONDS)
                        .until { mdcSubscriber.messages > 0 }
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

    }
}

private class MockInitialization {
    var started = false
    private var isDone = false

    private val failAfter = Duration.ofSeconds(5)

    fun start() = runBlocking {
        started = true
        val start = Instant.now()
        while (!isDone) {
            if (Instant.now() > start + failAfter) {
                throw TimeoutException("Failed to complete within $failAfter seconds.")
            }
            delay(50)
        }
    }

    fun complete() {
        isDone = true
    }
}

private class TestClient {

    val port = ServerSocket(0).use { it.localPort }
    private val url = "http://localhost:$port"

    private val httpClient = HttpClient { }

    suspend fun get(path: String) = httpClient.get("$url$path")
}

private data class TestStateHolder(
    var healthy: Boolean = true,
    var state: TestState = TestState.Waiting,
    var greenBeads: Int = 0
)

private enum class TestState {
    Waiting, Starting, Running, Stopped
}
