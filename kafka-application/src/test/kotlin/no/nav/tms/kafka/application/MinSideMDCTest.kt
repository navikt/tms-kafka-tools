package no.nav.tms.kafka.application

import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.ktor.client.statement.bodyAsText
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import kotlinx.coroutines.*
import no.nav.tms.kafka.application.KafkaTestContainer.TEST_TOPIC
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.slf4j.MDC
import java.util.concurrent.TimeUnit
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MinSideMDCTest {
    val testClient = TestClient()

    @BeforeAll
    fun setup() {
        KafkaTestContainer.cleanTopic()
    }

    @Test
    fun `Disabler min side MDC og legger til default felt event`() = minSideMdcTest {
        subscribeToEvent = "test_event"
        messageContent = emptyMap()
        testConfig = {
            enabled = false
        }
        subscriberAssertions = {
            kafkaValues().size shouldBe 7
            nonKafkaValues().size shouldBe 2
            nonKafkaValues()["subscriber"] shouldBe "MdcSubscriber"
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
            subscribeTofields = listOf("producer", "id", "@event_name")
            testConfig = {
                domain = Domain.microfrontend
                producedByFieldName = "producer"
                idFieldName = "id"
            }
            subscriberAssertions = {
                kafkaValues().size shouldBe 7
                val minSideValues = nonKafkaValues()
                minSideValues.size shouldBe 5
                minSideValues["subscriber"] shouldBe "MdcSubscriber"
                minSideValues["domain"] shouldBe "microfrontend"
                minSideValues["produced_by"] shouldBe "test-producer"
                minSideValues["minside_id"] shouldBe "test-id"
                minSideValues["event"] shouldBe "mdc_event"
            }
        }
    }

    @Test
    fun `Legger til custom MDC på apikall`() {
        minSideMdcTest {
            subscribeToEvent = "mdc_event"
            messageContent = mapOf(
                "producer" to "test-producer",
                "id" to "test-id"
            )
            subscribeTofields = listOf("producer", "id", "@event_name")
            testConfig = {
                domain = Domain.microfrontend
                producedByFieldName = "producer"
                idFieldName = "id"
            }
            apiAssertions = {
                val response = testClient.get("/assertmdc")
                response.bodyAsText() shouldBe """{"method": "GET", "route": "/assertmdc"}"""

            }
            subscriberAssertions = {
                //send ny melding for å hente ut ny MDC i subscriber etter API kall
                KafkaTestContainer.sendMessage(builder.message)
                await("wait for messages to be processed")
                    .atMost(1, TimeUnit.SECONDS)
                    .until { messages.get() > 1 }
                //verifiser at api mdc ikke lekker ut i subscriber
                kafkaValues().size shouldBe 7
                nonKafkaValues().apply {
                    size shouldBe 5
                    keys shouldContainExactly setOf("subscriber", "produced_by", "domain", "minside_id", "event")
                }
            }
        }

    }

    private class MdcSubscriber(
        val builder: MinSideMdcTestBuilder,
        val messages: AtomicInteger = AtomicInteger(0)
    ) : Subscriber() {
        var mdcValues: Map<String, String> = emptyMap()
        override fun subscribe() = Subscription
            .forEvent(builder.subscribeToEvent).apply {
                withFields(*builder.subscribeTofields.toTypedArray())
                if (builder.optionalIdFieldName != null) {
                    withOptionalFields(builder.optionalIdFieldName!!)
                }
            }


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
            set(value) {
                field = value
                message = buildMessage()
            }
        var message: String = buildMessage()
        var groupId: String = UUID.randomUUID().toString()
        var testConfig: MinSideMdcConfigBuilder.() -> Unit = {}
        var subscriberAssertions: (MdcSubscriber.() -> Unit)? = null
        var apiAssertions: (suspend (MdcSubscriber) -> Unit)? = null
        var subscribeTofields: List<String> = emptyList()
        var optionalIdFieldName: String? = null

        private fun buildMessage(): String {
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
                minSideMdcConfig = builder.testConfig,
                initJob = initJob,
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

                KafkaTestContainer.sendMessage(builder.message)
                await("wait for messages to be processed")
                    .atMost(3, TimeUnit.SECONDS)
                    .until { mdcSubscriber.messages.get() > 0 }
                if (builder.apiAssertions != null) {
                    builder.apiAssertions!!.invoke(mdcSubscriber)
                }
                builder.subscriberAssertions!!.invoke(mdcSubscriber)

            } finally {
                application.stop()
                await("Wait for app to stop")
                    .atMost(5, TimeUnit.SECONDS)
                    .until { !application.isRunning() }
                testJob?.cancelAndJoin()
            }
        }
    }

    private fun setupApplication(
        stateHolder: TestStateHolder,
        subscriber: Subscriber,
        minSideMdcConfig: MinSideMdcConfigBuilder.() -> Unit = { enabled = false },
        initJob: MockInitialization,
        builder: MinSideMdcTestBuilder
    ) = KafkaApplication.build {
        httpPort = testClient.port

        kafkaConfig {
            readTopic(TEST_TOPIC)
            groupId = builder.groupId // Use unique groupId per test
            environment = KafkaTestContainer.applicationKafkaEnv
            enableSSL = false
        }
        onStartup {
            stateHolder.state = TestState.Starting
            initJob.start()
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

        ktorModule {
            routing {
                get("assertmdc") {
                    val contextMap = MDC.getCopyOfContextMap() ?: emptyMap()
                    call.respond(contextMap.entries.joinToString(prefix = "{", postfix = "}") { (k, v) ->
                        "\"$k\": \"$v\""
                    })
                }
            }
        }
    }
}
