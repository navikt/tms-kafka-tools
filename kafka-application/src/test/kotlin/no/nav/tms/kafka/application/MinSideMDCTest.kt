package no.nav.tms.kafka.application

import io.kotest.matchers.shouldBe
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

    @BeforeAll
    fun setup() {
        KafkaTestContainer.cleanTopic()
    }

    @Test
    fun `Disabler min side MDC og legger til default felt event`() = minSideMdcTest {
        subscribeToEvent = "test_event"
        messageContent = emptyMap()
        testConfig = {
            disable = true
        }
        mdcAssertions = {
            kafkaValues().size shouldBe 7
            nonKafkaValues().size shouldBe 1
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
            set(value) {
                field = value
                message = buildMessage()
            }
        var message: String = buildMessage()
        var groupId: String = UUID.randomUUID().toString()
        var testConfig: MinSideMdcConfig.() -> Unit = {}
        var mdcAssertions: (MdcSubscriber.() -> Unit)? = null
        var subscribeTofields: List<String> = emptyList()

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

    private fun setupApplication(
        stateHolder: TestStateHolder,
        subscriber: Subscriber,
        minSideMdcConfig: MinSideMdcConfig.() -> Unit = { disable = true },
        initJob: MockInitialization,
        builder: MinSideMdcTestBuilder
    ) = KafkaApplication.build {
        kafkaConfig {
            readTopic(TEST_TOPIC)
            groupId = builder.groupId // Use unique groupId per test
            environment = KafkaTestContainer.applicationKafkaEnv
            enableSSL = false
        }
        httpPort = 0 // Use random port or configure as needed
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
    }
}
