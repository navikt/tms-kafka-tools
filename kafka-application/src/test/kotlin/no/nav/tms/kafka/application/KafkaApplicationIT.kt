package no.nav.tms.kafka.application

import io.kotest.matchers.shouldBe
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import no.nav.tms.kafka.application.KafkaTestContainer.sendMessage
import no.nav.tms.kafka.application.KafkaTestContainer.sendMessageWithoutKey
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID
import java.util.concurrent.TimeUnit


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaApplicationIT {

    private val testClient = TestClient()

    @BeforeAll
    fun setup() {
        KafkaTestContainer.cleanTopic()
    }


    @Test
    fun `check lifecycle for app that counts beads from a kafka topic`() = runBlocking<Unit> {

        // Setup application with desired configuration

        val stateHolder = GreenBeadsTestStateHolder()

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

        val application = setupApplication(stateHolder, greenBeadCounter, ktorApi, { enabled = false }, initializatinJob)

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
        stateHolder: GreenBeadsTestStateHolder,
        subscriber: Subscriber,
        ktorModule: Application.() -> Unit,
        minSideMdcConfig: MinSideMdcConfigBuilder.() -> Unit = {
            enabled = false
        },
        initializatinJob: MockInitialization
    ) = KafkaApplication.build {

        kafkaConfig {
            readTopic(KafkaTestContainer.TEST_TOPIC)
            groupId = UUID.randomUUID().toString()
            environment = KafkaTestContainer.applicationKafkaEnv
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
}

private class GreenBeadsTestStateHolder : TestStateHolder() {
    var greenBeads: Int = 0
}
