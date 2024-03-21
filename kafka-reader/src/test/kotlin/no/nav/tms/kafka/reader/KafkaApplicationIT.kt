package no.nav.tms.kafka.reader

import io.kotest.matchers.shouldBe
import io.ktor.client.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.net.ServerSocket
import java.util.UUID
import java.util.concurrent.TimeUnit


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaApplicationIT {

    private val testGroupId = "test-app"
    private val testTopic = "test-topic"

    private val kafkaEnv: Map<String, String> by lazy {
        mapOf("KAFKA_BROKERS" to kafkaContainer.bootstrapServers)
    }
    private val kafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.1"))
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

        val application = setupApplication(stateHolder, greenBeadCounter, ktorApi)

        // Start application and verify startup hook

        stateHolder.state shouldBe TestState.Waiting

        launch(Dispatchers.IO) {
            application.start()
        }

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

        await("wait for green beads to be counted")
            .atMost(10, TimeUnit.SECONDS)
            .until{ stateHolder.greenBeads == 15 }

        stateHolder.greenBeads shouldBe 15

        // Test custom module

        testClient.get("/count").bodyAsText() shouldBe "15"

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
        ktorModule: Application.() -> Unit
    ) = KafkaApplication.build {

        kafkaConfig {
            readTopic(testTopic)
            groupId = testGroupId
            environment = kafkaEnv
            enableSSL = false
        }

        httpPort = testClient.port

        onStartup {
            stateHolder.state = TestState.Running
        }

        onShutdown {
            stateHolder.state = TestState.Stopped
        }

        subscriber {
            subscriber
        }

        ktorModule(ktorModule)
    }

    private fun sendMessage(body: String) {
        producer.send(
            ProducerRecord(testTopic, UUID.randomUUID().toString(), body)
        )
    }
}

private class TestClient {

    val port = ServerSocket(0).use { it.localPort }
    private val url = "http://localhost:$port"

    private val httpClient = HttpClient { }

    suspend fun get(path: String) = httpClient.get("$url$path")
}

private data class TestStateHolder(
    var state: TestState = TestState.Waiting,
    var greenBeads: Int = 0
)

private enum class TestState {
    Waiting, Running, Stopped
}
