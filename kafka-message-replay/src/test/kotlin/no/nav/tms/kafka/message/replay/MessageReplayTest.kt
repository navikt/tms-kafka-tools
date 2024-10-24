package no.nav.tms.kafka.message.replay

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.kotest.matchers.shouldBe
import io.ktor.client.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.auth.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import no.nav.tms.kafka.application.JsonMessage
import no.nav.tms.kafka.application.KafkaApplication
import no.nav.tms.kafka.application.Subscriber
import no.nav.tms.kafka.application.Subscription
import no.nav.tms.token.support.azure.validation.mock.azureMock
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
class MessageReplayTest {

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
    fun `message replay api plugin should enable fine-grained replay of kafka messages`() = runBlocking<Unit> {

        // Setup application with desired configuration

        val stateHolder = TestStateHolder()

        val greenBeadCounter = object : Subscriber() {
            override fun subscribe() = Subscription.forEvent("beads_counted")
                .withFields("color", "amount")

            override suspend fun receive(jsonMessage: JsonMessage) {
                stateHolder.beads.compute(jsonMessage["color"].asText()) { _, value ->
                    if (value != null) {
                        value + jsonMessage["amount"].asInt()
                    } else {
                        jsonMessage["amount"].asInt()
                    }
                }
            }
        }

        val ktorApi: Application.() -> Unit = {
            install(Authentication) {
                azureMock {
                    alwaysAuthenticated = true
                    setAsDefault = true
                }
            }

            install(MessageReplayApi) {
                enableKafkaSsl = false
                requireAuthentication = false
                environment = kafkaEnv
            }

            routing {
                get("/count/{color}") {
                    val color = call.parameters["color"]

                    call.respondText(stateHolder.beads[color].toString())
                }
            }
        }

        val application = setupApplication(stateHolder, greenBeadCounter, ktorApi)

        // Start application and verify startup hook

        launch(Dispatchers.IO) {
            application.start()
        }

        await("Wait for app to start")
            .atMost(5, TimeUnit.SECONDS)
            .until(application::isRunning)

        // Send kafka events and verify that the correct messages were passed on to subscriber

        val threeGreenBeads = """{ "@event_name": "beads_counted", "color": "green", "amount": 3 }"""
        val fiveBlueBeads = """{ "@event_name": "beads_counted", "color": "blue", "amount": 5 }"""
        val sevenRedBeads = """{ "@event_name": "beads_counted", "color": "red", "amount": 7 }"""
        val elevenYellowBeads = """{ "@event_name": "beads_counted", "color": "yellow", "amount": 11 }"""

        sendMessage(threeGreenBeads)
        val blueOffset = sendMessage(fiveBlueBeads)
        sendMessage(sevenRedBeads)
        sendMessage(elevenYellowBeads)

        await("wait for red beads to be counted")
            .atMost(10, TimeUnit.SECONDS)
            .until{ stateHolder.beads["yellow"] == 11 }

        // Confirm initial bead counts

        testClient.get("/count/green").bodyAsText() shouldBe "3"
        testClient.get("/count/blue").bodyAsText() shouldBe "5"
        testClient.get("/count/red").bodyAsText() shouldBe "7"
        testClient.get("/count/yellow").bodyAsText() shouldBe "11"

        // Replay blue and red

        val replayRequest = ReplayRequest(
            topic = testTopic,
            count = 2,
            offset = blueOffset,
            partition = 0
        )

        testClient.post("/message/replay", replayRequest)

        testClient.get("/count/green").bodyAsText() shouldBe "3"
        testClient.get("/count/blue").bodyAsText() shouldBe "10"
        testClient.get("/count/red").bodyAsText() shouldBe "14"
        testClient.get("/count/yellow").bodyAsText() shouldBe "11"

        application.stop()
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

    private fun sendMessage(body: String): Long {
        return producer.send(
            ProducerRecord(testTopic, UUID.randomUUID().toString(), body)
        ).get().offset()
    }
}

private class TestClient {

    val port = ServerSocket(0).use { it.localPort }
    private val baseUrl = "http://localhost:$port"

    private val httpClient = HttpClient { }

    suspend fun get(path: String) = httpClient.get("$baseUrl$path")

    private val objectMapper = jacksonObjectMapper()

    suspend fun post(path: String, payload: Any){
        try {
            httpClient.post {
                url("$baseUrl$path")
                header(HttpHeaders.ContentType, ContentType.Application.Json)
                setBody(objectMapper.writeValueAsString(payload))
            }
        } catch (e: Exception) {
            println("")
        }
    }
}

private data class TestStateHolder(
    var state: TestState = TestState.Waiting,
    var beads: MutableMap<String, Int> = mutableMapOf()
)

private enum class TestState {
    Waiting, Running, Stopped
}
