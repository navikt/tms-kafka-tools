package no.nav.helse.rapids_rivers

import io.kotest.matchers.shouldBe
import io.ktor.client.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.testing.*
import kotlinx.coroutines.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.*
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.net.ServerSocket
import java.util.*
import java.util.concurrent.TimeUnit


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AppTest {

    private val testGroupId = "test-app"
    private val testTopic = "test-topic"

    private val testClient = TestClient()

    private val kafkaEnv: Map<String, String> by lazy {
        mapOf("KAFKA_BROKERS" to kafkaContainer.bootstrapServers)
    }
    private val kafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.1"))
    private lateinit var producer: KafkaProducer<String, String>

    private lateinit var testApp: KafkaApplication

    @BeforeAll
    fun setup() = runBlocking {
        kafkaContainer.start()

        producer = KafkaTestFactory(kafkaContainer).createProducer()

        testApp = setupApplication()

        launch(Dispatchers.Default + Job()) {
            testApp.start()
        }

        await("Wait for app to start")
            .atMost(5, TimeUnit.SECONDS)
            .until(testApp::isRunning)

    }


    @Test
    fun `meta-endpoints should be added automatically`() = runBlocking<Unit> {
        testClient.get("/isalive").status shouldBe HttpStatusCode.OK
        testClient.get("/isready").status shouldBe HttpStatusCode.OK
        testClient.get("/metrics").status shouldBe HttpStatusCode.OK
    }

    private fun testKafkaApp(block: suspend (TestClient) -> Unit) = runBlocking<Unit>  {

    }

    private fun setupApplication(subscriber: Subscriber? = null) = KafkaApplication.build {
        kafkaConfig {
            readTopic(testTopic)
            groupId = testGroupId
            environment = kafkaEnv
            enableSSL = false
        }

        httpPort = testClient.port

        if (subscriber != null){
            subscriber {
                subscriber
            }
        }
    }

    private class TestClient {

        val port = ServerSocket(0).use { it.localPort }
        private val url = "http://localhost:$port"

        private val httpClient = HttpClient { }

        suspend fun get(path: String) = httpClient.get("$url$path")
    }
}
