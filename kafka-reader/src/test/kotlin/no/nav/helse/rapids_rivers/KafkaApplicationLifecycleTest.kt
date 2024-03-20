package no.nav.helse.rapids_rivers

import io.kotest.matchers.shouldBe
import io.ktor.client.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.KafkaProducer
import org.awaitility.Awaitility
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.util.concurrent.TimeUnit


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaApplicationLifecycleTest {

    private val kafkaEnv: Map<String, String> by lazy {
        mapOf("KAFKA_BROKERS" to kafkaContainer.bootstrapServers)
    }
    private val kafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.1"))
    private lateinit var producer: KafkaProducer<String, String>

    private lateinit var testApp: KafkaApplication

    @BeforeAll
    fun setup() = runBlocking<Unit> {
        kafkaContainer.start()

        producer = KafkaTestFactory(kafkaContainer).createProducer()

        launch(Dispatchers.Default + Job()) {
            testApp.start()
        }
    }

    @Test
    fun `start and shutdown hooks should work as intended`() = runBlocking<Unit> {

        val stateHolder = TestStateHolder()

        val application = setupApplication(stateHolder)

        stateHolder.state shouldBe TestState.Waiting

        launch(Dispatchers.IO) {
            application.start()
        }

        Awaitility.await("Wait for app to start")
            .atMost(5, TimeUnit.SECONDS)
            .until(application::isRunning)

        stateHolder.state shouldBe TestState.Running

        application.stop()

        Awaitility.await("Wait for app to stop")
            .atMost(5, TimeUnit.SECONDS)
            .until { !application.isRunning() }

        stateHolder.state shouldBe TestState.Stopped
    }


    private fun setupApplication(stateHolder: TestStateHolder) = KafkaApplication.build {
        kafkaConfig {
            readTopic("N/A")
            groupId = "N/A"
            environment = kafkaEnv
            enableSSL = false
        }

        onStartup {
            stateHolder.state = TestState.Running
        }

        onShutdown {
            stateHolder.state = TestState.Stopped
        }
    }
}

private data class TestStateHolder(
    var state: TestState = TestState.Waiting,
    var messagesAccepted: Int = 0
)

private enum class TestState {
    Waiting, Running, Stopped
}
