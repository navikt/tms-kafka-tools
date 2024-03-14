package no.nav.helse.rapids_rivers

import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import org.junit.jupiter.api.Test

class AppTest {

    @Test
    fun test() {
        val application = KafkaApplication.build {
            kafkaConfig {
                readTopic("my-test-v1")
                groupId = "group-123"
            }

            ktorModule {
                routing {
                    get("/hello") {
                        call.respond("Hi!")
                    }
                }
            }

            onStartup {
                println("Starting!")
            }

            onShutdown {
                println("Stopping!")
            }

            subscriber {
                Subby()
            }
        }

        application.start()
    }
}

class Subby: Subscriber() {
    override fun subscribe() = Subscription.forEvent("test").requireFields("field")

    override fun receive(jsonMessage: NewJsonMessage) {
        println("Yo")
    }
}
