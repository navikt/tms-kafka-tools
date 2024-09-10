package no.nav.tms.kafka.message.replay

import com.fasterxml.jackson.databind.DatabindException
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.auth.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.util.*
import io.ktor.util.pipeline.*
import no.nav.tms.kafka.application.MessageChannel
import no.nav.tms.token.support.azure.validation.AzureAuthenticator


class ReplayChannelConfig {
    var requireAuthentication = true
    var authenticatorName = AzureAuthenticator.name
    var enableKafkaSsl = true
    var environment: Map<String, String> = System.getenv()
}

class ReplayMessageChannel(val config: ReplayChannelConfig) {

    companion object : BaseApplicationPlugin<Application, ReplayChannelConfig, ReplayMessageChannel> {

        override val key: AttributeKey<ReplayMessageChannel> = AttributeKey("BackdoorChannel")
        override fun install(application: Application, configure: ReplayChannelConfig.() -> Unit): ReplayMessageChannel {
            val config = ReplayChannelConfig().apply(configure)

            val consumerFactory = ConsumerFactory.init(
                config.enableKafkaSsl,
                config.environment
            )

            requireNotNull(application.pluginOrNull(MessageChannel)) { "ReplayMessageChannel must be installed in a kafka-application app." }

            val broadcaster = application.plugin(MessageChannel).channel.broadcaster

            val messageReplay = MessageReplay(broadcaster, consumerFactory)

            application.routing {
                if (config.requireAuthentication) {

                    val authentication = application.pluginOrNull(Authentication)

                    requireNotNull(authentication) { "ReplayMessageChannel must be installed after Authentication" }

                    authenticate(config.authenticatorName) {
                        replayChannelApi(messageReplay)
                    }
                } else {
                    replayChannelApi(messageReplay)
                }

            }


            return ReplayMessageChannel(config)
        }
    }
}

private fun Route.replayChannelApi(messageReplay: MessageReplay) {
    val log = KotlinLogging.logger {}

    val objectMapper = jacksonObjectMapper()

    post("/message/replay") {
        try {
            val request = call.receiveText().let {
                objectMapper.readValue<ReplayRequest>(it)
            }

            require(request.count > 0) { "Must read at least one message" }

            log.info { "Processing request to replay messages: $request" }

            val readMessages = messageReplay.replayMessages(request)

            log.info { "Replayed $readMessages messages from topic: ${request.topic}, partition: ${request.partition}, offset: ${request.offset}" }
            call.respondText { "Replayed $readMessages messages from topic: ${request.topic}, partition: ${request.partition}, offset: ${request.offset}" }
        } catch (eoe: EmptyOffsetException) {
            call.respondText(status = HttpStatusCode.Accepted, text = eoe.message!!)
        } catch (iae: IllegalArgumentException) {
            call.respond(HttpStatusCode.BadRequest)
        } catch (de: DatabindException) {
            call.respondText(status = HttpStatusCode.BadRequest, text = "Invalid format for replay request")
        } catch (e: Exception) {
            call.respond(HttpStatusCode.InternalServerError)
        }
    }
}

data class ReplayRequest(
    val topic: String,
    val partition: Int,
    val offset: Long,
    val count: Int = 1
)
