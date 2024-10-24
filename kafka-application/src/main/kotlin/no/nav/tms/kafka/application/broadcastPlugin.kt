package no.nav.tms.kafka.application

import io.ktor.server.application.*
import io.ktor.util.*


class ChannelWrapper {
    lateinit var broadcaster: RecordBroadcaster
}

class MessageChannel(val channel: ChannelWrapper) {

    companion object : BaseApplicationPlugin<Application, ChannelWrapper, MessageChannel> {

        override val key: AttributeKey<MessageChannel> = AttributeKey("MessageChannel")
        override fun install(pipeline: Application, configure: ChannelWrapper.() -> Unit): MessageChannel {
            val config = ChannelWrapper().apply(configure)
            return MessageChannel(config)
        }
    }
}

