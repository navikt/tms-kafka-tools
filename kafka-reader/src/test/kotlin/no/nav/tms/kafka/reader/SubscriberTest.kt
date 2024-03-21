package no.nav.tms.kafka.reader

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.time.ZonedDateTime
import java.util.*


class SubscriberTest {
    private val objectMapper = jacksonObjectMapper()

    @Test
    fun `accepts only messages with correct name`() = runBlocking<Unit> {
        val orderListener = object : Subscriber() {
            var count = 0

            override fun subscribe() = Subscription.forEvent("order")

            override suspend fun receive(jsonMessage: JsonMessage) {
                count++
            }
        }

        """{ "@event_name": "sale" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 0

        """{ "@event_name": "delivery" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 0

        """{ "@event_name": "order" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 1
    }

    @Test
    fun `accepts only messages with required fields`() = runBlocking<Unit> {
        val orderListener = object : Subscriber() {
            var count = 0

            override fun subscribe() = Subscription.forEvent("order")
                .withFields("item")

            override suspend fun receive(jsonMessage: JsonMessage) {
                count++
            }
        }

        """{ "@event_name": "order" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 0

        """{ "@event_name": "order", "item": "apples" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 1

        """{ "@event_name": "order", "item": "apples", "price": 25.50 }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 2

        """{ "@event_name": "order", "price": 25.50 }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 2
    }

    @Test
    fun `accepts only messages with required value`() = runBlocking<Unit> {
        val orderListener = object : Subscriber() {
            var count = 0

            override fun subscribe() = Subscription.forEvent("order")
                .withValue("color", "green")

            override suspend fun receive(jsonMessage: JsonMessage) {
                count++
            }
        }

        """{ "@event_name": "order", "color": "blue" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 0

        """{ "@event_name": "order", "color": "green" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 1

        """{ "@event_name": "order", "color": "red" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 1
    }

    @Test
    fun `accepts only messages with any of required values`() = runBlocking<Unit> {
        val orderListener = object : Subscriber() {
            var count = 0

            override fun subscribe() = Subscription.forEvent("order")
                .withAnyValue("color", "green", "red")

            override suspend fun receive(jsonMessage: JsonMessage) {
                count++
            }
        }

        """{ "@event_name": "order", "color": "blue" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 0

        """{ "@event_name": "order", "color": "green" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 1

        """{ "@event_name": "order", "color": "red" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 2
    }

    @Test
    fun `allows required value to be non-string`() = runBlocking<Unit> {
        val orderListener = object : Subscriber() {
            var count = 0

            override fun subscribe() = Subscription.forEvent("order")
                .withValue("amount", 10)
                .withValue("price", 10.50)
                .withValue("delivered", true)

            override suspend fun receive(jsonMessage: JsonMessage) {
                count++
            }
        }

        """{ "@event_name": "order", "amount": 5, "price": 10.50, "delivered": true }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 0

        """{ "@event_name": "order", "amount": 10, "price": 20.0, "delivered": true }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 0

        """{ "@event_name": "order", "amount": 10, "price": 10.50, "delivered": false }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 0

        """{ "@event_name": "order", "amount": 10, "price": 10.50, "delivered": true }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 1
    }

    @Test
    fun `ignores rejected value`() = runBlocking<Unit> {
        val orderListener = object : Subscriber() {
            var count = 0

            override fun subscribe() = Subscription.forEvent("order")
                .rejectValue("color", "green")

            override suspend fun receive(jsonMessage: JsonMessage) {
                count++
            }
        }

        """{ "@event_name": "order", "color": "blue" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 1

        """{ "@event_name": "order", "color": "green" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 1

        """{ "@event_name": "order", "color": "red" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 2
    }

    private fun String.asMessage(topic: String = "testTopic", key: String = UUID.randomUUID().toString()) =
        objectMapper.readTree(this).let {
            JsonMessage(
                eventName = it["@event_name"].asText(),
                json = it,
                metadata = EventMetadata(
                    topic = topic,
                    kafkaEvent = KafkaEvent(key, this),
                    opprettet = ZonedDateTime.now(),
                    lest = ZonedDateTime.now()
                )
            )
        }

}
