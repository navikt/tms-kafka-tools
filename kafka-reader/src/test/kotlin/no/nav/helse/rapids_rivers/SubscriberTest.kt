package no.nav.helse.rapids_rivers

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import java.time.ZonedDateTime
import java.util.*


class SubscriptionStressTest {
    private val objectMapper = jacksonObjectMapper()

    @Test
    fun `accepts only messages with correct name`() {
        val orderListener = object : Subscriber() {
            var count = 0

            override fun subscribe() = Subscription.forEvent("order")

            override fun receive(jsonMessage: JsonMessage) {
                count++
            }
        }

        """{ "@event_name": "sale" }"""
            .asMessage()
            .let(orderListener::onMessage)

        orderListener.count shouldBe 0

        """{ "@event_name": "delivery" }"""
            .asMessage()
            .let(orderListener::onMessage)

        orderListener.count shouldBe 0

        """{ "@event_name": "order" }"""
            .asMessage()
            .let(orderListener::onMessage)

        orderListener.count shouldBe 1
    }

    @Test
    fun `accepts only messages with required fields`() {
        val orderListener = object : Subscriber() {
            var count = 0

            override fun subscribe() = Subscription.forEvent("order")
                .requireFields("item")

            override fun receive(jsonMessage: JsonMessage) {
                count++
            }
        }

        """{ "@event_name": "order" }"""
            .asMessage()
            .let(orderListener::onMessage)

        orderListener.count shouldBe 0

        """{ "@event_name": "order", "item": "apples" }"""
            .asMessage()
            .let(orderListener::onMessage)

        orderListener.count shouldBe 1

        """{ "@event_name": "order", "item": "apples", "price": 25.50 }"""
            .asMessage()
            .let(orderListener::onMessage)

        orderListener.count shouldBe 2

        """{ "@event_name": "order", "price": 25.50 }"""
            .asMessage()
            .let(orderListener::onMessage)

        orderListener.count shouldBe 2
    }

    @Test
    fun `accepts only messages with required value`() {
        val orderListener = object : Subscriber() {
            var count = 0

            override fun subscribe() = Subscription.forEvent("order")
                .requireValue("color", "green")

            override fun receive(jsonMessage: JsonMessage) {
                count++
            }
        }

        """{ "@event_name": "order", "color": "blue" }"""
            .asMessage()
            .let(orderListener::onMessage)

        orderListener.count shouldBe 0

        """{ "@event_name": "order", "color": "green" }"""
            .asMessage()
            .let(orderListener::onMessage)

        orderListener.count shouldBe 1

        """{ "@event_name": "order", "color": "red" }"""
            .asMessage()
            .let(orderListener::onMessage)

        orderListener.count shouldBe 1
    }

    @Test
    fun `accepts only messages with any of required values`() {
        val orderListener = object : Subscriber() {
            var count = 0

            override fun subscribe() = Subscription.forEvent("order")
                .requireAnyValue("color", "green", "red")

            override fun receive(jsonMessage: JsonMessage) {
                count++
            }
        }

        """{ "@event_name": "order", "color": "blue" }"""
            .asMessage()
            .let(orderListener::onMessage)

        orderListener.count shouldBe 0

        """{ "@event_name": "order", "color": "green" }"""
            .asMessage()
            .let(orderListener::onMessage)

        orderListener.count shouldBe 1

        """{ "@event_name": "order", "color": "red" }"""
            .asMessage()
            .let(orderListener::onMessage)

        orderListener.count shouldBe 2
    }

    @Test
    fun `allows required value to be non-string`() {
        val orderListener = object : Subscriber() {
            var count = 0

            override fun subscribe() = Subscription.forEvent("order")
                .requireValue("amount", 10)
                .requireValue("price", 10.50)
                .requireValue("delivered", true)

            override fun receive(jsonMessage: JsonMessage) {
                count++
            }
        }

        """{ "@event_name": "order", "amount": 5, "price": 10.50, "delivered": true }"""
            .asMessage()
            .let(orderListener::onMessage)

        orderListener.count shouldBe 0

        """{ "@event_name": "order", "amount": 10, "price": 20.0, "delivered": true }"""
            .asMessage()
            .let(orderListener::onMessage)

        orderListener.count shouldBe 0

        """{ "@event_name": "order", "amount": 10, "price": 10.50, "delivered": false }"""
            .asMessage()
            .let(orderListener::onMessage)

        orderListener.count shouldBe 0

        """{ "@event_name": "order", "amount": 10, "price": 10.50, "delivered": true }"""
            .asMessage()
            .let(orderListener::onMessage)

        orderListener.count shouldBe 1
    }

    @Test
    fun `ignores rejected value`() {
        val orderListener = object : Subscriber() {
            var count = 0

            override fun subscribe() = Subscription.forEvent("order")
                .rejectValue("color", "green")

            override fun receive(jsonMessage: JsonMessage) {
                count++
            }
        }

        """{ "@event_name": "order", "color": "blue" }"""
            .asMessage()
            .let(orderListener::onMessage)

        orderListener.count shouldBe 1

        """{ "@event_name": "order", "color": "green" }"""
            .asMessage()
            .let(orderListener::onMessage)

        orderListener.count shouldBe 1

        """{ "@event_name": "order", "color": "red" }"""
            .asMessage()
            .let(orderListener::onMessage)

        orderListener.count shouldBe 2
    }

    private fun String.asMessage(topic: String = "testTopic", key: String = UUID.randomUUID().toString()) = JsonMessage(
        json = objectMapper.readTree(this),
        metadata = EventMetadata(
            topic = topic,
            kafkaEvent = KafkaEvent(key, this),
            opprettet = ZonedDateTime.now(),
            lest = ZonedDateTime.now()
        )
    )
}
