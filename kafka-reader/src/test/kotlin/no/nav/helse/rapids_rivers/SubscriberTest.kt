package no.nav.helse.rapids_rivers

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.time.measureTime


class SubscriptionStressTest {
    val listener = JsonListener()

    val toHandle = (0..100000).map { NewJsonMessage(json(), null) }

    @Test
    fun `test handling 100000 messages`() {
        val duration = measureTime {
            toHandle.forEach(listener::onMessage)
        }

        println(duration)
    }
}

class Counter {
    private val counter: MutableMap<String, Int> = mutableMapOf()

    fun inc(name: String) {
        counter.compute(name) { _, count ->
            count?.inc() ?: 1
        }
    }

    fun count() = counter
}

class JsonListener: Subscriber() {
    val drinks = Counter()
    val food = Counter()
    var greens = 0;

    override fun subscribe() = Subscription.forEvent("testEvent")
        .requireFields("food", "drink")
        .optionalFields("venue", "date")
        .rejectValue("cancelled", true)
        .requireValue("color", "green")

    override fun receive(jsonMessage: NewJsonMessage) {
        food.inc(jsonMessage["food"].asText())
        drinks.inc(jsonMessage["drink"].asText())
        greens++
    }
}

private val objectMapper = jacksonObjectMapper()

private val foods = listOf("pizza", "ham", "apples", "banana", "potatoes")
private val drinks = listOf("soda", "wine", "water", "beer")
private val colors = listOf("red", "green", "blue")

private fun json() = """
{
    "@event_name": "${if (roll(95)) "testEvent" else "other"}",
    "food": "${foods.random()}",
    "drink": "${drinks.random()}",
    "color": "${colors.random()}",
    "cancelled": ${if (roll(10)) "true" else "false"},
    "date": "2024-04-01"
}
""".let { objectMapper.readTree(it) }



private fun roll(chancePercent: Int) = Random.nextInt(100) < chancePercent

private fun <T> List<T>.random() = this[Random.nextInt(size)]

