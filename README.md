# Team min-side kafka tools

Bibliotek for å forenkle lesing fra- eller produsering til kafka

## kafka-application

Setter opp en ktor app som leser fra kafka ut av boksen. 

En kafka-application basert på dette biblioteket leser meldinger fra kafka og videreformidler godkjente meldinger til et
vilkårlig `Subscriber`-objekter. Hvilke Subscribere som behandler hvilke eventer bestemmes av hver enkel Subscribers `Subscription`.

### Kafka-melding

For at et kafka-event skal være godkjent må det oppfylle følgende krav:

 - Innholdet i kafka-eventet er en json-string (I.E. ingen AVRO eller andre binærformat).
 - Eventet har et felt `@event_name` definert på toppen av json-objektet.
 - Eventets nøkkel har ingen meningsbærende informasjon.

Eksempel på gyldig innhold for event:

```json
{
  "@event_name": "orderConfirmed",
  "item": {
    "id": 123,
    "name": "Apple"
  },
  "amount": 10
}
```

### Subscriber og Subscription

En Subscriber konsumerer kafka-meldinger med innhold som matcher dens Subscription. En Subscription er et sett med regler
som definerer et gyldig json-objekt i sin kontekst, og garanterer at kun slike eventer blir prosessert av denne Subscriberen.

Følgende regler kan brukes i Subscription:

 - Hvilken type event som skal leses basert på feltet `@event_name`. En Subscriber kan kun lytte på én type eventer.
 - Hvilke felt skal finnes på toppnivå av json.
 - Hvilke verdier på disse feltene som er godkjente eller ugyldige.
 - Egendefinert filter på json-node. 

Felt som finnes i det originale eventet fra kafka, men som ikke nevnes i en subscription, blir ignorert.

Eksempel på oppsett av Subscriber:

```kotlin
class OrderConfimedSubscriber : Subscriber() {
    override fun subscription() = Subscription.forEvent("orderConfirmed")
        .withFields("item")
        .withoutValue("amount", 0)
    
    val itemIdsSeen = mutableSetOf<Long>()
    var totalOrdered = 0

    override suspend fun receive(jsonMessage: JsonMessage) {
        itemsOrdered += jsonMessage["amount"].asInt()
        itemsSeen.add(jsonMessage["item"]["id"].asLong())
    }
}
```

### Top-level oppsett av kafka-application

Bruk av biblioteket forutsetter at appen kjører på NAIS med 
kafka enabled i yaml, eller at følgende miljøvariabler er konfigurert manuelt:

 - `KAFKA_BROKERS` (Url til kakfka bootstrap-server. Helt nødvendig i alle tilfeller)
 - `KAFKA_TRUSTSTORE_PATH` og `KAFKA_CREDSTORE_PASSWORD` (Auentiseringsinfo. Påkrevd som default. Ikke nødvendig dersom en skrur av ssl for eksempel ved lokal kjøring)

Biblioteket krever ytterligere to ting for å kjøre:

 - Minst ett kafka topic å lytte på
 - Group id, som brukes for å konsumere topic.

Eksempel på minimumsoppsett for en app som lytter på meldinger på topic `test-topic-v1`. 

```kotlin
fun main() {
    KafkaApplication.build {
        kafkaConfig {
            groupId = "group-1"
            readTopic("test-topic-v1")
        }
    }.start()
}
```

Mer praktisk eksempel på app som lytter på topic `order-topic-v1` og behandler bestemte eventer.

```kotlin
fun main() {
    KafkaApplication.build {
        kafkaConfig {
            groupId = "my-app-001"
            readTopic("order-topic-v1")
        }
        
        ktorModule {
            someApi()
        }
        
        subsriber {
            OrderConfirmedSubscriber()
        }

        onStartup {
            startBatchJob()
        }

        onShutdown {
            gracefullyStopBatchJob()
        }
    }.start()
}
```

### Feilhåndtering

Feil som oppstår under lesing fra kafka behandles ulikt basert på årsak:

 - Hvis det ligger inhhold på feil format (E.G. feilaktig json eller binærdata) på kafka-topicet vil appen hoppe over disse eventene. 
 - Hvis en Subscriber kaster et MessageException vil dette logges og appen lese videre.
 - Hvis det kastes en uventet exception vil appen stoppe videre lesing fra kafka. 
