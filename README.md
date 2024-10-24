# Team min-side kafka tools

Bibliotek for å forenkle lesing fra- eller produsering til kafka

## kafka-application

Setter opp en ktor app som leser fra kafka ut av boksen. 

En kafka-application basert på dette biblioteket leser meldinger fra kafka og videreformidler godkjente meldinger til et
vilkårlig `Subscriber`-objekter. Hvilke Subscribere som behandler hvilke eventer bestemmes av hver enkel Subscribers `Subscription`.

### Kafka-melding

For at et kafka-event skal være godkjent må det oppfylle følgende krav:

 - Innholdet i kafka-eventet er en json-string (I.E. ingen AVRO eller andre binærformat).
 - Eventet har et navn-felt `@event_name` (eller et alternativ basert på config) definert på toppen av json-objektet.
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

 - Hvilken type event som skal leses basert på eventets navn. Som default er dette feltet `@event_name`. En Subscriber kan kun lytte på én type eventer.
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

### Eventer og navn

Det er forventet at alle kafka-eventer har ett felt som indikerer eventets type. Innholdet i dette feltet må 
være en String. Navnet på feltet er som default `@event_name`, men kan endres ved i konfigurasjonen som følger:

```kotlin
fun main() {
    KafkaApplication.build {
        kafkaConfig {
            eventNameFields("@alternativ", "@annet")
            
            ...
        }
        
        ...
    }.start()
}
```

Det er ingen spesielle regler hva feltet kan hete, men konvensjonen er at det er prefikset med '@'.

Dersom en spesifiserer flere felt som mulig eventnavn, og et event har flere slike felt, er det feltet som
ble satt først i lista som er gjeldende eventnavn. I eksempelet over vil det være `@alternativ`.

I utgangspunktet regnes eventnavn som metadata, og vil ikke bli med i json-objektet i JsonMessage. Dersom en ønsker
å inkludere dette feltet, kan en i tillegg spesifisere det i Subscription som om det var et vanlig felt:

```kotlin
class SomeSubscriber : Subscriber() {
    override fun subscription() = Subscription.forEvent("name")
        .withFields("@event_name")
    
    override suspend fun receive(jsonMessage: JsonMessage) {
        consumeName(jsonMessage["@event_name"])
    }
}
```

### Feilhåndtering

Feil som oppstår under lesing fra kafka behandles ulikt basert på årsak:

 - Hvis det ligger inhhold på feil format (E.G. feilaktig json eller binærdata) på kafka-topicet vil appen hoppe over disse eventene. 
 - Hvis en Subscriber kaster et MessageException vil dette logges og appen lese videre.
 - Hvis det kastes en uventet exception vil appen stoppe videre lesing fra kafka. 

### Message channel

Kafka-application eksponerer sin broadcast-channel via plugin `MessageChannel` som kan hentes fra Ktors `Application` som følger: `application.plugin(MessageChannel).channel.broadcaster`. 

Denne kan en bruke som en 'bakdør' til å sende eventer til alle insallerte subscribere programatisk, uten at det kommer fra kafka.

I skrivende stund godtar `RecordBroadcaster` kun `ConsumerRecords`.

Eksempel:

```kotlin
fun Application.example() {
    val broadCaster = plugin(MessageChannel).channel.broadcaster
    
    val broadcaster.broadcastRecord(...)
}
```

### Message replay

Biblioteket tilbyr også en måte å repetere bestemte kafka-eventer uten å måtte spille av alt på nytt fra starten eller et gitt offset.

Dette gjør en ved å installere plugin `MessageReplayApi`: 

```kotlin
fun Application.setup() {
    install(MessageReplayApi) {
        requireAuthentication = false   
    }
}
```

Deretter kan en sende POST-kall til endepunktet `/message/replay` med format (eksempeldata):

```json
{
  "topic": "order-topic-v1",
  "partition": 0,
  "offset":  500,
  "count": 10
}
```

I eksempelet over vil kafka-application forsøke å lese opp til 10 eventer på nytt fra topic 'order-topic-v1', partisjon 0, fra og med offset 500.

Dersom er lavere en tidligste offset, leses 10 eventer derfa. Hvis offsettet er høyere enn seneste offset leses ingen eventer.
