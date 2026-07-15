# Kafka-producer utils

Bibliotek for med en rekke utils for oppretting av- og sending via kafka-producere.

## Producer builder

`KafkaProducerBuilder` Tilbyr to metoder for å initialisere en kafka-producer med generisk config. 

```kotlin
fun exampleWithStringProducer(record: ProducerRecord<String, String>) {
    val stringProducer = KafkaProducerBuilder.stringProducer()
    
    stringProducer.send(record)
}

fun exampleWithProducer(record: ProducerRecord<Long, String>) {
    val producer = KafkaProducerBuilder.producer(
        keySerializer = LongSerializer::class,
        valueSerializer = StringSerializer::class
    )
    
    stringProducer.send(record)
}
```

Dersom en ønsker å bruke avro-serialiserere, eller endre default config, kan en gjøre som følger:

```kotlin
fun exampleWithAvroProducer(record: ProducerRecord<String, AvroObject>) {
    val producer = KafkaProducerBuilder.producer(
        keySerializer = StringSerializer::class,
        valueSerializer = AvroSerializer::class
    ) {
        put(KafkaAvroSerializerConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO")
        put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, environment.kafkaSchemaRegistry)
        put(
            KafkaAvroSerializerConfig.USER_INFO_CONFIG,
            "${environment.kafkaSchemaRegistryUser}:${environment.kafkaSchemaRegistryPassword}"
        )

        put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 20000)
    }
}
```

### Standardverdier

Default config er basert på følgende miljøvariabler og kan overskrives etter behov:

 - brokers: KAFKA_BROKERS
 - clientId: NAIS_POD_NAME
 - truststorePath: KAFKA_TRUSTSTORE_PATH
 - keystorePath: KAFKA_KEYSTORE_PATH
 - credstorePassword: KAFKA_CREDSTORE_PASSWORD

## Producer send utils

`ProducerSendUtils` tilbyr flere funksjoner for kontrollert sending av kafka-records.

Alle funksjonene kan kaste `RetriableSendException` eller `FatalSendException` under sending. En `RetriableSendException`
betyr at årsaken til feil ved sending kan ha løst seg innen appen forsøker igjen. En `FatalSendException` indikerer en 
mer grunnleggende feil, som for eksempel feilaktig autentiseringsinfo.

### Synchronized send

Denne funksjonen sender en kafka-record og venter på status før den returnerer.

```kotlin
fun sendEvent(record: ProducerRecord<String, String>): Boolean {
    return try {
        producer.sendSynchronized(record)

        true
    } catch (rse: RetriableSendException) {
        log.warn { "Feil ved sending, forsøker igjen senere." }

        false
    } catch (fse: FatalSendException) {
        log.error { "Feil ved sending, avbryter prosessering." }
        throw fse
    }
}
```

### Batched send

Denne funskjonen åpner en batch der en kan sende flere kafka-records som synkroniseres samtidig. Selv om de sendes samtidig, 
trenger ikke status for hver enkel record være den samme. I en batch med 10 records kan 8 være vellykket og 2 feile. Det 
er derfor sterkt anbefalt å supplere en `onSuccess` funksjon for å kvittere ut hver enkel record individuelt.

```kotlin
fun processQueue(queueItems: List<Entry>) {
    producer.batched {
        queueItems.forEach { entry ->
            val record = entry.toRecord()
            
            sendInBatch(record) { // onSuccess
                database.completeEntry(entry)
            }
        }
    }
}
```

### Transactional send

Denne funksjonen åpner en kafka-transaksjon for å sende flere kafka records samtidig, i situasjoner der både input og output 
er et kafka-topic. Offset lest av consumer blir ikke committed før kafka bekrefter sending.  Slike transaksjoner kan ikke
brukes sammen med databasetransaksjoner eller tilsvarende.

```kotlin
fun pollAndProcessNext() {
    val inputRecords = consumer.poll(Duration.ofSeconds(1))

    producer.transactional(inputRecords, consumer) {
        inputRecords.forEach { record ->
            val outputRecord = record.toOutputRecord()
            
            sendInTransaction(outputRecord)
        }
    }
}
```
