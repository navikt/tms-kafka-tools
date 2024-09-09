package no.nav.tms.kafka.message.replay

import no.nav.tms.kafka.application.RecordBroadcaster
import org.apache.kafka.common.TopicPartition
import java.lang.Integer.min
import java.time.Duration

internal class MessageReplay(
    private val broadcaster: RecordBroadcaster,
    private val consumerFactory: ConsumerFactory
) {

    suspend fun replayMessages(request: ReplayRequest): Int {
        val maxPoll = min(500, request.count)

        val consumer = consumerFactory.createConsumer(maxPoll)

        val topicPartition = TopicPartition(request.topic, request.partition)

        var readRecords = 0

        consumer.use {
            consumer.assign(listOf(topicPartition))
            consumer.seek(topicPartition, request.offset)

            while (readRecords < request.count) {
                val consumerRecords = consumer.poll(Duration.ofSeconds(1))

                if (consumerRecords.isEmpty) {
                    break
                }

                consumerRecords.forEach {
                    broadcaster.broadcastRecord(it)

                    readRecords++
                }
            }
        }

        return readRecords
    }
}
