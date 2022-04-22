package integration.kafka.tiered.storage

import kafka.tiered.storage.{TieredStorageTestBuilder, TieredStorageTestHarness}
import org.apache.kafka.common.errors.UnsupportedActionForTopicException

class DisableDeleteRecordOnTopicWithRemoteLogEnabled extends TieredStorageTestHarness {
    private val (broker0, broker1, topicA, p0) = (0, 1, "topicA", 0)

    /* Cluster of two brokers */
    override protected def brokerCount: Int = 2

    override protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit = {
        val assignment = Map(p0 -> Seq(broker0, broker1))
        builder
          .createTopic(topicA, partitionsCount = 1, replicationFactor = 2, maxBatchCountPerSegment = 1, assignment)
          // send records to partition 0
          .produce(topicA, p0, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
          .deleteRecords(topicA, p0, beforeOffset = 1, Some(classOf[UnsupportedActionForTopicException]))
    }
}
