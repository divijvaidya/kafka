/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tiered.storage

import kafka.controller.{KafkaController, ReplicaAssignment}
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

import scala.collection.Seq
import scala.util.{Failure, Success, Try}

class UncleanLeaderElectionAndTieredStorageTest extends TieredStorageTestHarness {
  private val (leader, follower, _, topicA, p0, p1) = (0, 1, 2, "topicB", 0, 1)

  override protected def brokerCount: Int = 2

  override protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit = {
    val assignment = Map(p0 -> Seq(leader, follower))
    val tp0 = new TopicPartition(topicA, p0)

    builder
      .createTopic(topicA, partitionsCount = 1, replicationFactor = 2, maxBatchCountPerSegment = 1, assignment)
      .produce(topicA, p0, ("k1", "v1"))
      .produce(topicA, p0, ("k2", "v2"), ("k3", "v3"))
      .withBatchSize(topicA, p0, 1)
      .expectSegmentToBeOffloaded(leader, topicA, p0, baseOffset = 0, ("k1", "v1"));

    Try(builder.complete()) match {
      case Success(actions) =>
        contextOpt.foreach(context => actions.foreach(_.execute(context)))

      case Failure(e) =>
        throw new AssertionError("Could not build test specifications. No test was executed.", e)
    }

    val tp1 = new TopicPartition(topicA, p1)
    val expandedAssignment = Map(
      tp0 -> ReplicaAssignment(Seq(0, 1), Seq(), Seq()),
      tp1 -> ReplicaAssignment(Seq(0, 1), Seq(), Seq()))

    var topicIdAfterPartitionExpansion = zkClient.getTopicIdsForTopics(Set(tp0.topic())).get(tp0.topic())
    assertTrue(topicIdAfterPartitionExpansion.nonEmpty)
    System.out.println("oldTopic:" + topicIdAfterPartitionExpansion)
    val topicIdOld = topicIdAfterPartitionExpansion;

    val initZkEpoch = KafkaController.InitialControllerEpochZkVersion + 1
    zkClient.setTopicAssignment(tp0.topic, /*empty topic Id*/ Option.empty, expandedAssignment, initZkEpoch)

    Thread.sleep(10)
    topicIdAfterPartitionExpansion = zkClient.getTopicIdsForTopics(Set(tp0.topic())).get(tp0.topic())
    assertTrue(topicIdAfterPartitionExpansion.isEmpty)

    System.out.println("Controller beginning shutdown")
    val controller1 = getController()
    controller1.shutdown()
    controller1.awaitShutdown()
    System.out.println("Controller is shutdown")

    topicIdAfterPartitionExpansion = zkClient.getTopicIdsForTopics(Set(tp0.topic())).get(tp0.topic())
    System.out.println("newTopicId:" + topicIdAfterPartitionExpansion)
    val topicIdnew = topicIdAfterPartitionExpansion;
    Thread.sleep(50)
    assertTrue(topicIdAfterPartitionExpansion.nonEmpty)

    System.out.println("controller startup")
    controller1.startup()
    TestUtils.generateAndProduceMessages(servers, tp0.topic(), 5)

    assertEquals(topicIdOld, topicIdnew)
  }
}
