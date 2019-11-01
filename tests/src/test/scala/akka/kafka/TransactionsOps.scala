/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka
import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.PartitionOffset
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Transactional}
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.scalatest.TestSuite

import scala.collection.immutable
import scala.concurrent.duration.{Duration, FiniteDuration}

trait TransactionsOps extends TestSuite {
  def transactionalCopyStream(
      consumerSettings: ConsumerSettings[String, String],
      producerSettings: ProducerSettings[String, String],
      sourceTopic: String,
      sinkTopic: String,
      transactionalId: String,
      idleTimeout: FiniteDuration,
      restartAfter: Option[Int] = None
  ): Source[ProducerMessage.Results[String, String, PartitionOffset], Control] =
    Transactional
      .source(consumerSettings, Subscriptions.topics(sourceTopic))
      .zip(Source.unfold(1)(count => Some((count + 1, count))))
      .map {
        case (msg, count) =>
          if (restartAfter.exists(restartAfter => count >= restartAfter))
            throw new Error("Restarting transactional copy stream")
          msg
      }
      .idleTimeout(idleTimeout)
      .map { msg =>
        ProducerMessage.single(new ProducerRecord[String, String](sinkTopic, msg.record.value), msg.partitionOffset)
      }
      .via(Transactional.flow(producerSettings, transactionalId))

  def transactionalPartitionedCopyStream(
      consumerSettings: ConsumerSettings[String, String],
      producerSettings: ProducerSettings[String, String],
      sourceTopic: String,
      sinkTopic: String,
      transactionalId: String,
      idleTimeout: FiniteDuration,
      maxPartitions: Int,
      restartAfter: Option[Int] = None
  ): Source[ProducerMessage.Results[String, String, PartitionOffset], Control] =
    Transactional
      .partitionedSource(consumerSettings, Subscriptions.topics(sourceTopic))
      .flatMapMerge(
        maxPartitions, {
          case (_, source) =>
            val results: Source[ProducerMessage.Results[String, String, PartitionOffset], NotUsed] = source
              .zip(Source.unfold(1)(count => Some((count + 1, count))))
              .map {
                case (msg, count) =>
                  if (restartAfter.exists(restartAfter => count >= restartAfter))
                    throw new Error("Restarting transactional copy stream")
                  msg
              }
              .idleTimeout(idleTimeout)
              .map { msg =>
                ProducerMessage.single(new ProducerRecord[String, String](sinkTopic, msg.record.value),
                                       msg.partitionOffset)
              }
              .via(Transactional.flow(producerSettings, transactionalId))
            results
        }
      )

  def checkForDuplicates(values: immutable.Seq[(Long, String)], expected: immutable.IndexedSeq[String]): Unit =
    withClue("Checking for duplicates: ") {
      val duplicates = values.map(_._2) diff expected
      if (duplicates.nonEmpty) {
        val duplicatesWithDifferentOffsets = values
          .filter {
            case (_, value) => duplicates.contains(value)
          }
          .groupBy(_._2) // message
          .map(kv => (kv._1, kv._2.map(_._1))) // keep offset
          .filter {
            case (_, offsets) => offsets.distinct.size > 1
          }

        if (duplicatesWithDifferentOffsets.nonEmpty) {
          fail(s"Got ${duplicates.size} duplicates. Messages and their offsets: $duplicatesWithDifferentOffsets")
        } else {
          println("Got duplicates, but all of them were due to rebalance replay when counting")
        }
      }
    }

  def checkForMissing(values: immutable.Seq[(Long, String)], expected: immutable.IndexedSeq[String]): Unit =
    withClue("Checking for missing: ") {
      val missing = expected diff values.map(_._2)
      if (missing.nonEmpty) {
        val continuousBlocks = missing
          .scanLeft(("-1", 0)) {
            case ((last, block), curr) => if (last.toInt + 1 == curr.toInt) (curr, block) else (curr, block + 1)
          }
          .tail
          .groupBy(_._2)
        val blockDescription = continuousBlocks
          .map { block =>
            val msgs = block._2.map(_._1)
            s"Missing ${msgs.size} in continuous block, first ten: ${msgs.take(10)}"
          }
          .mkString(" ")
        fail(s"Did not get ${missing.size} expected messages. $blockDescription")
      }
    }

  def valuesProbeConsumer(
      settings: ConsumerSettings[String, String],
      topic: String
  )(implicit actorSystem: ActorSystem, mat: Materializer): TestSubscriber.Probe[String] =
    offsetValueSource(settings, topic)
      .map(_._2)
      .runWith(TestSink.probe)

  def offsetValueSource(settings: ConsumerSettings[String, String],
                        topic: String): Source[(Long, String), Consumer.Control] =
    Consumer
      .plainSource(settings, Subscriptions.topics(topic))
      .map(r => (r.offset(), r.value()))

  def withProbeConsumerSettings(settings: ConsumerSettings[String, String],
                                groupId: String): ConsumerSettings[String, String] =
    settings
      .withGroupId(groupId)
      .withProperties(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")

  def withTestProducerSettings(settings: ProducerSettings[String, String]): ProducerSettings[String, String] =
    settings
      .withCloseTimeout(Duration.Zero)
      .withProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

  def withTransactionalProducerSettings(settings: ProducerSettings[String, String]): ProducerSettings[String, String] =
    settings
      .withParallelism(20)
      .withCloseTimeout(Duration.Zero)
}
