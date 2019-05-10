/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.internal

import org.testcontainers.containers.KafkaContainer

object TestcontainersKafkaHelper {

  val ConfluentPlatformVersionDefault: String = "5.1.2" // contains Kafka 2.1.x
  private var kafkaContainer: KafkaContainer = _
  private var kafkaBootstrapServersInternal: String = _
  private var kafkaPortInternal: Int = -1

  private def requireStarted(): Unit =
    require(kafkaPortInternal != -1, "Testcontainers Kafka hasn't been started via `setUp`")

  def kafkaPort: Int = {
    requireStarted()
    kafkaPortInternal
  }

  def bootstrapServers: String = {
    TestcontainersKafkaHelper.requireStarted()
    kafkaBootstrapServersInternal
  }

  def startKafka(confluentPlatformVersion: String): String = startKafka(confluentPlatformVersion, Map.empty)

  def startKafka(confluentPlatformVersion: String, envVars: Map[String, String]): String = {
    if (kafkaPortInternal == -1) {
      val kafkaContainer = new KafkaContainer(confluentPlatformVersion)
      // transactions helper topic config must be overridden to support single brokers
      kafkaContainer.addEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
      kafkaContainer.addEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
      envVars.foreach {
        case (k, v) => kafkaContainer.addEnv(k, v)
      }
      kafkaContainer.start()
      kafkaBootstrapServersInternal = kafkaContainer.getBootstrapServers
      kafkaPortInternal =
        kafkaBootstrapServersInternal.substring(kafkaBootstrapServersInternal.lastIndexOf(":") + 1).toInt
    }
    kafkaBootstrapServersInternal
  }

  def stopKafka(): Unit =
    if (kafkaPortInternal != -1) {
      kafkaContainer.stop()
      kafkaPortInternal = -1
      kafkaContainer = null
    }
}
