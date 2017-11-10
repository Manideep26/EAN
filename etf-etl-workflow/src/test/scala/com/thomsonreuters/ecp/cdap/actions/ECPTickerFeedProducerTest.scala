package com.thomsonreuters.ecp.cdap.actions

import com.thomsonreuters.ecp.AppHelper.makeAppConfig
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{FlatSpec, Matchers}

class ECPTickerFeedProducerTest extends FlatSpec with EmbeddedKafka with Matchers {

  info("Starting ProducerTest...")

  //  TODO: Update unit tests to mock Kafka zookeeper instead of using local zookeeper
  implicit val config = EmbeddedKafkaConfig(kafkaPort = 7000, zooKeeperPort = 7001)

  import ECPTickerFeedProducer._

  val conf = makeAppConfig()
  val broker = conf.getString("kafka.broker")
  val topic = conf.getString("kafka.TopicControl")

  "sending messages" should "work" in {
    sendDefaultEcpMessage(broker,topic)
    sendReadyFileMessage(broker, topic, "test_ready_file.ready")
  }

}
