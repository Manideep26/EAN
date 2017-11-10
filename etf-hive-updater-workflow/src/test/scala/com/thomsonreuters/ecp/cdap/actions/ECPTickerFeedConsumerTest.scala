package com.thomsonreuters.ecp.cdap.actions

import com.thomsonreuters.ecp.AppHelper.makeAppConfig
import com.thomsonreuters.ecp.cdap.actions.ECPTickerFeedConsumer._
import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.{FlatSpec, Matchers}

class ECPTickerFeedConsumerTest extends FlatSpec with EmbeddedKafka with Matchers{

  info("Starting ConsumerTest...")

  //  TODO: Update unit tests to mock Kafka zookeeper instead of using local zookeeper
  //implicit val config = EmbeddedKafkaConfig(kafkaPort = 7020, zooKeeperPort = 7021)

  val conf = makeAppConfig()
  val broker = conf.getString("kafka.broker")
  val controlTopic = conf.getString("kafka.TopicControl")
  val dataTopic = conf.getString("kafka.TopicData")
  val logsTopic = conf.getString("kafka.TopicLog")
  val errorsTopic = conf.getString("kafka.TopicError")


  "Sending and Consuming Control" should "work" in {
    // FIXME Can't call Producer methods from here anymore.
    // FIXME Should also be testing with ASSERTIONS, not just printing.

    // sendReadyEcpControlMessage(broker,"test_control_ready_file.ready")
    // printECPTickerFeedControlTopic
  }

//  "Sending and Consuming Data" should "work" in {
//    sendReadyFileMessage(broker, dataTopic, "test_data")
//    readECPTickerFeedDataTopic
//  }
//
//  "Sending and Consuming Logs" should "work" in {
//    sendReadyFileMessage(broker, logsTopic, "test_log")
//    readECPTickerFeedLogTopic
//  }
//
//  "Sending and Consuming Errors" should "work" in {
//    sendReadyFileMessage(broker, errorsTopic, "test_error")
//    readECPTickerFeedErrorTopic
//  }

}
