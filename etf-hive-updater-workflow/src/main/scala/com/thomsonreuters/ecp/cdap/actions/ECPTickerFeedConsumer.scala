package com.thomsonreuters.ecp.cdap.actions

import java.util
import java.util.Properties

import com.thomsonreuters.ecp.AppHelper
import com.tr.ecp.message.ECPMessage
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

trait ECPTickerFeedConsumerTraits{


  val logger = LoggerFactory.getLogger(getClass)
  val conf = AppHelper.makeAppConfig()
  val props = new Properties()
  val broker = conf.getString("kafka.broker")
  val groupId = conf.getString("kafka.groupID")
  val keySerializer = conf.getString("kafka.keySerializer")
  val valueSerializer = conf.getString("kafka.valueSerializer")
  val keyDeserializer = conf.getString("kafka.keyDeserializer")
  val valueDeserializer = conf.getString("kafka.valueDeserializer")
  val partitionAssignmentStrategy = conf.getString("kafka.partitionAssignmentStrategy")
  val zookeeperSessionTimeoutMS = conf.getString("kafka.zookeeperSessionTimeoutMS")
  val zookeeperSyncTimeMS = conf.getString("kafka.zookeeperSyncTimeMS")
  val autoCommitIntervalMS = conf.getString("kafka.autoCommitIntervalMS")
  val controlTopic = conf.getString("kafka.TopicControl")
  val dataTopic = conf.getString("kafka.TopicData")
  val errorTopic = conf.getString("kafka.TopicError")
  val logTopic = conf.getString("kafka.TopicLog")


//  TODO: determine what is needed. bootstrap or zookeeper
  props.put("bootstrap.servers", broker)
  props.put("key.deserializer", keyDeserializer)
  props.put("value.deserializer", valueDeserializer)
  props.put("group.id", groupId)
//  props.put("partition.assignment.strategy",partitionAssignmentStrategy)
//  props.put("zookeeper.session.timeout.ms", zookeeperSessionTimeoutMS)
//  props.put("zookeeper.sync.time.ms", zookeeperSyncTimeMS)
//  props.put("auto.commit.interval.ms", autoCommitIntervalMS)

  val consumer = new KafkaConsumer(props)
//  consumer.subscribe(util.Arrays.asList(controlTopic, dataTopic, errorTopic, logTopic))
}

object ECPTickerFeedConsumer extends App with ECPTickerFeedConsumerTraits {

  def readECPTickerFeedControlTopic: ConsumerRecords[Nothing, Nothing] = {

    consumer.subscribe(util.Arrays.asList(controlTopic))
    logger.info(s"Subscribed to $controlTopic on $broker")

    val records = consumer.poll(zookeeperSessionTimeoutMS.toLong)

    consumer.unsubscribe()
    logger.info(s"Unsubscribed from $controlTopic on $broker")

    return records
  }

  def printECPTickerFeedControlTopic ={
    for (record <- readECPTickerFeedControlTopic.asScala) {
      val message = ECPMessage.deserializeDefaultHeader(record.value.toString).toString
      logger.info(message)
      println(record.value.toString)
    }
  }

  def readECPTickerFeedDataTopic: ConsumerRecords[Nothing, Nothing] = {

    consumer.subscribe(util.Arrays.asList(dataTopic))
    logger.info(s"Subscribed to $dataTopic on $broker")

    val records = consumer.poll(zookeeperSessionTimeoutMS.toLong)

    consumer.unsubscribe()
    logger.info(s"Unsubscribed from $dataTopic on $broker")

    return records
  }

  def printECPTickerFeedDataTopic ={
    for (record <- readECPTickerFeedDataTopic.asScala) {
      val message = ECPMessage.deserializeDefaultHeader(record.value.toString).toString
      logger.info(message)
      println(record.value.toString)
    }
  }

  def readECPTickerFeedErrorTopic: ConsumerRecords[Nothing, Nothing] = {

    consumer.subscribe(util.Arrays.asList(errorTopic))
    logger.info(s"Subscribed to $errorTopic on $broker")

    val records = consumer.poll(zookeeperSessionTimeoutMS.toLong)

    consumer.unsubscribe()
    logger.info(s"Unsubscribed from $errorTopic on $broker")

    return records
  }

  def printECPTickerFeedErrorTopic ={
    for (record <- readECPTickerFeedErrorTopic.asScala) {
      val message = ECPMessage.deserializeDefaultHeader(record.value.toString).toString
      logger.info(message)
      println(record.value.toString)
    }
  }

  def readECPTickerFeedLogTopic: ConsumerRecords[Nothing, Nothing] = {

    consumer.subscribe(util.Arrays.asList(logTopic))
    logger.info(s"Subscribed to $logTopic on $broker")

    val records = consumer.poll(zookeeperSessionTimeoutMS.toLong)

    logger.info(s"Unsubscribed from $logTopic on $broker")
    consumer.unsubscribe()

    return records
  }

  def printECPTickerFeedLogTopic ={
    for (record <- readECPTickerFeedLogTopic.asScala) {
      val message = ECPMessage.deserializeDefaultHeader(record.value.toString).toString
      logger.info(message)
      println(record.value.toString)
    }
  }

  //TODO: Not currently used but will breack down Kafka message into mapped object
  def parseMessage(message: String): Option[Map[String, String]] = {
    def parsePropertiesSubMap(map: Map[String, Any]): Map[String, String] = {
      map.mapValues {
        case list: Iterable[String] => list.mkString(",")
        case d: Double if d - d.toLong == 0 => d.toLong.toString
        case t => t.toString
      }
    }
    try {
      if (message == null) {
        //TODO: throw error message possibly
        logger.warn("Kafka Message Empty")
      }
      val parsed = ECPMessage.deserializeDefaultHeader(message)
      val spProps = parsed.getSpProperties
      val ourProps = if (spProps != null) spProps.getAdditionalProperties else new util.HashMap[String, AnyRef]()
      val tickerProps = scala.collection.immutable.Map(ourProps.asScala.toSeq: _*)
      Option(parsed.getSpEventType) match {
        case None => None
        case Some(eventType) => eventType match {
          case "ready" => Option(parsePropertiesSubMap(tickerProps))
          case _ => None
        }
      }
    }
    //TODO: add catch for error handling
  }

  logger.info("Closing ECP Ticker Feed Consumer")
  consumer.close()
}
