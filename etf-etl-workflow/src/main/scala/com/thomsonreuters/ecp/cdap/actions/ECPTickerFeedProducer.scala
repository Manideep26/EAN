package com.thomsonreuters.ecp.cdap.actions

import com.thomsonreuters.ecp.AppHelper.makeAppConfig
import com.thomsonreuters.ecp.exceptions.InternalException
import com.tr.ecp.message.domain.SpProperties
import com.tr.ecp.message.{KafkaMessenger, MsgContext}
import org.slf4j.LoggerFactory

trait ECPTickerFeedProducerTraits {

  val eventID: String = java.util.UUID.randomUUID.toString
  val eventSourceID: String = java.util.UUID.randomUUID.toString
  val timeStamp = new java.util.Date
  val logger = LoggerFactory.getLogger(getClass)
  val conf = makeAppConfig()
  val controlTopic = conf.getString("kafka.TopicControl")
  val dataTopic = conf.getString("kafka.TopicData")
  val errorTopic = conf.getString("kafka.TopicLog")
  val logTopic = conf.getString("kafka.TopicError")
  val kafkaBroker = conf.getString("kafka.broker")
}

object ECPTickerFeedProducer extends App with ECPTickerFeedProducerTraits{


  def sendFileMessageToEcpControl(spMessage: String) {

    logger.info(s"Sending ReadyFile Message to $controlTopic on $kafkaBroker")
    val messenger = new KafkaMessenger(new ProducerContext(conf.getString("kafka.TopicControl")),kafkaBroker)
    val message = messenger.controlHeader()

    message.setSpMessage(spMessage)
    logger.info(message.toString)
    messenger.sendCustomControlMsg("key", message)
    messenger.close
  }

  def sendReadyEcpControlMessage(broker: String, spMessage: String) {

    logger.info(s"Sending ReadyFile Message to $controlTopic on $broker")
    val messenger = new KafkaMessenger(new ProducerContext(conf.getString("kafka.TopicControl")),broker)
    val message = messenger.controlHeader()
//    val spProps = new SpProperties()

//    msgProps.foreach(x => spProps.setAdditionalProperty(x._1, x._2))
//    message.setSpProperties(spProps)
    message.setSpMessage(spMessage)
//    if (msgProps.contains("sp-eventType")) message.setSpEventType(msgProps("sp-eventType"))
    logger.info(message.toString)
    messenger.sendCustomControlMsg("key", message)
    messenger.close
  }

  def sendEcpErrorMessage(broker: String, spMessage: String) {

    logger.info(s"Sending ReadyFile Message to $errorTopic on $broker")
    val messenger = new KafkaMessenger(new ProducerContext(conf.getString("kafka.TopicError")),broker)
    //TODO: add throwable error for message
    val message = messenger.errorHeader(null)
    //    val spProps = new SpProperties()

    //    msgProps.foreach(x => spProps.setAdditionalProperty(x._1, x._2))
    //    message.setSpProperties(spProps)
    message.setSpMessage(spMessage)
    //    if (msgProps.contains("sp-eventType")) message.setSpEventType(msgProps("sp-eventType"))
    logger.info(message.toString)
    messenger.sendCustomControlMsg("key", message)
    messenger.close
  }

  def sendReadyFileMessage(broker: String, topic: String, message: String) {

    val msgProps = Map(
      ("eventSchemaVersion", "4.0"),
      ("timestamp", timeStamp.toString),
      ("sp-eventSourceUUID", eventSourceID),
      ("sp-eventSeverity", "ok"),
      ("sp-eventType", "ready"),
      ("sp-message", message)
    )

    sendEcpControlMessage(broker, new ProducerContext(topic), msgProps)
  }

  def sendErrorMessage(broker: String, topic: String, errorMessage: String) {

//    var timeStamp = new java.util.Date

    val newMsg = Map(
      ("timestamp", timeStamp.toString),
      ("sp-eventType", "reply"),
      ("sp-eventSourceUUID", eventSourceID),
      ("sp-eventSchemaVersion", "4.0"),
      ("sp-message", errorMessage)
    )

    sendEcpErrorMessage(broker, topic, newMsg)
  }

  def sendEcpControlMessage(broker: String, topic: String,msgProps: Map[String, String]) {
    sendEcpControlMessage(broker, new ProducerContext(topic), msgProps)
  }

  def sendEcpControlMessage(broker: String, context: ProducerContext, msgProps: Map[String, String]) {
    val messenger = new KafkaMessenger(context,broker)
    val message = messenger.controlHeader()
    val spProps = new SpProperties()

    msgProps.foreach(x => spProps.setAdditionalProperty(x._1, x._2))
    message.setSpProperties(spProps)

    if (msgProps.contains("sp-eventType")) message.setSpEventType(msgProps("sp-eventType"))

    messenger.sendCustomControlMsg("key", message)
    messenger.close
  }

  def sendEcpDataMessage(broker: String, topic: String,msgProps: Map[String, String]) {
    sendEcpDataMessage(broker, new ProducerContext(topic), msgProps)
  }

  def sendEcpDataMessage(broker: String, context: ProducerContext, msgProps: Map[String, String]) {
    val messenger = new KafkaMessenger(context,broker)
    val message = messenger.dataHeader()
    val spProps = new SpProperties()

    msgProps.foreach(x => spProps.setAdditionalProperty(x._1, x._2))
//    message.setSpProperties(spProps)

    if (msgProps.contains("sp-eventType")) message.setSpEventType(msgProps("sp-eventType"))

    messenger.sendCustomDataMsg("key", message)
    messenger.close
  }

  def sendEcpErrorMessage(broker: String, topic: String, msgProps: Map[String, String]) {
    sendEcpErrorMessage(broker, new ProducerContext(topic), msgProps)
  }

  def sendEcpErrorMessage(broker: String, context: ProducerContext, msgProps: Map[String, String]) {
    val messenger = new KafkaMessenger(context,broker)
    val message = messenger.errorHeader(InternalException(msgProps.getOrElse("message", "An ECPTickerFeed Error occurred.")))
    val spProps = new SpProperties()

    messenger.sendCustomErrorMsg(null, message)
    messenger.close
  }

  def sendDefaultEcpMessage(broker: String, topic: String) {
    val messenger = new KafkaMessenger(new ProducerContext(topic),broker)

    messenger.sendDefaultControlMsg()
    messenger.close
  }

  private class ProducerContext(topic: String) extends MsgContext {
    override def getNamespace: String = topic.split("\\.").head

    override def getClassName: String = "cdap.actions.ECPTickerFeedProducer"

    override def getServiceName: String = "ECPTickerFeed"

    override def getApplicationName: String = "EAN"

    override def getEventSourceUUID: String = eventSourceID
  }
}
