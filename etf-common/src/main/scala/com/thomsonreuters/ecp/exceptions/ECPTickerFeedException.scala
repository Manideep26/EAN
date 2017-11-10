package com.thomsonreuters.ecp.exceptions

import scala.runtime.ScalaRunTime

abstract class ECPTickerFeedException(message: String = null,
                                      cause: Throwable = null)
  extends RuntimeException(ECPTickerFeedException.defaultMessage(message, cause), cause)
    with Product with Serializable {
  override def toString = ScalaRunTime._toString(this)
}

object ECPTickerFeedException {
  def defaultMessage(message: String, cause: Throwable) = {
    if (message != null) message
    else if (cause != null) cause.toString()
    else null
  }
}

case class InternalException(message: String = null,
                                cause: Throwable = null)
  extends ECPTickerFeedException(message, cause)

case class InternalKafkaException(message: String = null,
                                     cause: Throwable = null)
  extends ECPTickerFeedException(message, cause)

