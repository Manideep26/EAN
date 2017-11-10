package com.thomsonreuters.ecp.exceptions

import org.scalatest.{FlatSpec, Matchers}

class ECPTickerFeedExceptionTest  extends FlatSpec with Matchers {

  "ceg exception" should "have default values" in {
    val e = new InternalException()
    e.getMessage() should be (null)
    e.getCause() should be (null)
  }

  "ceg exception" should "preserve cause" in {
    val e = new Exception("cause message")
    val ce = new InternalException("ceg message", cause = e)
    ce.getMessage() should be ("ceg message")
    ce.getCause() should be (e)
  }

  "ceg exception" should "use cause message if message is null" in {
    val e = new Exception("cause message")
    val ce = new InternalException(cause = e)
    ce.getMessage should be ("java.lang.Exception: cause message")
  }
}
