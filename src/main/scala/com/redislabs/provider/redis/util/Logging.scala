package com.redislabs.provider.redis.util

import org.slf4j.{Logger, LoggerFactory}

/**
  * @author The Viet Nguyen
  */
trait Logging {

  @transient private var _logger: Logger = _

  protected def loggerName: String =
    this.getClass.getName.stripSuffix("$")

  protected def logger: Logger = {
    if (_logger == null) {
      _logger = LoggerFactory.getLogger(loggerName)
    }
    _logger
  }

  def logInfo(msg: => String): Unit = {
    if (logger.isInfoEnabled) {
      _logger.info(msg)
    }
  }
}
