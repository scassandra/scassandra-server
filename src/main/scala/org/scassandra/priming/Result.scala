package org.scassandra.priming

abstract class Result(val string: String)

case object Success extends Result("success")

case object ReadTimeout extends Result("read_request_timeout")

case object Unavailable extends Result("unavailable")

case object WriteTimeout extends Result("write_request_timeout")

object Result {
  def fromString(string: String): Result = {
    string match {
      case ReadTimeout.string => ReadTimeout
      case Unavailable.string => Unavailable
      case WriteTimeout.string => WriteTimeout
      case Success.string => Success
      case _ => Success
    }
  }
}


