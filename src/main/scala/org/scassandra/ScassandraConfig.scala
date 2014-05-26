package org.scassandra

import com.typesafe.config.ConfigFactory

object ScassandraConfig {

  println(System.getProperty("scassandra.config.resource"))

  private val config = System.getProperty("scassandra.config.resource") match {
    case s: String => ConfigFactory.load(s)
    case _ => ConfigFactory.load()
  }

  val binaryPort = config.getInt("scassandra.binary.port")
  val binaryListenAddress = config.getString("scassandra.binary.listen-address")
  val adminPort = config.getInt("scassandra.admin.port")
  val adminListenAddress = config.getString("scassandra.admin.listen-address")
}
