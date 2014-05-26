package org.scassandra

import com.typesafe.config.ConfigFactory

object ScassandraConfig {
  private val config =  ConfigFactory.load()

  val binaryPort =             config.getInt("scassandra.binary.port")
  val binaryListenAddress = config.getString("scassandra.binary.listen-address")
  val adminPort =              config.getInt("scassandra.admin.port")
  val adminListenAddress =  config.getString("scassandra.admin.listen-address")
}
