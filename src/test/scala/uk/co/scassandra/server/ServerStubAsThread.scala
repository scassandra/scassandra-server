package uk.co.scassandra.server

import uk.co.scassandra.ServerStubRunner

object ServerStubAsThread {
  def apply() = {
    new ServerStubAsThread
  }
}

class ServerStubAsThread {
  val serverStub = new ServerStubRunner
  val thread = new Thread(new Runnable {
    def run() {
      serverStub.start()
    }
  })

  def start() = {
    thread.start()
  }
  def shutdown() = {
    serverStub.shutdown()
  }
}
