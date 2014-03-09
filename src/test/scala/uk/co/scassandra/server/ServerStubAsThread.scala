package uk.co.scassandra.server

import uk.co.scassandra.ServerStubRunner

object ServerStubAsThread {
  def apply() = new Thread(new Runnable {
    def run() {
      ServerStubRunner.run()
    }
  })
}
