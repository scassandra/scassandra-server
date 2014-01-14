
object ServerStubAsThread {
  def apply() = new Thread(new Runnable {
    def run() {
      ServerStubRunner.run()
    }
  })
}
