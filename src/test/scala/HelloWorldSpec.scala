/**
 * Date: 27/10/13
 * Time: 13:53
 */

import org.specs2.mutable._

class HelloWorldSpec extends Specification {

  "sayHi()" should {
    "return expected string" in {
      HelloWorld.sayHi() must equalTo("Hello World!")
    }
  }

}
