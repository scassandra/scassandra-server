/*
 * Copyright (C) 2014 Christopher Batey and Dogan Narinc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scassandra.cqlmessages

abstract class Consistency {
  val code : Short
  val string: String = {
    val className = this.getClass.getSimpleName
    className.stripSuffix("$")
  }
}

object ANY extends Consistency {
  val code: Short = 0
}
object ONE extends Consistency {
  val code: Short = 1
}

object TWO extends Consistency {
  val code: Short = 2
}

object THREE extends Consistency {
  val code: Short = 3
}

object QUORUM extends Consistency {
  val code: Short = 4
}

object ALL extends Consistency {
  val code: Short = 5
}

object LOCAL_QUORUM extends Consistency {
  val code: Short = 6
}

object EACH_QUORUM extends Consistency {
  val code: Short = 7
}

object SERIAL extends Consistency {
  val code: Short = 8
}

object LOCAL_SERIAL extends Consistency {
  val code: Short = 9
}

object LOCAL_ONE extends Consistency {
  val code: Short = 10
}




/*
                     0x0000    ANY
                     0x0001    ONE
                     0x0002    TWO
                     0x0003    THREE
                     0x0004    QUORUM
                     0x0005    ALL
                     0x0006    LOCAL_QUORUM
                     0x0007    EACH_QUORUM
                     0x0008    SERIAL
                     0x0009    LOCAL_SERIAL
                     0x0010    LOCAL_ONE
 */

object Consistency {
  def fromCode(code: Short) : Consistency = {
    code match {
      case ANY.code => ANY
      case ONE.code => ONE
      case TWO.code => TWO
      case THREE.code => THREE
      case QUORUM.code => QUORUM
      case ALL.code => ALL
      case LOCAL_QUORUM.code => LOCAL_QUORUM
      case EACH_QUORUM.code => EACH_QUORUM
      case SERIAL.code => SERIAL
      case LOCAL_SERIAL.code => LOCAL_SERIAL
      case LOCAL_ONE.code => LOCAL_ONE

    }
  }  
  
  def fromString(code: String) : Consistency = {
    code match {
      case ANY.string => ANY
      case ONE.string => ONE
      case TWO.string => TWO
      case THREE.string => THREE
      case QUORUM.string => QUORUM
      case ALL.string => ALL
      case LOCAL_QUORUM.string => LOCAL_QUORUM
      case EACH_QUORUM.string => EACH_QUORUM
      case SERIAL.string => SERIAL
      case LOCAL_SERIAL.string => LOCAL_SERIAL
      case LOCAL_ONE.string => LOCAL_ONE

    }
  }

  val all = List(ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL, LOCAL_ONE)
}


