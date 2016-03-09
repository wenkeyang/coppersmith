//
// Copyright 2016 Commonwealth Bank of Australia
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//        http://www.apache.org/licenses/LICENSE-2.0
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

package commbank.coppersmith.lift

import org.specs2.Specification

import scalaz.Scalaz._

import commbank.coppersmith.Join
import commbank.coppersmith.lift.memory._

class MemorySpec extends Specification {
  def is =
    s2"""
         Joins                   $joins
         Left joins              $leftJoins
      """


  case class A(a:Int, b:String)
  case class B(a:Int, b: String)
  case class C(b: String)

    val as = List(
      A(1, "1"),
      A(2, "2"),
      A(2, "22"),
      A(3, "333")
    )
    val bs = List(
      B(2, "2"),
      B(2, "22"),
      B(2, "222"),
      B(3, "333"),
      B(4, "4")
    )

  def joins = {

    val expected = List(
      A(2,"2") -> B(2, "2"),
      A(2,"2") -> B(2, "22"),
      A(2,"2") -> B(2, "222"),
      A(2,"22") -> B(2, "2"),
      A(2,"22") -> B(2, "22"),
      A(2,"22") -> B(2, "222"),
      A(3,"333") -> B(3, "333")
    )

    liftJoin(Join[A].to[B].on(_.a, _.a))(as, bs) === expected
  }

  def leftJoins = {

    val expected = List(
      A(1, "1") -> None,
      A(2,"2") -> Some(B(2, "2")),
      A(2,"2") -> Some(B(2, "22")),
      A(2,"2") -> Some(B(2, "222")),
      A(2,"22") -> Some(B(2, "2")),
      A(2,"22") -> Some(B(2, "22")),
      A(2,"22") -> Some(B(2, "222")),
      A(3,"333") -> Some(B(3, "333"))
    )

    liftLeftJoin(Join.left[A].to[B].on(_.a, _.a))(as, bs) === expected
  }

}
