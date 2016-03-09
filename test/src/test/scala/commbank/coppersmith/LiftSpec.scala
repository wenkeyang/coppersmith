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

package commbank.coppersmith

import org.specs2.Specification

import scalaz.Scalaz._

import commbank.coppersmith.Join.CompleteJoinHl
import commbank.coppersmith.lift.memory._

class LiftSpec extends Specification {
  implicit val lift = commbank.coppersmith.lift.memory


  def is = s2"""
      Multiway join fold function
        Joins once          $join1
        Joins a second time $join2
        Folds lengh 0       $fold0
        Folds lengh 1       $fold1
        Left join  $twoWayLeftJoin
      Join
        Can do two way      $twoWayJoin
        Can do three way   $threeWayJoin
    """

  case class A(id:Int)
  case class B(id:Int, str:String)
  case class C(str:String)


  def join1 = {
    import shapeless._

    val as = List[A]()
    val bs = List[B]()

    val soFar: List[A :: HNil] = as.map(_ :: HNil)
    val pipeWithJoin: (NextPipe[List, B, B], (A :: HNil => Int, B => Int)) =
      (NextPipe[List, B, B](bs), ((a: A :: HList) => a.head.id, (b: B) => b.id))

    val result:List[(A,B)] = JoinFolders.joinFolder(soFar, pipeWithJoin).map(_.tupled)

    result must_== List()
  }

  def join2 = {
    import shapeless._


    val abs = List[(A, B)]()
    val cs = List[C]()

    val soFar: List[A :: B :: HNil] = abs.map { case (a, b) => a :: b :: HNil }
    val pipeWithJoin: (NextPipe[List, C,C], (A :: B :: HNil => String, C => String)) =
      (NextPipe[List, C, C](cs), ((x : A :: B :: HNil) => x.tail.head.str, (c: C) => c.str))

    val result:List[A :: B :: C :: HNil] = JoinFolders.joinFolder(soFar, pipeWithJoin)

    result.map(_.tupled) === List[(A,B,C)]()
  }

  def fold0 = {
    import shapeless._

    val as = List[A]()

    val initial: List[A :: HNil] = as.map(_ :: HNil)
    val toFold : HNil = HNil

    toFold.foldLeft(initial)(JoinFolders.joinFolder).map(_.tupled) === as
  }

  def fold1 = {
    import shapeless._

    val as = List[A]()
    val bs = List[B]()

    val initial: List[A :: HNil] = as.map(_ :: HNil)
    val pipeWithJoin: (NextPipe[List, B, B], (A :: HNil => Int, B => Int) ) =
      (NextPipe[List, B,B](bs),  ((a: A :: HNil) => a.head.id, (b: B) => b.id))

    val toFold: (NextPipe[List, B, B], (A ::HNil  => Int, B => Int)) :: HNil = pipeWithJoin :: HNil
    toFold.foldLeft(initial)(JoinFolders.joinFolder) === List[(A,B)]()
  }

  def twoWayJoin = {
    import shapeless._

    val as = List[A](A(1), A(2), A(1))
    val bs = List[B](B(1, "One"))

    type Types = A :: B :: HNil
    type Joins = (A :: HNil => Int, B => Int) :: HNil

    val join: CompleteJoinHl[Types, Joins] =
      Join.multiway[A].inner[B].on((a: A) => a.id, (b: B) => b.id)

    import ToNextPipe._


    val resultFixedTypes = liftMultiwayJoin[
      (List[A], List[B]),
      List[A] :: List[B] :: HNil,
      List[A],
      A,
      List[B] :: HNil,
      NextPipe[List, B,B] :: HNil,
      A :: B :: HNil,
      A,
      B :: HNil,
      Joins,
      (A,B),
      ( NextPipe[List, B,B], (A :: HNil => Int, B => Int)) :: HNil
      ] (join)((as, bs))

    val resultInferredTypes = liftMultiwayJoin (join)((as, bs))

    Seq(
      resultInferredTypes === List(
        A(1) -> B(1, "One"),
        A(1) -> B(1, "One")
      ),
      resultFixedTypes === resultInferredTypes
    )
  }

  def twoWayLeftJoin = {
    import shapeless._

    val as = List[A](A(1), A(2), A(1))
    val bs = List[B](B(1, "One"))

    type Types = A :: Option[B] :: HNil
    type Joins = (A :: HNil => Int, B => Int) :: HNil

    val join: CompleteJoinHl[Types, Joins] =
      Join.multiway[A].left[B].on((a: A) => a.id, (b: B) => b.id)

    import ToNextPipe._


    val result = liftMultiwayJoin (join)((as, bs))
    val expected = List[(A, Option[B])](
      A(1) -> Some(B(1, "One")),
      A(2) -> None,
      A(1) -> Some(B(1, "One"))
    )
    result === expected
  }

  def threeWayJoin = {
    val as = List(
      A(1),
      A(2),
      A(2),
      A(3)
    )
    val bs = List(
      B(2, "2"),
      B(2, "22"),
      B(2, "222"),
      B(3, "333"),
      B(4, "4")
    )

    val cs = List(
      C("2"),
      C("22")
    )
    val join = Join.multiway[A].inner[B].on((a: A) => a.id, (b: B) => b.id).
      inner[C].on((a: A, b: B) => b.str, (c: C) => c.str)


    val result = liftMultiwayJoin(join)((as, bs, cs))
    val expected = List(
      (A(2), B(2, "2"), C("2")),
      (A(2), B(2, "2"), C("2")),
      (A(2), B(2, "22"), C("22")),
      (A(2), B(2, "22"), C("22"))
    )

    result === expected
  }


}
