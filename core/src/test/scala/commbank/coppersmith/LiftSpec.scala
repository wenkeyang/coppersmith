package commbank.coppersmith

import commbank.coppersmith.Join.CompleteJoinHl
import commbank.coppersmith.lift.memory.{joinFolder => folder, _}
import org.specs2.Specification
import scalaz._, Scalaz._

class LiftSpec extends Specification {

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
    val pipeWithJoin: (NextPipe[B, B], (A :: HNil => Int, B => Int)) =
      (NextPipe[B, B](bs), ((a: A :: HList) => a.head.id, (b: B) => b.id))

    val result:List[(A,B)] = folder(soFar, pipeWithJoin).map(_.tupled)

    result === List()
  }

  def join2 = {
    import shapeless._


    val abs = List[(A, B)]()
    val cs = List[C]()

    val soFar: List[A :: B :: HNil] = abs.map { case (a, b) => a :: b :: HNil }
    val pipeWithJoin: (NextPipe[C,C], (A :: B :: HNil => String, C => String)) =
      (NextPipe[C, C](cs), ((x : A :: B :: HNil) => x.tail.head.str, (c: C) => c.str))

    val result:List[A :: B :: C :: HNil] = folder(soFar, pipeWithJoin)

    result.map(_.tupled) === List[(A,B,C)]()
  }

  def fold0 = {
    import shapeless._

    val as = List[A]()

    val initial: List[A :: HNil] = as.map(_ :: HNil)
    val toFold : HNil = HNil

    toFold.foldLeft(initial)(folder).map(_.tupled) === as
  }

  def fold1 = {
    import shapeless._

    val as = List[A]()
    val bs = List[B]()

    val initial: List[A :: HNil] = as.map(_ :: HNil)
    val pipeWithJoin: (NextPipe[B, B], (A :: HNil => Int, B => Int) ) =
      (NextPipe[B,B](bs),  ((a: A :: HNil) => a.head.id, (b: B) => b.id))

    val toFold: (NextPipe[B, B], (A ::HNil  => Int, B => Int)) :: HNil = pipeWithJoin :: HNil
    toFold.foldLeft(initial)(folder) === List[(A,B)]()
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


    val result = liftMultiwayJoin (join)((as, bs))
    // Keeping the type parameters commented out before as they get tedious to derive by hand, and are required when
    // debugging type inference
//      [
//      (List[A], List[B]),
//      List[A] :: List[B] :: HNil,
//      List[A],
//      A,
//      List[B] :: HNil,
//      NextPipe[B,B] :: HNil,
//      A :: B :: HNil,
//      A,
//      B :: HNil,
//      Joins,
//      (A,B),
//      ( NextPipe[B,B], (A :: HNil => Int, B => Int)) :: HNil
//      ]

    result === List(
      A(1) -> B(1, "One"),
      A(1) -> B(1, "One")
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
