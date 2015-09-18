package commbank.coppersmith

import commbank.coppersmith.Join.CompleteJoinHl
import commbank.coppersmith.lift.memory
import org.specs2.Specification
import shapeless._
import shapeless.ops.hlist._

import scalaz._, Scalaz._

class LiftSpec extends Specification {
  def is = s2"""
      Multiway join fold function
        Joins once          $join1
        Joins a second time $join2
        Folds lengh 0       $fold0
        Folds lengh 1       $fold1
      Join
        Can do two way      $twoWayJoin
    """

  case class A(id:Int)
  case class B(id:Int, str:String)
  case class C(str:String)

  def join1 = {
    import shapeless._

    val folder = memory.joinFolder

    val as = List[A]()
    val bs = List[B]()

    val soFar: List[A :: HNil] = as.map(_ :: HNil)
    val pipeWithJoin: (List[B], (A :: HNil => Int, B => Int)) = (bs, ((a: A :: HList) => a.head.id, (b: B) => b.id))

    val result:List[(A,B)] = folder(soFar, pipeWithJoin).map(_.tupled)

    result === List()
  }

  def join2 = {
    import shapeless._

    val folder = memory.joinFolder

    val abs = List[(A, B)]()
    val cs = List[C]()

    val soFar: List[A :: B :: HNil] = abs.map { case (a, b) => a :: b :: HNil }
    val pipeWithJoin: (List[C], (A :: B :: HNil => String, C => String)) = (cs, ((x : A :: B :: HNil) => x.tail.head.str, (c: C) => c.str))

    val result:List[A :: B :: C :: HNil] = folder(soFar, pipeWithJoin)

    result.map(_.tupled) === List[(A,B,C)]()
  }

  def fold0 = {
    import shapeless._

    val folder = memory.joinFolder

    val as = List[A]()

    val initial: List[A :: HNil] = as.map(_ :: HNil)
    val toFold : HNil = HNil

    toFold.foldLeft(initial)(folder).map(_.tupled) === as
  }

  def fold1 = {
    import shapeless._

    val folder = memory.joinFolder

    val as = List[A]()
    val bs = List[B]()

    val initial: List[A :: HNil] = as.map(_ :: HNil)
    val pipeWithJoin: (List[B], (A :: HNil => Int, B => Int) ) = (bs,  ((a: A :: HNil) => a.head.id, (b: B) => b.id))
//
    val toFold: (List[B], (A ::HNil  => Int, B => Int)) :: HNil = pipeWithJoin :: HNil
    toFold.foldLeft(initial)(folder) === List[(A,B)]()
  }

  def twoWayJoin = {
    val as = List[A](A(1), A(2), A(1))
    val bs = List[B](B(1, "One"))

    val join: CompleteJoinHl[A :: B :: HNil, (A :: HNil => Int, B => Int) :: HNil] = Join.multiway[A].inner[B].on((a: A) => a.id, (b: B) => b.id).complete



    val result = memory.liftMultiwayJoin(join)((as, bs))

    result === List(
      A(1) -> B(1, "One"),
      A(1) -> B(1, "One")
    )
  }



  def experiment() = {
    ex((List[A]() :: List[B]() :: HNil))
  }

  def ex[TupleHL <: HList, Hel, Head, T <: HList](in : TupleHL)(implicit isHCons: IsHCons.Aux[TupleHL, Head, T],
                                                                       ev1 : List[Hel] =:= Head,
                                                                       ev2 : Head =:= List[Hel] ) = {
    val head: List[Hel] = ev2(isHCons.head(in))

  }

}
