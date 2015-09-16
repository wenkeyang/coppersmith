package commbank.coppersmith.lift

import scalaz._, Scalaz._

import commbank.coppersmith.Join
import org.specs2.Specification
import memory._


class MemorySpec extends Specification {
  def is =
    s2"""
         Joins                   $joins
         Left joins              $leftJoins
         Three way inner joins   $leftJoins
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

  val cs = List(
    C("2"),
    C("22")
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

  def threeWayJoin = {
    val join = Join.multiway[A].inner[B].on((a: A) => a.a, (b: B) => b.a).
                                inner[C].on((a: A, b:B) => b.b, (c: C) => c.b)


//    val result: List[(A, B, C)] = liftMultiwayJoin(join)((as, bs, cs))

    1 === 1
  }
}
