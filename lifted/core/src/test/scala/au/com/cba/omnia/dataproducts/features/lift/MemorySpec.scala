package au.com.cba.omnia.dataproducts.features.lift

import au.com.cba.omnia.dataproducts.features.Join
import org.specs2.Specification

class MemorySpec extends Specification {
  def is =
    s2"""
         Joins $joins
      """


  case class A(a:Int, b:String)
  case class B(a:Int, b: String)

  def joins = {
    import memory._

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
}
