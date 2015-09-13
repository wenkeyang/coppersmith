package commbank.coppersmith
package lift

import shapeless._


class JoinSpec {
  case class A(id:Int)
  case class B(id:Int)
  case class C(id:Int)
  case class D(id:Int)
  case class E(id:Int)
  case class F(id:Int)

  val twoWay = Join.multiway[A].inner[B].on(_.head.id, _.id)
  val threeWay = twoWay.inner[C].on({case (a, b) => b.id}, _.id)
}
