package commbank.coppersmith
package lift

class JoinSpec {
  case class A(id:Int)
  case class B(id:Int)
  case class C(id:Int)
  case class D(id: Int, bId: Int)

  val fourWayInner = Join.multiway[A].inner[B].on((a: A) => a.id,           (b: B) => b.id)
                                     .inner[C].on((a: A, b:B) => b.id,      (c:C) => c.id)
                                     .inner[D].on((a: A, b:B, c:C) => b.id, (d:D) => d.bId)

}
