package commbank.coppersmith
package lift

//Really just for testing the compilation of joins
class JoinSpec {
  case class A(id:Int)
  case class B(id:Int, str:String)
  case class C(id:String)
  case class D(id: Int, bId: Int)

  val fourWayInner = Join.multiway[A].inner[B].on((a: A)                    => a.id,  (b: B) => b.id)
                                     .left [C].on((a: A, b:B)               => b.str, (c: C) => c.id)
                                     .inner[D].on((a: A, b:B, c:Option[C])  => b.id,  (d: D) => d.bId)

}
