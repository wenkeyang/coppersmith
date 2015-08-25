package au.com.cba.omnia.dataproducts.features

object Join {
  sealed trait JoinType
  sealed trait LeftOuter extends JoinType
  sealed trait Inner extends JoinType

  trait InnerJoinableTo[L] {
    def to[R]: IncompleteJoin[L, R, Inner] = new IncompleteJoin[L,R, Inner]
  }
  trait LeftOuterJoinableTo[L] {
    def to[R]: IncompleteJoin[L, R, LeftOuter] = new IncompleteJoin[L, R, LeftOuter]
  }

  class EmptyInnerJoinableTo[L] extends InnerJoinableTo[L]
  class EmptyLeftOuterJoinableTo[L] extends LeftOuterJoinableTo[L]

  class IncompleteJoin[L, R, JT <: JoinType] {
    //Write as many of these as we need...
    def on[J : Ordering](l: L => J, r: R => J): Joined[L, R, J, JT] = Joined[L, R, J, JT](l, r)
  }

  case class Joined[L, R, J : Ordering, JT <: JoinType](left: L => J, right: R => J)

  def join[T]: InnerJoinableTo[T] = new EmptyInnerJoinableTo[T]
  def left[T]: LeftOuterJoinableTo[T] = new EmptyLeftOuterJoinableTo[T]
  def apply[T] = join[T]
}