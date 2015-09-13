package commbank.coppersmith

import shapeless._
import shapeless.ops.hlist._

import TypeHelpers._

object From {
  def apply[S](): From[S] = From(None)
}

case class From[S](filter: Option[S => Boolean] = None) extends FeatureSource[S, From[S]](filter) {
  type FS = From[S]

  def copyWithFilter(filter: Option[S => Boolean]) = copy(filter)
}

case class Joined[L, R, J: Ordering, S](left: L => J, right: R => J, filter: Option[S => Boolean])
  extends FeatureSource[S, Joined[L, R, J, S]](filter) {
  type FS = Joined[L, R, J, S]

  def copyWithFilter(filter: Option[S => Boolean]) = copy(filter = filter)
}


object Join {
  sealed trait InnerJoinableTo[L] {
    def to[R]: IncompleteJoin[L, R, (L, R)] = new IncompleteJoin[L, R, (L, R)]
  }
  sealed trait LeftOuterJoinableTo[L] {
    def to[R]: IncompleteJoin[L, R, (L, Option[R])] = new IncompleteJoin[L, R, (L, Option[R])]
  }

  class EmptyInnerJoinableTo[L] extends InnerJoinableTo[L]

  class EmptyLeftOuterJoinableTo[L] extends LeftOuterJoinableTo[L]

  class IncompleteJoin[L, R, S] {
    //Write as many of these as we need...
    def on[J: Ordering](l: L => J, r: R => J): Joined[L, R, J, S] = Joined(l, r, None)
  }


  def join[T]: InnerJoinableTo[T] = new EmptyInnerJoinableTo[T]

  def left[T]: LeftOuterJoinableTo[T] = new EmptyLeftOuterJoinableTo[T]

  def apply[T] = join[T]

  def multiway[A] = Multiway[A]()

  case class Multiway[A]() {
    def inner[B] = IncompleteJoinedHl[A :: HNil, B, (HNil, B)]()
  }
  case class IncompleteJoinedHl[HL <: HList, N, S]()(implicit tupler:Tupler[HL]) {
    def on[J : Ordering](left: tupler.Out => J, right:N => J)(implicit prepend: HL :+ N): JoinedHl[HL, N, J, S] = JoinedHl[HL, N, J, S](left, right)
  }
  case class JoinedHl[HL <: HList, N, J : Ordering, S](left: HL => J, right:N => J)(implicit tupler: Tupler[HL], val prepend : HL :+ N) {
    def inner[B](implicit pTupler: Tupler[prepend.Out]) = IncompleteJoinedHl[prepend.Out, N, (prepend.Out, B)]
  }
}
