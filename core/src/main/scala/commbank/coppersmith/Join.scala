package commbank.coppersmith

import shapeless._

import shapeless.ops.function._
import shapeless.ops.hlist.Prepend
import shapeless.syntax.std.function._


import shapeless.ops.product.ToHList

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
    def inner[B] = IncompleteJoinedHl[A :: HNil, B, A :: B :: HNil]()
    def left[B]  = IncompleteJoinedHl[A :: HNil, Option[B], A :: Option[B] :: HNil]()
  }

  case class IncompleteJoinedHl[HL <: HList, N, O <: HList]() {

    def on[J : Ordering, F](leftFun: F, rightFun: N => J)(implicit fnHLister : FnToProduct[F] {type Out = HL => J}, prepend : Prepend.Aux[HL, N :: HNil, O]) = {
      val leftHListFun = leftFun.toProduct

      JoinedHl(leftHListFun, rightFun)
    }
  }

  case class JoinedHl[HL <: HList, N, J : Ordering, S, O <: HList](l: HL => J, r: N => J)(implicit pp1 : Prepend.Aux[HL, N :: HNil, O]) {
    def inner[B](implicit prepend: Prepend[O, B :: HNil]) = IncompleteJoinedHl[O, B, prepend.Out]()
    def left[B](implicit prepend: Prepend[O, Option[B] :: HNil]) = IncompleteJoinedHl[O, Option[B], prepend.Out]()
  }

}
