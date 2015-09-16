package commbank.coppersmith

import shapeless._

import shapeless.ops.function._
import shapeless.ops.hlist._
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
    def inner[B] = IncompleteJoinedHl[A :: HNil, B, B, A :: B :: HNil, HNil](HNil)
    def left[B]  = IncompleteJoinedHl[A :: HNil, Option[B], B, A :: Option[B] :: HNil, HNil](HNil)
  }

  case class IncompleteJoinedHl[LeftSides <: HList, RightSide, FlatRight, Out <: HList, PreviousJoins <: HList](pjs: PreviousJoins) {

    def on[J : Ordered, F, NextJoins <: HList](leftFun: F, rightFun: FlatRight => J)(implicit
                                                          fnHLister : FnToProduct[F] {type Out = LeftSides => J},
                                                          pp1 : Prepend.Aux[LeftSides, RightSide :: HNil, Out],
                                                          pp2 : Prepend.Aux[PreviousJoins, (LeftSides => J, RightSide => J) :: HNil, NextJoins]) = {
      val leftHListFun = leftFun.toProduct
      JoinedHl[LeftSides, RightSide, FlatRight, PreviousJoins, J, Out](leftHListFun, rightFun, pjs)
    }
  }



  case class JoinedHl[
  LeftSides <: HList,
  RightSide,    //This is either FlatRight or Option[FlatRight]
  FlatRight,
  PreviousJoins <: HList,
  JoinColumn: Ordered,
  Out <: HList](l : LeftSides => JoinColumn,
                r: FlatRight  => JoinColumn,
                pjs: PreviousJoins)
               (implicit pp1 : Prepend.Aux[LeftSides, RightSide :: HNil, Out]) {
    def inner[B, NextJoins <: HList, OutNext <: HList]
      (implicit pp2: Prepend.Aux[Out, B :: HNil, OutNext],
       prependNext: Prepend.Aux[PreviousJoins, (LeftSides => JoinColumn, FlatRight => JoinColumn) :: HNil, NextJoins]) =
      IncompleteJoinedHl[Out, B, B, OutNext, NextJoins](pjs :+ ((l, r)))

    def left[B, NextJoins <: HList, OutNext <: HList]
    (implicit pp2: Prepend.Aux[Out, Option[B] :: HNil, OutNext],
      prependNext: Prepend.Aux[PreviousJoins, (LeftSides => JoinColumn, FlatRight => JoinColumn) :: HNil, NextJoins]) =
      IncompleteJoinedHl[Out, Option[B], B, OutNext, NextJoins](pjs :+ ((l, r)))
  }
}
