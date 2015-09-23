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

    def left[B] = IncompleteJoinedHl[A :: HNil, Option[B], B, A :: Option[B] :: HNil, HNil](HNil)
  }

  case class IncompleteJoinedHl[
    LeftSides <: HList,
    RightSide, FlatRight,
    Out <: HList,
    PreviousJoins <: HList](pjs: PreviousJoins) {

    def on[J: Ordering, F, NextJoins <: HList]
      (leftFun: F, rightFun: FlatRight => J)
      (implicit
       fnHLister : FnToProduct.Aux[F,  LeftSides => J] ,
       pp1       : Prepend.Aux[LeftSides, RightSide :: HNil, Out],
       pp2       : Prepend.Aux[PreviousJoins, (LeftSides => J, FlatRight => J) :: HNil, NextJoins]): CompleteJoinHl[Out, NextJoins] = {
      val leftHListFun: LeftSides => J = leftFun.toProduct
      CompleteJoinHl[Out, NextJoins](pjs :+ ((leftHListFun, rightFun)))
    }
  }

  case class CompleteJoinHl[Types <: HList, Joins <: HList](joins: Joins) {
    def inner[B](implicit np: Prepend[Types, B :: HNil]) =
      IncompleteJoinedHl[Types, B, B, np.Out, Joins](joins)

    def left[B] (implicit np: Prepend[Types, Option[B] :: HNil]) =
      IncompleteJoinedHl[Types, Option[B], B, np.Out, Joins](joins)

    def src[TypesTuple <: Product]
    (implicit tupler: shapeless.ops.hlist.Tupler.Aux[Types,TypesTuple]):FeatureSource[TypesTuple, CompleteJoinHlFeatureSource[Types, Joins, TypesTuple]] =
      CompleteJoinHlFeatureSource[Types, Joins, TypesTuple](this, None)
  }

  case class CompleteJoinHlFeatureSource[Types <: HList, Joins <: HList, TypesTuple <: Product]
    (join: CompleteJoinHl[Types, Joins], filter: Option[TypesTuple => Boolean])
    (implicit tupler: Tupler.Aux[Types, TypesTuple])
    extends FeatureSource[TypesTuple, CompleteJoinHlFeatureSource[Types, Joins, TypesTuple]] {
    def copyWithFilter(filter: Option[TypesTuple => Boolean]) = copy(filter = filter)
  }
}
