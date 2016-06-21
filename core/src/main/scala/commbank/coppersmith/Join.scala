//
// Copyright 2016 Commonwealth Bank of Australia
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//        http://www.apache.org/licenses/LICENSE-2.0
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

package commbank.coppersmith

import shapeless._
import shapeless.ops.function._
import shapeless.ops.hlist._
import shapeless.ops.product.ToHList
import shapeless.syntax.std.function._

import commbank.coppersmith.generated.Joined2

object From {
  def apply[S](): From[S] = From(None)
}

case class From[S](filter: Option[S => Boolean] = None) extends FeatureSource[S, From[S]](filter) {
  type FS = From[S]

  def copyWithFilter(filter: Option[S => Boolean]) = copy(filter)
}

object Join {
  def join[T]: InnerJoinableTo[T] = new InnerJoinableTo[T]
  def apply[T] = join[T]

  def left[T]: LeftOuterJoinableTo[T] = new LeftOuterJoinableTo[T]

  def multiway[T]: JoinableTo[T] = new JoinableTo[T]

  class InnerJoinableTo[L] {
    def to[R]: IncompleteJoin[L, R, L, R] = new IncompleteJoin[L, R, L, R]
  }

  class LeftOuterJoinableTo[L] {
    def to[R]: IncompleteJoin[L, R, L, Option[R]] = new IncompleteJoin[L, R, L, Option[R]]
  }

  class JoinableTo[L] {
    def inner[R]: IncompleteJoin[L, R, L, R]         = new IncompleteJoin[L, R, L, R]
    def left[R]:  IncompleteJoin[L, R, L, Option[R]] = new IncompleteJoin[L, R, L, Option[R]]
  }

  class IncompleteJoin[S1, S2, T1, T2] {
    def on[J: Ordering](s1: T1 => J, s2: S2 => J): Joined2[S1, S2, J, T1, T2] = Joined2(s1, s2, None)
  }

  def multiwayShapeless[A] = Multiway[A]()

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
    (implicit tupler: shapeless.ops.hlist.Tupler.Aux[Types,TypesTuple]):CompleteJoinHlFeatureSource[Types, Joins, TypesTuple] =
      CompleteJoinHlFeatureSource[Types, Joins, TypesTuple](this, None)
  }

  case class CompleteJoinHlFeatureSource[Types <: HList, Joins <: HList, TypesTuple <: Product](
      join: CompleteJoinHl[Types, Joins],
      filter: Option[TypesTuple => Boolean])
     (implicit tupler: Tupler.Aux[Types, TypesTuple])
        extends FeatureSource[TypesTuple, CompleteJoinHlFeatureSource[Types, Joins, TypesTuple]] {
      def copyWithFilter(filter: Option[TypesTuple => Boolean]) = copy(filter = filter)
  }
}
