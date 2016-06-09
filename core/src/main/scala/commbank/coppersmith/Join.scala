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
import shapeless.syntax.std.function._


import shapeless.ops.product.ToHList

object From {
  def apply[S](): From[S] = From(None)
}

case class From[S](filter: Option[S => Boolean] = None) extends FeatureSource[S, From[S]](filter) {
  type FS = From[S]

  def copyWithFilter(filter: Option[S => Boolean]) = copy(filter)
}

case class Joined[L, R, J : Ordering, S](left: L => J, right: R => J, filter: Option[S => Boolean])
    extends FeatureSource[S, Joined[L, R, J, S]](filter) {
  type FS = Joined[L, R, J, S]

  def copyWithFilter(filter: Option[S => Boolean]) = copy(filter = filter)

  def innerJoinTo[S3] = Join.IncompleteJoin3[L, R, S3, J, S, (S, S3)](left, right)
  def leftJoinTo[S3] = Join.IncompleteJoin3[L, R, S3, J, S, (S, Option[S3])](left, right)
}

case class Joined3[S1, S2, S3, J1 : Ordering, J2 : Ordering, S12, S123](
  s1j1:   S1  => J1,
  s2j1:   S2  => J1,
  s12j2:  S12 => J2,
  s3j2:   S3  => J2,
  filter: Option[S123 => Boolean]
) extends FeatureSource[S123, Joined3[S1, S2, S3, J1, J2, S12, S123]](filter) {
  type FS = Joined3[S1, S2, S3, J1, J2, S12, S123]

  def copyWithFilter(filter: Option[S123 => Boolean]) = copy(filter = filter)
  def innerJoinTo[S4] =
    Join.IncompleteJoin4[S1, S2, S3, S4, J1, J2, S12, S123, (S123, S4)](s1j1, s2j1, s12j2, s3j2)
  def leftJoinTo[S4] =
    Join.IncompleteJoin4[S1, S2, S3, S4, J1, J2, S12, S123, (S123, Option[S4])](s1j1, s2j1, s12j2, s3j2)
}

case class Joined4[S1, S2, S3, S4, J1 : Ordering, J2 : Ordering, J3 : Ordering, S12, S123, S1234](
  s1j1:   S1   => J1,
  s2j1:   S2   => J1,
  s12j2:  S12  => J2,
  s3j2:   S3   => J2,
  s123j3: S123 => J3,
  s4j3:   S4   => J3,
  filter: Option[S1234 => Boolean]
) extends FeatureSource[S1234, Joined4[S1, S2, S3, S4, J1, J2, J3, S12, S123, S1234]](filter) {
  type FS = Joined4[S1, S2, S3, S4, J1, J2, J3, S12, S123, S1234]

  def copyWithFilter(filter: Option[S1234 => Boolean]) = copy(filter = filter)
}

object Join {
  def join[T]: InnerJoinableTo[T] = new EmptyInnerJoinableTo[T]
  def apply[T] = join[T]

  def left[T]: LeftOuterJoinableTo[T] = new EmptyLeftOuterJoinableTo[T]

  class EmptyInnerJoinableTo[L] extends InnerJoinableTo[L]

  class EmptyLeftOuterJoinableTo[L] extends LeftOuterJoinableTo[L]

  sealed trait InnerJoinableTo[L] {
    def to[R]: IncompleteJoin[L, R, (L, R)] = new IncompleteJoin[L, R, (L, R)]
  }

  sealed trait LeftOuterJoinableTo[L] {
    def to[R]: IncompleteJoin[L, R, (L, Option[R])] = new IncompleteJoin[L, R, (L, Option[R])]
  }

  class IncompleteJoin[L, R, S] {
    def on[J: Ordering](l: L => J, r: R => J): Joined[L, R, J, S] = Joined(l, r, None)
  }

  case class IncompleteJoin3[S1, S2, S3, J1 : Ordering, S12, S123](s1j1: S1 => J1, s2j1: S2 => J1) {
    def on[J2 : Ordering](
      s12j2: S12 => J2,
      s3j2:  S3 => J2
    ): Joined3[S1, S2, S3, J1, J2, S12, S123] = Joined3(s1j1, s2j1, s12j2, s3j2, None)
  }

  case class IncompleteJoin4[S1, S2, S3, S4, J1 : Ordering, J2 : Ordering, S12, S123, S1234](
    s1j1:  S1  => J1,
    s2j1:  S2  => J1,
    s12j2: S12  => J2,
    s3j2:  S3   => J2
  ) {
    def on[J3 : Ordering](
      s123j3: S123 => J3,
      s4j3:   S4 => J3
    ): Joined4[S1, S2, S3, S4, J1, J2, J3, S12, S123, S1234] =
      Joined4(s1j1, s2j1, s12j2, s3j2, s123j3, s4j3, None)
  }

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
