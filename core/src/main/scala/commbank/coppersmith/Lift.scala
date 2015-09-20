package commbank.coppersmith

import commbank.coppersmith.Join.{CompleteJoinHl, JoinedHl}
import commbank.coppersmith.lift.memory
import shapeless._
import shapeless.ops.function.FnToProduct
import shapeless.ops.hlist._
import shapeless.ops.nat.Pred

import scalaz.{Ordering => _, Length => _, Zip => _, _}, Scalaz._

import Feature.Value

trait Lift[P[_]] {
  def lift[S, V <: Value](f:Feature[S,V])(s: P[S]): P[FeatureValue[V]]

  def lift[S](fs: FeatureSet[S])(s: P[S]): P[FeatureValue[_]]


  //Join stuff

  def innerJoinNext[LeftSides <: HList, RightSide, J : Ordering, Out <: HList]
    (l: LeftSides => J, r: RightSide => J )
    (a:P[LeftSides], b: P[RightSide])
    (implicit pp: Prepend.Aux[LeftSides, RightSide :: HNil, Out])
    : P[Out]

  def leftJoinNext[LeftSides <: HList, RightSide, J : Ordering, Out <: HList]
  (l: LeftSides => J, r: RightSide => J )
  (a:P[LeftSides], b: P[RightSide])
  (implicit pp: Prepend.Aux[LeftSides, Option[RightSide] :: HNil, Out])
  : P[Out]


  //two is a special case, a little easier to do than the general case
  def liftJoin[A, B, J : Ordering](joined: Joined[A, B, J, (A, B) ])(a:P[A], b: P[B])(implicit functor: Functor[P]): P[(A, B)] = {
    innerJoinNext[A :: HNil, B, J, A :: B :: HNil]((l: A :: HNil) => joined.left(l.head), joined.right)(a.map(_ :: HNil), b).map(_.tupled)
  }

  def liftLeftJoin[A, B, J : Ordering](joined: Joined[A, B, J, (A, Option[B])])(a: P[A], b: P[B])(implicit functor: Functor[P]): P[(A, Option[B])] = {
    leftJoinNext[A :: HNil, B, J, A :: Option[B] :: HNil]((l: A :: HNil) => joined.left(l.head), joined.right)(a.map(_ :: HNil), b).map(_.tupled)
  }

  def liftMultiwayJoin[
  InTuple <: Product,
  InHList <: HList,
  InHeadType,
  InHeadElementType,
  InTail <: HList,
  Types <: HList,
  Joins <: HList,
  OutTuple <: Product,
  Zipped <: HList](join: CompleteJoinHl[Types, Joins])
                  (in : InTuple)
                  (implicit
                   inToHlist : Generic.Aux[InTuple, InHList],
                   inIsCons   : IsHCons.Aux[InHList, InHeadType, InTail],
                   pEl1       : P[InHeadElementType] =:= InHeadType,
                   pEl2       : InHeadType =:= P[InHeadElementType],
                   zipper     : Zip.Aux[InTail :: Joins :: HNil, Zipped],
                   leftFolder : LeftFolder.Aux[Zipped, P[InHeadElementType :: HNil], memory.joinFolder.type, P[Types]],
                   tupler     : Tupler.Aux[Types, OutTuple],
                   pFunctor   : Functor[P]
                                          )
  : P[OutTuple] = {
    val inHl : InHList = inToHlist.to(in)
    val inHead: P[InHeadElementType] = pEl2(inHl.head)
    val inTail: InTail = inHl.tail
    val zipped: Zipped = inTail zip join.joins
    val initial: P[InHeadElementType :: HNil] = inHead.map(_ :: HNil)
    val folded : P[Types] = zipped.foldLeft(initial)(memory.joinFolder)
    folded.map(_.tupled)
  }

  //Lower priority since inner joins have less specific types than left joins
  trait innerFolder extends Poly2 {
    implicit def doInnerJoin[
      SoFar <: HList,
      Next,
      J : Ordering,
      OutInner <: HList,
      Joins <: HList
    ](implicit prepend: Prepend.Aux[SoFar, Next :: HNil, OutInner]) =
      at[P[SoFar], (NextPipe[Next, Next], (SoFar => J, Next => J))] {
      (acc: P[SoFar], pipeWithJoin: (NextPipe[Next, Next], (SoFar => J, Next => J) )) =>
        val fnSoFar: SoFar => J = pipeWithJoin._2._1
        val fnNext: Next => J = pipeWithJoin._2._2
        innerJoinNext[SoFar, Next, J, OutInner](fnSoFar, fnNext)(acc, pipeWithJoin._1.pipe)
    }
  }

  object joinFolder extends innerFolder {
    implicit def doLeftJoin[
    SoFar <: HList,
    Next,
    J : Ordering,
    OutInner <: HList,
    Joins <: HList
    ](implicit prepend: Prepend.Aux[SoFar, Next :: HNil, OutInner]) =
      at[P[SoFar], (NextPipe[Next, Option[Next]], (SoFar => J, Next => J))] {
        (acc: P[SoFar], pipeWithJoin: (NextPipe[Next, Option[Next]], (SoFar => J, Next => J) )) =>
          val fnSoFar: SoFar => J = pipeWithJoin._2._1
          val fnNext: Next => J = pipeWithJoin._2._2
          leftJoinNext[SoFar, Next, J, OutInner](fnSoFar, fnNext)(acc, pipeWithJoin._1.pipe)
      }
  }

  def liftBinder[S, U <: FeatureSource[S, U], B <: SourceBinder[S, U, P]](
    underlying: U,
    binder: B,
    filter: Option[S => Boolean]
  ): BoundFeatureSource[S, P]


  case class NextPipe[Next, JoinType](pipe: P[Next])
}
