package commbank.coppersmith

import commbank.coppersmith.Join.{CompleteJoinHl}
import shapeless._
import shapeless.ops.function.FnToProduct
import shapeless.ops.hlist._
import shapeless.ops.nat.Pred

import util.Conversion

import scalaz.{Ordering => _, Length => _, Zip => _, _}, Scalaz._

import Feature.Value

// Would have preferred functor to be specified as P[_] : Functor, but upcoming code in
// ContextFeatureSource.bindWithContext is unable to derive implicit Functor instance,
// and needs to access it via the Lift instance instead.
abstract class Lift[P[_]](implicit val functor: Functor[P]) {
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
  def liftJoin[A, B, J : Ordering]
    (joined: Joined[A, B, J, (A, B)])
    (a:P[A], b: P[B]): P[(A, B)] = {
    innerJoinNext((l: A :: HNil) =>
      joined.left(l.head), joined.right)(a.map(_ :: HNil), b).map(_.tupled)
  }

  def liftLeftJoin[A, B, J : Ordering]
    (joined: Joined[A, B, J, (A, Option[B])])
    (a: P[A], b: P[B]): P[(A, Option[B])] = {
    leftJoinNext((l: A :: HNil) =>
      joined.left(l.head), joined.right)(a.map(_ :: HNil), b).map(_.tupled)
  }

  def liftMultiwayJoin[ //type examples as comments for better readability
  InTuple <: Product, // (List[A], List[B], List[C])
  InHList <: HList, // List[A] :: List[B] :: List[C]
  InHeadType, //List[A]
  InHeadElementType, // A
  InTail <: HList, // List[B] :: List[C]
  NextPipes <: HList, // NextPipe[B,B] ::  NextPipe[C,C] :: HNil
  Types <: HList,    // A :: B :: C :: HNil
  TypesHead, // A
  TypesTail <: HList, // B :: C :: HNil
  Joins <: HList, // (A :: HNil => J, B => J) :: (A :: B :: HNil => J, C => J) :: HNil
  OutTuple <: Product, // (A,B,C)
  Zipped <: HList //  (NextPipe[B,B], (A :: HNil => J, B => J) ::
                  // (NextPipe[C,C], (A :: B :: HNil => J, C => J)) :: HNil)
  ](join: CompleteJoinHl[Types, Joins])
   (in : InTuple)
   (implicit inToHlist  : Conversion.Aux[InTuple, InHList],
             inIsCons   : IsHCons.Aux[InHList, InHeadType, InTail],
             typesIsCons: IsHCons.Aux[Types, TypesHead, TypesTail],
             tnp        : ToNextPipe.Aux[InTail, TypesTail, NextPipes],
             pEl1       : P[InHeadElementType] =:= InHeadType,
             pEl2       : InHeadType =:= P[InHeadElementType],
             zipper     : Zip.Aux[NextPipes :: Joins :: HNil, Zipped],
             leftFolder : LeftFolder.Aux[Zipped, P[InHeadElementType :: HNil],
                                            JoinFolders.joinFolder.type, P[Types]],
             tupler     : Tupler.Aux[Types, OutTuple])
  : P[OutTuple] = {
    val inHl : InHList = inToHlist.to(in)
    val inHead: P[InHeadElementType] = pEl2(inHl.head)
    val inTail: InTail = inHl.tail
    val tailWithJoined: NextPipes = tnp(inTail)
    val zipped: Zipped = tailWithJoined zip join.joins
    val initial: P[InHeadElementType :: HNil] = inHead.map(_ :: HNil)
    val folded : P[Types] = zipped.foldLeft(initial)(JoinFolders.joinFolder)
    folded.map(_.tupled)
  }


  def liftBinder[S, U <: FeatureSource[S, U], B <: SourceBinder[S, U, P]](
    underlying: U,
    binder: B,
    filter: Option[S => Boolean]
  ): BoundFeatureSource[S, P]

  def liftFilter[S](p: P[S], f: S => Boolean): P[S]

}

trait NullHList[HL <: HList] extends DepFn0 {
  type Out = HL
}

object NullHList {
  implicit object nullHListHnil extends NullHList[HNil] {
    def apply() = HNil
  }
  implicit def nullHListHcons[H, T <: HList](implicit tailNhl: NullHList[T]) = new NullHList[H :: T] {
    def apply() = null.asInstanceOf[H] :: tailNhl()
  }
}

case class NextPipe[P[_], Next, JoinType](pipe: P[Next])

trait ToNextPipe[L <: HList, R <: HList] extends DepFn1[L]

object ToNextPipe {
  def apply[L <: HList, R <: HList](implicit tnp: ToNextPipe[L, R]) = tnp

  type Aux[L <: HList, R <: HList, Out0] = ToNextPipe[L, R] { type Out = Out0 }


  object nextPipeComb extends Poly2 {
    implicit def default[P[_], U, V] = at[P[U], V] ( (u: P[U], _: V) => NextPipe[P, U, V](u) )
  }

  implicit def toNextPipe[L <: HList, R <: HList, Out0 <: HList]
  (implicit nhl     : NullHList[R],
   zipWith : ZipWith.Aux[L, R, nextPipeComb.type, Out0]): ToNextPipe.Aux[L, R, Out0] = new ToNextPipe[L, R] {
    type Out = Out0

    def apply(l: L) = {
      val nulls: R = nhl()

      l.zipWith(nulls)(nextPipeComb)
    }
  }
}

object JoinFolders {
  //Lower priority since inner joins have less specific types than left joins
  trait innerFolder extends Poly2 {
    implicit def doInnerJoin[
    P[_] : Lift,
    SoFar <: HList,
    Next,
    J : Ordering,
    OutInner <: HList,
    Joins <: HList
    ](implicit prepend: Prepend.Aux[SoFar, Next :: HNil, OutInner]) =
      at[P[SoFar], (NextPipe[P, Next, Next], (SoFar => J, Next => J))] {
        (acc: P[SoFar], pipeWithJoin: (NextPipe[P, Next, Next], (SoFar => J, Next => J) )) =>
          val fnSoFar: SoFar => J = pipeWithJoin._2._1
          val fnNext: Next => J = pipeWithJoin._2._2
          implicitly[Lift[P]].innerJoinNext[SoFar, Next, J, OutInner](fnSoFar, fnNext)(acc, pipeWithJoin._1.pipe)
      }
  }

  object joinFolder extends innerFolder {
    implicit def doLeftJoin[
    P[_] : Lift,
    SoFar <: HList,
    Next,
    J : Ordering,
    OutInner <: HList,
    Joins <: HList
    ](implicit prepend: Prepend.Aux[SoFar, Option[Next] :: HNil, OutInner]) =
      at[P[SoFar], (NextPipe[P, Next, Option[Next]], (SoFar => J, Next => J))] {
        (acc: P[SoFar], pipeWithJoin: (NextPipe[P, Next, Option[Next]], (SoFar => J, Next => J) )) =>
          val fnSoFar: SoFar => J = pipeWithJoin._2._1
          val fnNext: Next   => J = pipeWithJoin._2._2
          implicitly[Lift[P]].leftJoinNext[SoFar, Next, J, OutInner](fnSoFar, fnNext)(acc, pipeWithJoin._1.pipe)
      }
  }
}
