package commbank.coppersmith

import commbank.coppersmith.Join.{CompleteJoinHl}
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
  def liftJoin[A, B, J : Ordering]
    (joined: Joined[A, B, J, (A, B) ])
    (a:P[A], b: P[B])
    (implicit functor: Functor[P]): P[(A, B)] = {
    innerJoinNext((l: A :: HNil) =>
      joined.left(l.head), joined.right)(a.map(_ :: HNil), b).map(_.tupled)
  }

  def liftLeftJoin[A, B, J : Ordering]
    (joined: Joined[A, B, J, (A, Option[B])])
    (a: P[A], b: P[B])(implicit functor: Functor[P]): P[(A, Option[B])] = {
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
   (implicit inToHlist  : Generic.Aux[InTuple, InHList],
             inIsCons   : IsHCons.Aux[InHList, InHeadType, InTail],
             typesIsCons: IsHCons.Aux[Types, TypesHead, TypesTail],
             tnp        : ToNextPipe.Aux[InTail, TypesTail, NextPipes],
             pEl1       : P[InHeadElementType] =:= InHeadType,
             pEl2       : InHeadType =:= P[InHeadElementType],
             zipper     : Zip.Aux[NextPipes :: Joins :: HNil, Zipped],
             leftFolder : LeftFolder.Aux[Zipped, P[InHeadElementType :: HNil],
                                            memory.joinFolder.type, P[Types]],
             tupler     : Tupler.Aux[Types, OutTuple],
             pFunctor   : Functor[P])
  : P[OutTuple] = {
    val inHl : InHList = inToHlist.to(in)
    val inHead: P[InHeadElementType] = pEl2(inHl.head)
    val inTail: InTail = inHl.tail
    val tailWithJoined: NextPipes = tnp(inTail)
    val zipped: Zipped = tailWithJoined zip join.joins
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
    ](implicit prepend: Prepend.Aux[SoFar, Option[Next] :: HNil, OutInner]) =
      at[P[SoFar], (NextPipe[Next, Option[Next]], (SoFar => J, Next => J))] {
        (acc: P[SoFar], pipeWithJoin: (NextPipe[Next, Option[Next]], (SoFar => J, Next => J) )) =>
          val fnSoFar: SoFar => J = pipeWithJoin._2._1
          val fnNext: Next   => J = pipeWithJoin._2._2
          leftJoinNext[SoFar, Next, J, OutInner](fnSoFar, fnNext)(acc, pipeWithJoin._1.pipe)
      }
  }


  def liftBinder[S, U <: FeatureSource[S, U], B <: SourceBinder[S, U, P]](
    underlying: U,
    binder: B,
    filter: Option[S => Boolean]
  ): BoundFeatureSource[S, P]

  object comb extends Poly2 {
    implicit def default[U, V] = at[P[U], V] ( (u: P[U], _: V) => NextPipe[U, V](u) )
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

  case class NextPipe[Next, JoinType](pipe: P[Next])

  trait ToNextPipe[L <: HList, R <: HList] extends DepFn1[L]

  object ToNextPipe {
    def apply[L <: HList, R <: HList](implicit tnp: ToNextPipe[L, R]) = tnp

    type Aux[L <: HList, R <: HList, Out0] = ToNextPipe[L, R] { type Out = Out0 }

    implicit def toNextPipe[L <: HList, R <: HList, Out0 <: HList]
      (implicit nhl     : NullHList[R],
                zipWith : ZipWith.Aux[L, R, comb.type, Out0]): ToNextPipe.Aux[L, R, Out0] = new ToNextPipe[L, R] {
      type Out = Out0

      def apply(l: L) = {
        val nulls: R = nhl()

        l.zipWith(nulls)(comb)
      }
    }
  }

}
