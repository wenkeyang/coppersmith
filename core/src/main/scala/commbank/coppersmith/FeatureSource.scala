package commbank.coppersmith

import commbank.coppersmith.Join.{CompleteJoinHlFeatureSource, IncompleteJoinedHl, CompleteJoinHl}
import shapeless._
import shapeless.ops.hlist._
import scalaz.Functor
import scalaz.syntax.std.option.ToOptionIdOps

import util.Conversion

abstract class FeatureSource[S, FS <: FeatureSource[S, FS]](filter: Option[S => Boolean] = None) {
  self: FS =>

  def filter(p: S => Boolean): FS = copyWithFilter(filter.map(f => (s: S) => f(s) && p(s)).orElse(p.some))

  def copyWithFilter(filter: Option[S => Boolean]): FS

  def bind[P[_] : Lift](binder: SourceBinder[S, FS, P]): BoundFeatureSource[S, P] = {
    implicitly[Lift[P]].liftBinder(self, binder, filter)
  }
}

trait BoundFeatureSource[S, P[_]] {
  def load: P[S]
}

trait SourceBinder[S, U, P[_]] {
  def bind(underlying: U): P[S]
}

object SourceBinder extends SourceBinderInstances

trait SourceBinderInstances {
  def from[S, P[_] : Lift](dataSource: DataSource[S, P]) = FromBinder(dataSource)

  def join[L, R, J : Ordering, P[_] : Lift : Functor]
    (leftSrc: DataSource[L, P], rightSrc: DataSource[R, P]) =
    JoinedBinder(leftSrc, rightSrc)

  def leftJoin[L, R, J : Ordering, P[_] : Lift : Functor]
    (leftSrc: DataSource[L, P], rightSrc: DataSource[R, P]) =
    LeftJoinedBinder(leftSrc, rightSrc)

  def joinMulti[  //These come from parameters
    P[_] : Functor : Lift,
    Tuple <: Product,
    Types <: HList,
    Joins <: HList,

    //For mapping data sources to pipes
    DSHL <:HList,
    PipesHL <: HList,
    PipesTuple <: Product,

    //Proving that pipes is cons
    PipesHead,
    PipesTail <: HList,

    //Proving that types is cons
    TypesHead,
    TypesTail <: HList,

    // Zip everything together
    NextPipes <: HList,

    //Element of head
    HeadElement,

    // (NextPipes, Joins)
    Zipped <: HList,

    TypesTuple <: Product]
    ( in: Tuple, j: CompleteJoinHlFeatureSource[Types, Joins, TypesTuple])
    (implicit
     //Map data source tuple to pipes tuple
     dshlGen     : Generic.Aux[Tuple, DSHL],
     mapper      : Mapper.Aux[DataSourcesToPipes.dataSourceToPipe.type, DSHL, PipesHL],
     pipesConv   : Conversion.Aux[PipesTuple, PipesHL],

     //Prove that pipes is cons
     inIsCons: IsHCons.Aux[PipesHL, PipesHead, PipesTail],

     //Prove that types is cons
     typesIsCons: IsHCons.Aux[Types, TypesHead, TypesTail],

     // Zip everything together
     tnp: ToNextPipe.Aux[PipesTail, TypesTail ,NextPipes],

     // Proof that head is a pipe
     pipeIsHead: P[HeadElement] =:= PipesHead,
     headIsPipe: PipesHead =:= P[HeadElement],

     //Folding to do actual join
     zipper : Zip.Aux[NextPipes :: Joins :: HNil, Zipped],
     leftFolder: LeftFolder.Aux[Zipped,P[HeadElement :: HNil], JoinFolders.joinFolder.type, P[Types]],

     //and finally turning to tuple
     typesTupler: Tupler.Aux[Types, TypesTuple]

      ) = MultiJoinedBinder(in, j.join, j.filter)
}

case class FromBinder[S, P[_]](src: DataSource[S, P]) extends SourceBinder[S, From[S], P] {
  def bind(from: From[S]): P[S] = src.load
}

case class JoinedBinder[L, R, J : Ordering, P[_] : Lift : Functor](
  leftSrc:  DataSource[L, P],
  rightSrc: DataSource[R, P]
) extends SourceBinder[(L, R), Joined[L, R, J, (L, R)], P] {
  def bind(j: Joined[L, R, J, (L, R)]): P[(L, R)] = {
    implicitly[Lift[P]].liftJoin(j)(leftSrc.load, rightSrc.load)
  }
}

case class LeftJoinedBinder[L, R, J : Ordering, P[_] : Lift : Functor](
  leftSrc:  DataSource[L, P],
  rightSrc: DataSource[R, P]
) extends SourceBinder[(L, Option[R]), Joined[L, R, J, (L, Option[R])], P] {
  def bind(j: Joined[L, R, J, (L, Option[R])]): P[(L, Option[R])] = {
    implicitly[Lift[P]].liftLeftJoin(j)(leftSrc.load, rightSrc.load)
  }
}

case class MultiJoinedBinder[
  //These come from parameters
  P[_] : Functor : Lift,
  Tuple <: Product,
  Types <: HList,
  Joins <: HList,

  //For mapping data sources to pipes
  DSHL <:HList,
  PipesHL <: HList,
  PipesTuple <: Product,

  //Proving that pipes is cons
  PipesHead,
  PipesTail <: HList,

  //Proving that types is cons
  TypesHead,
  TypesTail <: HList,

  // Zip everything together
  NextPipes <: HList,

  //Element of head
  HeadElement,

  // (NextPipes, Joins)
  Zipped <: HList,

  TypesTuple <: Product]
  (in: Tuple, j: CompleteJoinHl[Types, Joins], filter: Option[TypesTuple => Boolean])
  (implicit
   //Map data source tuple to pipes tuple
   dshlGen         : Generic.Aux[Tuple, DSHL],
   mapper          : Mapper.Aux[DataSourcesToPipes.dataSourceToPipe.type, DSHL, PipesHL],
   pipesConversion : Conversion.Aux[PipesTuple, PipesHL],

    //Prove that pipes is cons
   inIsCons: IsHCons.Aux[PipesHL, PipesHead, PipesTail],

   //Prove that types is cons
   typesIsCons: IsHCons.Aux[Types, TypesHead, TypesTail],

   // Zip everything together
   tnp: ToNextPipe.Aux[PipesTail, TypesTail ,NextPipes],

   // Proof that head is a pipe
   pipeIsHead: P[HeadElement] =:= PipesHead,
   headIsPipe: PipesHead =:= P[HeadElement],

   //Folding to do actual join
   zipper : Zip.Aux[NextPipes :: Joins :: HNil, Zipped],
   leftFolder: LeftFolder.Aux[Zipped,P[HeadElement :: HNil], JoinFolders.joinFolder.type, P[Types]],

  //and finally turning to tuple
  typesTupler: Tupler.Aux[Types, TypesTuple]

  ) extends SourceBinder[TypesTuple, Unit, P] {
  //in the case of multiway joins, binding needs to be more eager. Otherwise type inference breaks. Putting in
  //Unit placeholder in bind
  def bind(u: Unit): P[TypesTuple] = {
    val lift = implicitly[Lift[P]]
    val inPipes: PipesTuple = DataSourcesToPipes.convert(in)
    val joined = lift.liftMultiwayJoin(j)(inPipes)
    val filtered = filter.fold(joined)(f => lift.liftFilter(joined, f))
    filtered
  }
}

object DataSourcesToPipes {
  def convert[
    P[_],
    DS <: Product,
    Pipes <: Product,
    PipesHL <: HList,
    DSHL <: HList
  ]
    (ds: DS)
    (implicit dsGen       : Generic.Aux[DS, DSHL],
              mapper      : Mapper.Aux[dataSourceToPipe.type, DSHL, PipesHL],
              pipesTupler : Conversion.Aux[Pipes, PipesHL]): Pipes = {
    val dshl = dsGen.to(ds)
    val mapped = dshl.map(dataSourceToPipe)
    pipesTupler.from(mapped)
  }
  object dataSourceToPipe extends Poly1 {

    implicit def extractPipe[S, P[_]] = at[DataSource[S,P]] { (ds: DataSource[S, P]) =>
      ds.load
    }
  }

}


