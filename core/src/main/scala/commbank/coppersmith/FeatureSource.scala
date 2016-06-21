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

import scalaz.syntax.functor.ToFunctorOps
import scalaz.syntax.std.option.ToOptionIdOps

import shapeless._
import shapeless.ops.hlist._

import commbank.coppersmith.Join.{CompleteJoinHlFeatureSource, IncompleteJoinedHl, CompleteJoinHl}

import util.Conversion

abstract class FeatureSource[S, FS <: FeatureSource[S, FS]](filter: Option[S => Boolean] = None) {
  self: FS =>

  def filter(p: S => Boolean): FS = copyWithFilter(filter.map(f => (s: S) => f(s) && p(s)).orElse(p.some))

  def copyWithFilter(filter: Option[S => Boolean]): FS

  def bind[P[_] : Lift](binder: SourceBinder[S, FS, P]): BoundFeatureSource[S, P] = {
    implicitly[Lift[P]].liftBinder(self, binder, filter)
  }

  def withContext[C] = ContextFeatureSource[S, C, FS](this)
}

// TODO: See if it is possible to extend FeatureSource directly here to remove
// additional FeatureBuilderSourceInstances.fromCFS implicit method
case class ContextFeatureSource[S, C, FS <: FeatureSource[S, FS]](
  underlying: FeatureSource[S, FS],
  filter:     Option[((S, C)) => Boolean] = None
) {
  def bindWithContext[P[_] : Lift](
    binder: SourceBinder[S, FS, P],
    ctx:    C
  ): BoundFeatureSource[(S, C), P] = {
    new BoundFeatureSource[(S, C), P] {
      def load: P[(S, C)] = {
        val lift = implicitly[Lift[P]]
        val loaded = lift.functor.map(underlying.bind(binder).load)((_, ctx))
        filter.map(lift.liftFilter(loaded, _)).getOrElse(loaded)
      }
    }
  }

  def filter(p: ((S, C)) => Boolean) =
    copy(filter = filter.map(f => (sc: (S, C)) => f(sc) && p(sc)).orElse(p.some))
}

abstract class BoundFeatureSource[S, P[_] : Lift] {
  def load: P[S]
}

trait SourceBinder[S, U, P[_]] {
  def bind(underlying: U): P[S]
}

object SourceBinder extends SourceBinderInstances

trait SourceBinderInstances extends commbank.coppersmith.generated.GeneratedBindings {
  def from[S, P[_] : Lift](dataSource: DataSource[S, P]) = FromBinder(dataSource)

  def join[L, R, J : Ordering, P[_] : Lift](
    leftSrc:  DataSource[L, P],
    rightSrc: DataSource[R, P]
  ) = joinInner(leftSrc, rightSrc) // From GeneratedBindings

  def leftJoin[L, R, J : Ordering, P[_] : Lift](
    leftSrc:  DataSource[L, P],
    rightSrc: DataSource[R, P]
  ) = joinLeft(leftSrc, rightSrc) // From GeneratedBindings

  def joinMulti[  //These come from parameters
    P[_] : Lift,
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

    TypesTuple <: Product

    // FIXME: CompleteJoinHlFeatureSource instance is only required here to infer
    // implicit ToNextPipe param - it is not used in creating the SourceBinder
  ](in: Tuple, j: CompleteJoinHlFeatureSource[Types, Joins, TypesTuple])
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
     tnp: ToNextPipe.Aux[PipesTail, TypesTail, NextPipes],

     // Proof that head is a pipe
     pipeIsHead: P[HeadElement] =:= PipesHead,
     headIsPipe: PipesHead =:= P[HeadElement],

     //Folding to do actual join
     zipper : Zip.Aux[NextPipes :: Joins :: HNil, Zipped],
     leftFolder: LeftFolder.Aux[Zipped,P[HeadElement :: HNil], JoinFolders.joinFolder.type, P[Types]],

     //and finally turning to tuple
     typesTupler: Tupler.Aux[Types, TypesTuple]

   ) = // inIsCons & typesIsCons are currently ambiguous, hence explicitly providing implicits
    MultiJoinedBinder(in)(implicitly[Lift[P]], dshlGen, mapper, pipesConv, inIsCons, typesIsCons,
                          tnp, pipeIsHead, headIsPipe, zipper, leftFolder, typesTupler)
}

case class FromBinder[S, P[_]](src: DataSource[S, P]) extends SourceBinder[S, From[S], P] {
  def bind(from: From[S]): P[S] = src.load
}

case class MultiJoinedBinder[
  //These come from parameters
  P[_] : Lift,
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

  TypesTuple <: Product
](in: Tuple)
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
   tnp: ToNextPipe.Aux[PipesTail, TypesTail, NextPipes],

   // Proof that head is a pipe
   pipeIsHead: P[HeadElement] =:= PipesHead,
   headIsPipe: PipesHead =:= P[HeadElement],

   //Folding to do actual join
   zipper : Zip.Aux[NextPipes :: Joins :: HNil, Zipped],
   leftFolder: LeftFolder.Aux[Zipped,P[HeadElement :: HNil], JoinFolders.joinFolder.type, P[Types]],

  //and finally turning to tuple
  typesTupler: Tupler.Aux[Types, TypesTuple]
) extends SourceBinder[TypesTuple, CompleteJoinHlFeatureSource[Types, Joins, TypesTuple], P] {
  def bind(fs: CompleteJoinHlFeatureSource[Types, Joins, TypesTuple]): P[TypesTuple] = {
    val lift = implicitly[Lift[P]]
    val inPipes: PipesTuple = DataSourcesToPipes.convert(in)
    val joined = lift.liftMultiwayJoin(fs.join)(inPipes)
    val filtered = fs.filter.fold(joined)(f => lift.liftFilter(joined, f))
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
