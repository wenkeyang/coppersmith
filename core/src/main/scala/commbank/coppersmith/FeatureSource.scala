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
}

case class FromBinder[S, P[_]](src: DataSource[S, P]) extends SourceBinder[S, From[S], P] {
  def bind(from: From[S]): P[S] = src.load
}
