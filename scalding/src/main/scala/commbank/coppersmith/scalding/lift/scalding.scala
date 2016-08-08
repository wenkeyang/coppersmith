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

package commbank.coppersmith.scalding.lift

import com.twitter.scalding._

import commbank.coppersmith._, Feature.Value
import commbank.coppersmith.scalding.ScaldingBoundFeatureSource
import commbank.coppersmith.scalding.generated.GeneratedScaldingLift

import ScaldingScalazInstances.typedPipeFunctor

trait ScaldingLift extends Lift[TypedPipe] with GeneratedScaldingLift {

  def lift[S, V <: Value](f: Feature[S, V])(s: TypedPipe[S]): TypedPipe[FeatureValue[V]] = {
    s.flatMap(s => f.generate(s))
  }

  def lift[S](fs: FeatureSet[S])(s: TypedPipe[S]): TypedPipe[FeatureValue[_]] = {
    s.flatMap(s => fs.generate(s))
  }

  def liftBinder[S, U <: FeatureSource[S, U], B <: SourceBinder[S, U, TypedPipe]](
      underlying: U,
      binder: B,
      filter: Option[S => Boolean]
  ) = ScaldingBoundFeatureSource(underlying, binder, filter)

  def liftFilter[S](p: TypedPipe[S], f: S => Boolean) = p.filter(f)
}

object scalding extends ScaldingLift
