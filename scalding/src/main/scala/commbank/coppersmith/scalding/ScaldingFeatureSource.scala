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

package commbank.coppersmith.scalding

import com.twitter.scalding._

import commbank.coppersmith.{BoundFeatureSource, FeatureSource, SourceBinder}

case class ScaldingBoundFeatureSource[S, U <: FeatureSource[S, U]](
    underlying: U,
    binder: SourceBinder[S, U, TypedPipe],
    filter: Option[S => Boolean]
) extends BoundFeatureSource[S, TypedPipe] {
  def load: TypedPipe[S] = {
    val pipe = binder.bind(underlying)
    filter.map(f => pipe.filter(f)).getOrElse(pipe)
  }
}
