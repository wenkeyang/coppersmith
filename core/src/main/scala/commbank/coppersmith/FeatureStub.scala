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

import commbank.coppersmith.Feature._
import scala.reflect.runtime.universe.TypeTag

object FeatureStub {
  def apply[S : TypeTag, V  <: Value : TypeTag] = new FeatureStub[S, V]
}

/**
  *
  * @tparam S Feature source
  * @tparam V Value type
  */

class FeatureStub[S : TypeTag, V <: Value : TypeTag] {
  def asFeatureMetadata(
    featureType: Type,
    namespace:   Namespace,
    name:        Name,
    desc:        Description
  ): Metadata[S, V] = asFeatureMetadata(featureType, namespace, name, None, desc)

  def asFeatureMetadata(
    featureType: Type,
    namespace:   Namespace,
    name:        Name,
    range:       Option[Value.Range[V]],
    desc:        Description
  ): Metadata[S, V] = Metadata[S, V](namespace, name, desc, featureType, range)
}
