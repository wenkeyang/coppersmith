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

import scala.reflect.runtime.universe.TypeTag

import au.com.cba.omnia.maestro.api.Field

import Feature._

object Patterns {

  // All features should be able to be defined in the following terms
  def general[S : TypeTag, V <: Value : TypeTag](
    namespace: Namespace,
    name:      Name,
    desc:      Description,
    fType:     Type,
    entity:    S => EntityId,
    value:     S => Option[V],
    range:     Option[Value.Range[V]]
  ) =
    new Feature[S, V](Metadata[S, V](namespace, name, desc, fType, range)) {
      def generate(source: S) = value(source).map(
        FeatureValue(entity(source), name, _)
      )
    }

  def pivot[S : TypeTag, V <: Value : TypeTag, FV <% V](
    namespace: Namespace,
    fType:     Type,
    entity:    S => EntityId,
    field:     Field[S, FV],
    desc:      Description,
    range:     Option[Value.Range[V]]
  ) = general[S, V](namespace, field.name, desc, fType, entity, (s: S) => Option(field.get(s): V), range)
}
