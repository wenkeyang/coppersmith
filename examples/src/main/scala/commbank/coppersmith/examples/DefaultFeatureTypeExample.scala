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

package commbank.coppersmith.examples

import commbank.coppersmith.api._, Coppersmith._
import commbank.coppersmith.examples.thrift.User


object DefaultFeatureTypeExample {
  val select = From[User].featureSetBuilder("ns", _.id)

  val feature = select(_.age == 50).asFeature(Nominal, "isFifty", "is fifty")

  val featureDef = select(_.age == 50).asFeature("isFifty_v2", "is fifty")

  // Won't compile: no default feature type:
  // val featureNoDef = select(_.age).asFeature("age", "age")
}
