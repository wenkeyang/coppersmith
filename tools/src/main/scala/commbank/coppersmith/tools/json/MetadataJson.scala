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

package commbank.coppersmith.tools.json

import argonaut._

trait MetadataJson {
  def version: Int
}

object MetadataJson {
  def read(json: Json): Option[MetadataJson] = {
    val result:Option[MetadataJson] = readVersion(json).map {
      case 1 => MetadataJsonV1.read(json)
      case v => sys.error(s"Unsupported version: $v")
    } getOrElse MetadataJsonV0.read(json)
    result
  }

  def readVersion(json: Json) = for {
    field <- json.field("version")
    num <- field.number
    int <- num.toInt
  } yield int
}
