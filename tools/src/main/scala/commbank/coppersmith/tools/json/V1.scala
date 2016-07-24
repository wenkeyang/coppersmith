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

import argonaut._, Argonaut._

import MetadataJsonV1._
import commbank.coppersmith.tools.json.Util._

case class MetadataJsonV1(version: Int, featureSets: List[FeatureSetMetadataV1]) extends MetadataJson {
  require(version == 1)
}

case class FeatureSetMetadataV1(featureSet: String, features: List[FeatureMetadataV1])

case class FeatureMetadataV1(namespace: String,
                             name: String,
                             description: String,
                             sourceType: String,
                             typesConform: Boolean,
                             valueType: String,
                             featureType: String, range: Option[RangeV1])

object CodecsV1 {
  implicit lazy val decodeMetadataJsonV1: CodecJson[MetadataJsonV1] =
    casecodec2(MetadataJsonV1.apply, MetadataJsonV1.unapply)("version", "featureSets")

  implicit lazy val decodeFeatureSetMetadataV1: CodecJson[FeatureSetMetadataV1] =
    casecodec2(FeatureSetMetadataV1.apply, FeatureSetMetadataV1.unapply)("featureSet", "features")

  implicit lazy val decodeFeatureMetadataV1: CodecJson[FeatureMetadataV1] =
    CodecJson.derived(stripNullValuesFromObjects(EncodeJson.derive)("range"), DecodeJson.derive)

  implicit lazy val rangeV1Decode = CodecsV0.rangeV0Decode
  implicit lazy val rangeV1Encode = CodecsV0.rangeV0Encode
}

object MetadataJsonV1 {
  type RangeV1 = RangeV0

  import CodecsV1._

  def read(json: Json): Option[MetadataJsonV1] = json.as[MetadataJsonV1].toOption
  def write(md: MetadataJsonV1): Json = md.asJson
}
