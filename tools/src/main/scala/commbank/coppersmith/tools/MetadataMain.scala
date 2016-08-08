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

package commbank.coppersmith.tools

import commbank.coppersmith.{MetadataOutput, MetadataSet}
import commbank.coppersmith.Feature.Conforms, Conforms.conforms_?
import commbank.coppersmith.tools.util.ObjectFinder

object MetadataMain {
  def main(args: Array[String]) = {
    val (format, packagge) = args.take(2) match {
      case Array("--psv", pkg) => (MetadataOutput.Psv, pkg)
      case Array("--json", pkg) => (MetadataOutput.JsonObject, pkg)
      case Array(pkg) => (MetadataOutput.JsonObject, pkg)
      case _ => println("Invalid input"); sys.exit(1)
    }

    val metadataSets = ObjectFinder
      .findObjects[MetadataSet[_]](packagge, "commbank.coppersmith")
    val allConforms = ObjectFinder.findObjects[Conforms[_, _]](
        args(0),
        "commbank.coppersmith",
        "au.com.cba.omnia")

    metadataSets.foreach { ms =>
      val metadataConformsSet = ms.metadata
        .map(m => (m, allConforms.find(c => conforms_?(c, m))))
        .toList
      val outputString =
        MetadataOutput.metadataString(metadataConformsSet, format)
      println(outputString)
    }
  }
}
