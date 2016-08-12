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
import commbank.coppersmith.Feature.{Value, Type, Conforms}, Conforms.conforms_?
import commbank.coppersmith.tools.util.ObjectFinder

object MetadataMain {
  sealed trait FormatType
  case object PsvFormat extends FormatType
  case object JsonFormat extends FormatType

  def main(args:Array[String]) = {
    val (format, packagge, version) = args.take(2) match {
      case Array("--psv", pkg)                  => ( PsvFormat, pkg, 0)
      case Array("--json", pkg)                 => (JsonFormat, pkg, 0)
      case Array("--json", "--version", v, pkg) => (JsonFormat, pkg, v.toInt)
      case Array("--version", v, pkg)           => (JsonFormat, pkg, v.toInt)
      case Array(pkg)                           => (JsonFormat, pkg, 0)
      case _                                    => println("Invalid input"); sys.exit(1)
    }

    val metadataSets = ObjectFinder.findObjects[MetadataSet[Any]](packagge, "commbank.coppersmith").toList

    val allConforms: Set[Conforms[_, _]] = Conforms.allConforms

    // The repetition here is regrettable but getting the types without statically
    // knowing the formatter is really awkward

    val output: String = format match {
      case PsvFormat =>
        MetadataOutput.Psv.stringify(MetadataOutput.Psv.doOutput(metadataSets, allConforms))
      case JsonFormat =>
        if (!metadataSets.isEmpty) {
          if (version == 0) {
            MetadataOutput.Json0.stringify(MetadataOutput.Json0.doOutput(metadataSets, allConforms))
          } else if (version == 1) {
            MetadataOutput.Json1.stringify(MetadataOutput.Json0.doOutput(metadataSets, allConforms))
          } else {
            sys.error("Invalid JSON version received")
          }
        } else {
          ""
        }
    }
    print(output)
  }
}
