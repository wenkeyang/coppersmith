package commbank.coppersmith.tools

import commbank.coppersmith.{MetadataOutput, MetadataSet}
import commbank.coppersmith.Feature.Conforms, Conforms.conforms_?
import commbank.coppersmith.tools.util.ObjectFinder

object MetadataMain {
  def main(args:Array[String]) = {
    val (format, packagge) = args.take(2) match {
      case Array("--psv", pkg)  => ( MetadataOutput.HydroPsv, pkg)
      case Array("--json", pkg) => ( MetadataOutput.JsonObject, pkg)
      case Array(pkg)           => ( MetadataOutput.JsonObject, pkg)
      case _                    => println("Invalid input"); sys.exit(1)
    }

    val metadataSets = ObjectFinder.findObjects[MetadataSet[_]](packagge, "commbank.coppersmith")
    val allConforms =
      ObjectFinder.findObjects[Conforms[_, _]](args(0), "commbank.coppersmith", "au.com.cba.omnia")

    metadataSets.foreach { ms =>
      val metadataConformsSet = ms.metadata.map(m => (m, allConforms.find(c => conforms_?(c, m)))).toList
      val outputString = MetadataOutput.metadataString(metadataConformsSet, format)
      println(outputString)
    }
  }
}
