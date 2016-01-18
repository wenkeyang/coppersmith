package commbank.coppersmith.tools

import commbank.coppersmith.{MetadataOutput, MetadataSet}
import commbank.coppersmith.tools.util.ObjectFinder

object MetadataMain {
  def main(args:Array[String]) = {
    val (format, packagge) = args.take(2) match {
      case Array("--psv", pkg)  => ( MetadataOutput.HydroPsv, pkg)
      case Array("--lua", pkg)  => ( MetadataOutput.LuaTable, pkg)
      case Array(pkg)           => ( MetadataOutput.LuaTable, pkg)
      case _                    => println("Invalid input"); sys.exit(1)
    }

    val metadataSets = ObjectFinder.findObjects[MetadataSet[_]](packagge, "commbank.coppersmith")

    metadataSets.foreach { ms =>
      val outputString = MetadataOutput.metadataString(ms, format)
      println(outputString)
    }
  }
}
