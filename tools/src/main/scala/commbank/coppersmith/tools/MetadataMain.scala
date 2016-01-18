package commbank.coppersmith.tools

import commbank.coppersmith.{MetadataOutput, MetadataSet}
import commbank.coppersmith.tools.util.ObjectFinder

object MetadataMain {
  def main(args:Array[String]) = {
    val metadataSets = ObjectFinder.findObjects[MetadataSet[_]](args(0), "commbank.coppersmith")

    metadataSets.foreach { ms =>
      val outputString = MetadataOutput.metadataString(ms, MetadataOutput.LuaTable)
      println(outputString)
    }
  }
}
