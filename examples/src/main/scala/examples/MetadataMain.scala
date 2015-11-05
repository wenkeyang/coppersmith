package examples

import commbank.coppersmith.{MetadataOutput, MetadataSet}
import commbank.coppersmith.util.ObjectFinder
import io.github.lukehutch.fastclasspathscanner.scanner.ClasspathFinder

object MetadataMain {
  def main(args:Array[String]) = {
    val metadataSets = ObjectFinder.findObjects[MetadataSet[_]]("examples", "commbank.coppersmith")

    metadataSets.foreach { ms =>
      val outputString = MetadataOutput.metadataString(ms, MetadataOutput.Markdown)
      println(outputString)
    }
  }
}
