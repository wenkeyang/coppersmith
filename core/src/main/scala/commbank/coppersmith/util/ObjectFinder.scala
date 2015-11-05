package commbank.coppersmith.util

import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner
import io.github.lukehutch.fastclasspathscanner.matchprocessor.SubclassMatchProcessor

import scala.collection.convert.WrapAsScala
import scala.reflect.ClassTag

import scalaz._, Scalaz._

object ObjectFinder {
  def findObjects[T : ClassTag](packages: String*): Set[T] = {
    val ct = implicitly[ClassTag[T]]
    val classNames: List[String] = WrapAsScala.collectionAsScalaIterable(
        new FastClasspathScanner(packages: _*).scan().getNamesOfClassesImplementing(ct.runtimeClass.getName)
    ).toList

    val objectInstances: List[T] = classNames.flatMap { cn =>
      val objClass = Class.forName(cn)
      val fields = objClass.getDeclaredFields
      fields.find(_.getName === "MODULE$") >>= { f => Option(f.get(null).asInstanceOf[T]) }
    }

    objectInstances.toSet
  }
}
