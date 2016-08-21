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

package commbank.coppersmith.tools.util

import scala.collection.convert.decorateAsScala.collectionAsScalaIterableConverter
import scala.reflect.ClassTag

import scalaz.Scalaz._

import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner

object ObjectFinder {
  def findObjects[T : ClassTag](packages: String*): Set[T] = {
    val cls = implicitly[ClassTag[T]].runtimeClass
    val scanner = new FastClasspathScanner(packages: _*).scan()
    val classNames: Iterable[String] =
      if (cls.isInterface) {
        scanner.getNamesOfClassesImplementing(cls).asScala
      } else {
        scanner.getNamesOfSubclassesOf(cls).asScala
      }

    val objectInstances: Iterable[T] = classNames.flatMap { cn =>
      val objClass = Class.forName(cn)
      val fields = objClass.getDeclaredFields
      fields.find(_.getName === "MODULE$") >>= { f => Option(f.get(null).asInstanceOf[T]) }
    }

    objectInstances.toSet
  }
}
