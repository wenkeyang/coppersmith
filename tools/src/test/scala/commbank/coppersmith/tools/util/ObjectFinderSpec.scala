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

import org.specs2.Specification

package commbank.coppersmith.tools.util {
  trait T
  trait U

  object A extends T
  object B extends T
  object C extends U

  trait P[A]
  trait Q[A] extends P[A]
  object D extends P[Int]
  object E extends Q[String]

  object F extends someothersillypackage.Outside

  trait V
  object Outer {
    object Nested extends V
  }

  object ObjectFinderSpec extends Specification {
    def is =
      s2"""
        Object finder
          Can find objects of a given trait $findObjects
          Can find objects of a given parameterised trait $findObjectsParam
          Can find objects of a trait from another package $findObjectsTraitOtherPackage
          Can find nested objects $findNestedObjects
      """

    def findObjects = Seq(
      ObjectFinder.findObjects[T]("commbank.coppersmith.tools.util") === Set(A, B),
      ObjectFinder.findObjects[U]("commbank.coppersmith.tools.util", "scala") === Set(C)
    )

    def findObjectsParam = Seq(
      ObjectFinder.findObjects[Q[_]]("commbank.coppersmith.tools.util") === Set(E),
      ObjectFinder.findObjects[P[_]]("commbank.coppersmith.tools.util") === Set(D, E)
    )

    def findObjectsTraitOtherPackage = Seq(
      // Note the need to add the trait package to the list of package. This sucks and
      // is a bug in fast-classpath-scanner.
      // Raised at https://github.com/lukehutch/fast-classpath-scanner/issues/29
      ObjectFinder.findObjects[someothersillypackage.Outside](
        "commbank.coppersmith.tools.util",
        "someothersillypackage"
      ) === Set(F)
    )

    def findNestedObjects = {
      ObjectFinder.findObjects[V]("commbank.coppersmith.tools.util") === Set(Outer.Nested)
    }
  }
}

package someothersillypackage {
  trait Outside
}
