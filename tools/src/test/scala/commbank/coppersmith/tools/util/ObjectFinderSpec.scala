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
  trait TT
  trait UT
  class TC
  class UC

  object A extends TC with TT
  object B extends TC with TT
  object C extends UC with UT

  trait PT[A]
  trait QT[A] extends PT[A]
  class PC[A]
  class QC[A] extends PC[A]
  object D extends PC[Int] with PT[Int]
  object E extends QC[String] with QT[String]

  object F extends someothersillypackage.Outside

  trait VT
  class VC
  object Outer {
    object Nested extends VC with VT
  }

  object ObjectFinderSpec extends Specification {
    def is =
      s2"""
        Object finder
          Can find objects of a given type $findObjects
          Can find objects of a given parameterised type $findObjectsParam
          Can find objects of a type from another package $findObjectsTraitOtherPackage
          Can find nested objects $findNestedObjects
      """

    def findObjects = Seq(
      ObjectFinder.findObjects[TT]("commbank.coppersmith.tools.util") === Set(A, B),
      ObjectFinder.findObjects[TC]("commbank.coppersmith.tools.util") === Set(A, B),
      ObjectFinder.findObjects[UT]("commbank.coppersmith.tools.util", "scala") === Set(C),
      ObjectFinder.findObjects[UC]("commbank.coppersmith.tools.util", "scala") === Set(C)
    )

    def findObjectsParam = Seq(
      ObjectFinder.findObjects[QT[_]]("commbank.coppersmith.tools.util") === Set(E),
      ObjectFinder.findObjects[QC[_]]("commbank.coppersmith.tools.util") === Set(E),
      ObjectFinder.findObjects[PT[_]]("commbank.coppersmith.tools.util") === Set(D, E),
      ObjectFinder.findObjects[PC[_]]("commbank.coppersmith.tools.util") === Set(D, E)
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

    def findNestedObjects = Seq(
      ObjectFinder.findObjects[VT]("commbank.coppersmith.tools.util") === Set(Outer.Nested),
      ObjectFinder.findObjects[VC]("commbank.coppersmith.tools.util") === Set(Outer.Nested)
    )
  }
}

package someothersillypackage {
  trait Outside
}
