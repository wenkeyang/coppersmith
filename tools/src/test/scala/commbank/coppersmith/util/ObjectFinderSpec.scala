


package commbank.coppersmith.util {
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

  object ObjectFinderSpec extends Specification {
    def is =
      s2"""
        Object finder
          Can find objects of a given trait $findObjects
          Can find objects of a given parameterised trait $findObjectsParam
          Can find objects of a trait from another package $findObjectsTraitOtherPackage
      """


    def findObjects = Seq(
      ObjectFinder.findObjects[T]("commbank.coppersmith.util") === Set(A, B),
      ObjectFinder.findObjects[U]("commbank.coppersmith.util", "scala") === Set(C)
    )

    def findObjectsParam = Seq(
      ObjectFinder.findObjects[Q[_]]("commbank.coppersmith.util") === Set(E),
      ObjectFinder.findObjects[P[_]]("commbank.coppersmith.util") === Set(D, E)
    )

    def findObjectsTraitOtherPackage = Seq(
      // Note the need to add the trait package to the list of package. This sucks and
      // is a bug in fast-classpath-scanner.
      // Raised at https://github.com/lukehutch/fast-classpath-scanner/issues/29
      ObjectFinder.findObjects[someothersillypackage.Outside]("commbank.coppersmith.util", "someothersillypackage") === Set(F)
    )
  }

}

package someothersillypackage {
  trait Outside
}