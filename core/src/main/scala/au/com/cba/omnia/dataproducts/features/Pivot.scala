package au.com.cba.omnia.dataproducts.features

import au.com.cba.omnia.dataproducts.features.Feature.Namespace
import au.com.cba.omnia.maestro.macros.{Inspect, MacroSupport}
import com.twitter.scrooge.ThriftStruct

import scala.reflect.macros.whitebox.Context

object Pivot {
  def pivotThrift[A <: ThriftStruct](namespace:Namespace):Any = macro pivotImpl[A]
  
    //TODO: Really similar to maestro Fields. Refactor
  def pivotImpl[A <: ThriftStruct: c.WeakTypeTag](c: Context)(namespace:c.Expr[Namespace]) = {

    import c.universe._

    val typ        = c.universe.weakTypeOf[A]
    val entries    = Inspect.fields[A](c)



    val features = entries.map({
      case (method, field) =>
        val returnType = method.returnType
        val featureType = typeToFeatureType(c)(returnType)
        val valueType = typeToValueType(c)(returnType)

        val feature = q"""new au.com.cba.omnia.dataproducts.features.Feature[$typ] {
         type V = $returnType
         def namespace = $namespace
         def name = $field
         def generate(source:$typ):Option[au.com.cba.omnia.dataproducts.features.FeatureValue[$typ, $returnType]] = ???
         def featureType = $featureType
         def valueType = $valueType
         }"""

        q"""val ${TermName(field)} = $feature"""

    })
    val r =q"class FeaturesWrapper { ..$features }; new FeaturesWrapper {}"
      println(r)
    c.Expr(r)
  }

  def typeToFeatureType(c:Context)(typ: c.universe.Type) = {
    import c.universe._
    q"au.com.cba.omnia.dataproducts.features.Feature.Type.Categorical"
  }
  def typeToValueType(c:Context)(typ: c.universe.Type) = {
    import c.universe._
    q"au.com.cba.omnia.dataproducts.features.Feature.ValueType.Str"
  }
}
