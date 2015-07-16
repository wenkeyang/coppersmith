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
        val featureValueType = typeToFeatureValueType(c)(returnType)

        val feature =
          q"""
              import au.com.cba.omnia.dataproducts.features._

              val featureMetadata = FeatureMetadata($namespace, $field, Feature.Type.Categorical)

              new Feature[$typ, $featureValueType](featureMetadata) { self =>

                def generate(source: $typ):Option[FeatureValue[$typ, $featureValueType]] = {
                  val v = source.$method
                  Some(FeatureValue(self, "", Feature.Value(v), ""))
                }


             }"""

        q"""val ${TermName(field)} : Feature[$typ, $featureValueType] = $feature"""

    })
    val r =q"class FeaturesWrapper { ..$features }; new FeaturesWrapper {}"
//      println(r)
    c.Expr(r)
  }

  def typeToFeatureValueType(c:Context)(t: c.universe.Type)= {
    import c.universe._
    if (t =:= typeOf[String] || t =:= typeOf[Option[String]]) {
      typeOf[Feature.Value.Str]
    } else if (t =:= typeOf[Int] || t =:= typeOf[Option[Int]]) {
      typeOf[Feature.Value.Integral]
    } else {
     throw new RuntimeException(s"no value type for $t" )
    }
  }

}
