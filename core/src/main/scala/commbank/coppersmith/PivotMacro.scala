package commbank.coppersmith

import commbank.coppersmith.Feature._
import au.com.cba.omnia.maestro.macros.{Inspect, MacroSupport}
import com.twitter.scrooge.ThriftStruct

import scala.reflect.macros.whitebox.Context

object PivotMacro {
  def pivotThrift[A <: ThriftStruct](
    namespace:Namespace,
    entity: A => EntityId,
    time: A => Time
  ): Any = macro pivotImpl[A]

  def pivotImpl[A <: ThriftStruct: c.WeakTypeTag]
    (c: Context)
    (namespace:c.Expr[Namespace],
     entity:    c.Expr[A => EntityId],
     time:      c.Expr[A => Time]) = {

    import c.universe._

    val typ        = c.universe.weakTypeOf[A]
    val entries    = Inspect.info[A](c)

    val features = entries.map({
      case (int, field, method) =>
        val returnType = method.returnType
        val featureValueType = typeToFeatureValueType(c)(returnType)
        val mapperFn = typeMapper(c)(returnType)
        val continuous = isContinuous(c)(returnType)
        val fieldDescription = s"Feature auto-pivoted from ${typ.typeSymbol.toString}.${field}"
        val feature =
          q"""{
              import commbank.coppersmith._, Feature.Metadata

              val featureMetadata = Metadata[$typ, $featureValueType](
                  $namespace, ${field.toLowerCase}, $fieldDescription,
                  ${ if(continuous) q"Feature.Type.Continuous" else q"Feature.Type.Categorical"})

              new Feature[$typ, $featureValueType](featureMetadata) { self =>

                def generate(source: $typ):Option[FeatureValue[$featureValueType]] = {
                  val v = source.$method
                  Some(FeatureValue($entity(source),
                                    ${field.toLowerCase},
                                    Feature.Value.$mapperFn(v),
                                    $time(source)))
                }
             }}"""

        q"val ${TermName(field)} : Feature[$typ, $featureValueType] = $feature"
    })

      val featureRefs = entries.map({
        case (position, field, name) =>
          val n = TermName(field)
          q"$n"
      })

    val r =
      q"""class FeaturesWrapper extends PivotFeatureSet[$typ] {
          def namespace = $namespace
          def features = List(..$featureRefs)
          def entity(s: $typ) = $entity(s)
          def time(s: $typ) = $time(s)
         ..$features
         };
         new FeaturesWrapper {}
        """
    c.Expr(r)
  }

  def isContinuous(c:Context)(t:c.universe.Type) = {
    import c.universe._
    t =:= typeOf[Double] || t =:= typeOf[Option[Double]]
  }

  def typeMapper(c:Context)(t:c.universe.Type) = {
    import c.universe._

    if (t =:= typeOf[String]) {
      TermName("fromString")
    } else if (t =:= typeOf[Option[String]]) {
      TermName("fromOString")
    } else if (t =:= typeOf[Int]) {
      TermName("fromInt")
    } else if (t =:= typeOf[Option[Int]]) {
      TermName("fromOInt")
    } else if (t =:= typeOf[Double]) {
      TermName("fromDouble")
    } else if (t =:= typeOf[Option[Double]]) {
      TermName("fromODouble")
    } else if (t =:= typeOf[Long]) {
      TermName("fromLong")
    } else if (t =:= typeOf[Option[Long]]) {
      TermName("fromOLong")
    } else {
      throw new RuntimeException(s"no type mapper for $t" )
    }
  }

  def typeToFeatureValueType(c:Context)(t: c.universe.Type)= {
    import c.universe._
    if (t =:= typeOf[String] || t =:= typeOf[Option[String]]) {
      typeOf[Feature.Value.Str]
    } else if (t =:= typeOf[Int] || t =:= typeOf[Option[Int]]) {
      typeOf[Feature.Value.Integral]
    } else if (t =:= typeOf[Long] || t =:= typeOf[Option[Long]]) {
      typeOf[Feature.Value.Integral]
    } else if (t =:= typeOf[Double] || t =:= typeOf[Option[Double]]) {
      typeOf[Feature.Value.Decimal]
    } else {
     throw new RuntimeException(s"no value type for $t" )
    }
  }

}
