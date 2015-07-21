package au.com.cba.omnia.dataproducts.features

import scala.reflect.runtime.universe.TypeTag

import au.com.cba.omnia.maestro.api.Field

import Feature._

object Patterns {

  // All features should be able to be defined in the following terms
  def general[S, V <: Value : TypeTag, FV <% V](
    namespace: Namespace,
    name:      Name,
    fType:     Type,
    entity:    S => EntityId,
    value:     S => Option[V],
    time:      S => Time
  ) =
    new Feature[S, V](FeatureMetadata(namespace, name, fType)) {
      def generate(source: S) = value(source).map(
        FeatureValue[S, V](this, entity(source), _, time(source))
      )
    }

  def pivot[S, V <: Value : TypeTag, FV <% V](
    namespace: Namespace,
    fType:     Type,
    entity:    S => EntityId,
    time:      S => Time,
    field:     Field[S, FV]
  ) = general[S, V, FV](namespace, field.name, fType, entity, (s: S) => Option(field.get(s)), time)
}
