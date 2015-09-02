package commbank.coppersmith

import scala.reflect.runtime.universe.TypeTag

import au.com.cba.omnia.maestro.api.Field

import Feature._

object Patterns {

  // All features should be able to be defined in the following terms
  def general[S, V <: Value : TypeTag, FV <% V](
    namespace: Namespace,
    name:      Name,
    humanDescription: String,
    fType:     Type,
    entity:    S => EntityId,
    value:     S => Option[V],
    time:      S => Time
  ) =
    new Feature[S, V](FeatureMetadata(namespace, name, humanDescription, fType)) {
      def generate(source: S) = value(source).map(
        FeatureValue(entity(source), name, _, time(source))
      )
    }

  def pivot[S, V <: Value : TypeTag, FV <% V](
    namespace: Namespace,
    fType:     Type,
    entity:    S => EntityId,
    time:      S => Time,
    field:     Field[S, FV],
    humanDescription: String
  ) = general[S, V, FV](namespace, field.name, humanDescription, fType, entity, (s: S) => Option(field.get(s): V), time)
}
