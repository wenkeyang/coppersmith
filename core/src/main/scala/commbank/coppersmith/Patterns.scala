package commbank.coppersmith

import scala.reflect.runtime.universe.TypeTag

import au.com.cba.omnia.maestro.api.Field

import Feature._

object Patterns {

  // All features should be able to be defined in the following terms
  def general[S : TypeTag, V <: Value : TypeTag, FV <% V](
    namespace: Namespace,
    name:      Name,
    desc:      Description,
    fType:     Type,
    entity:    S => EntityId,
    value:     S => Option[V]
  ) =
    new Feature[S, V](Metadata[S, V](namespace, name, desc, fType)) {
      def generate(source: S) = value(source).map(
        FeatureValue(entity(source), name, _)
      )
    }

  def pivot[S : TypeTag, V <: Value : TypeTag, FV <% V](
    namespace: Namespace,
    fType:     Type,
    entity:    S => EntityId,
    field:     Field[S, FV],
    desc:      Description
  ) = general[S, V, FV](namespace, field.name, desc, fType, entity, (s: S) => Option(field.get(s): V))
}
