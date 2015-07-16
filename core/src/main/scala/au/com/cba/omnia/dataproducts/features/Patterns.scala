package au.com.cba.omnia.dataproducts.features

import scala.reflect.runtime.universe.TypeTag

import au.com.cba.omnia.maestro.api.Field

import Feature._

object Patterns {

  def pivot[S, V <: Value : TypeTag, FV <% V](
    namespace: Namespace,
    fType:     Type,
    entity:    S => EntityId,
    time:      S => Time,
    field:     Field[S, FV]
  ) =
    new Feature[S, V](FeatureMetadata(namespace, field.name, fType)) {
      def generate(source: S) = Option(FeatureValue(this, entity(source), field.get(source), time(source)))
    }
}
