package commbank.coppersmith

import com.rouesnel.typedsql.{DataSource => TypedSqlDataSource, _}

abstract class TypedSqlFeatureSet[Q <: CompiledSqlQuery](query: Q) extends FeatureSet[Q#Row] {

}
