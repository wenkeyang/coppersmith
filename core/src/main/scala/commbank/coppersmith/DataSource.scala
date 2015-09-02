package commbank.coppersmith

trait DataSource[S, P[_]] {
  def load: P[S]
}
