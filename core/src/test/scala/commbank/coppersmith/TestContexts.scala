package commbank.coppersmith

/**
 * A feature context specifically for the testing of features that do not make
 * use of the context. In other words, crashes if referenced, which we want.
 */
object NoContext extends FeatureContext {
  def generationTime = ???
}
