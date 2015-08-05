resolvers += Resolver.url("commbank-releases-ivy", new URL("http://commbank.artifactoryonline.com/commbank/ext-releases-local-ivy"))(Patterns("[organization]/[module]_[scalaVersion]_[sbtVersion]/[revision]/[artifact](-[classifier])-[revision].[ext]"))

val uniformVersion = "1.2.4-20150513065051-9b4cf64"
val humbugVersion  = "0.6.1-20150513010955-5eb6297"

addSbtPlugin("au.com.cba.omnia" % "uniform-core"       % uniformVersion)

addSbtPlugin("au.com.cba.omnia" % "uniform-dependency" % uniformVersion)

addSbtPlugin("au.com.cba.omnia" % "uniform-thrift"     % uniformVersion)

addSbtPlugin("au.com.cba.omnia" % "uniform-assembly"   % uniformVersion)

addSbtPlugin("au.com.cba.omnia" % "humbug-plugin"      % humbugVersion)
