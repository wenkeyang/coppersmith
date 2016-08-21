//
// Copyright 2016 Commonwealth Bank of Australia
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//        http://www.apache.org/licenses/LICENSE-2.0
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

version := "0.1-DEMO"

scalaVersion := "2.11.8"

lazy val all = Project(id = "all", base = file("."))

lazy val sub = Project(id = "sub", base = file("subproject")).settings(version := "0.1-DEMO",
  resolvers ++= Seq(
    "commbank-releases" at "http://commbank.artifactoryonline.com/commbank/ext-releases-local",
    "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"),
  {
  val pluginVersion = System.getProperty("plugin.version")
  if (pluginVersion == null)
    throw new RuntimeException("""|The system property 'plugin.version' is not defined.
                                  |Specify this property using the scriptedLaunchOpts -D.""".stripMargin)
  else libraryDependencies ++= Seq("au.com.cba.omnia" %% "coppersmith-core"     % pluginVersion,
                                   "au.com.cba.omnia" %% "coppersmith-scalding" % pluginVersion,
                                   "au.com.cba.omnia" %% "coppersmith-tools"    % pluginVersion
  )
})
