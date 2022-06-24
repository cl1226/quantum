package org.excitinglab.quantum.flink.config

import scala.language.reflectiveCalls

class ConfigBuilder(configFile: String) {


}

object ConfigBuilder {

  val PackagePrefix = "org.excitinglab.quantum.flink.core"
  val TransformPackage = PackagePrefix + ".transform"
  val InputPackage = PackagePrefix + ".input"
  val OutputPackage = PackagePrefix + ".output"

  val PluginNameKey = "plugin_name"
}
