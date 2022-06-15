package org.excitinglab.quantum.flink.config

import org.excitinglab.quantum.common.apis._
import org.excitinglab.quantum.config.ConfigFactory

import java.io.File
import java.util.ServiceLoader
import scala.language.reflectiveCalls
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import util.control.Breaks._

class ConfigBuilder(configFile: String) {


}

object ConfigBuilder {

  val PackagePrefix = "org.excitinglab.quantum.flink.core"
  val TransformPackage = PackagePrefix + ".transform"
  val InputPackage = PackagePrefix + ".input"
  val OutputPackage = PackagePrefix + ".output"

  val PluginNameKey = "plugin_name"
}
