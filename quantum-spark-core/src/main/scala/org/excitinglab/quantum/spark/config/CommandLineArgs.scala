package org.excitinglab.quantum.spark.config

case class CommandLineArgs(
  deployMode: String = "client",
  configFile: String = "application.conf",
  master: String = "",
  testConfig: Boolean = false)
