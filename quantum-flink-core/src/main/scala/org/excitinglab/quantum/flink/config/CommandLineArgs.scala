package org.excitinglab.quantum.flink.config

case class CommandLineArgs(
  configFile: String = "application.conf",
  testConfig: Boolean = false
)
