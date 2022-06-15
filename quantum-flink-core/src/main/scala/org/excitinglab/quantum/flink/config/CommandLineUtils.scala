package org.excitinglab.quantum.flink.config

object CommandLineUtils {

  /**
   * command line arguments parser.
   * */
  val parser = new scopt.OptionParser[CommandLineArgs]("start-quantum-flink.sh") {
    head("quantum-Flink", "1.0.0")

    opt[String]('c', "config").required().action((x, c) => c.copy(configFile = x)).text("config file")
    opt[Unit]('t', "check").action((_, c) => c.copy(testConfig = true)).text("check config")
    opt[String]('i', "variable")
      .optional()
      .text("variable substitution, such as -i city=beijing, or -i date=20190318")
      .maxOccurs(Integer.MAX_VALUE)
  }
}
