package org.excitinglab.quantum.spark

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.excitinglab.quantum.spark.apis.{BaseOutput, BaseStaticInput, BaseStreamingInput, BaseTransform, Plugin}
import org.excitinglab.quantum.common.config.CheckResult
import org.excitinglab.quantum.config._
import org.excitinglab.quantum.spark.config.{CommandLineArgs, CommandLineUtils, Common, ConfigBuilder, ConfigRuntimeException, UserRuntimeException}
import org.excitinglab.quantum.spark.utils.{AsciiArt, CompressionUtils}

import java.io.File
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConversions._

object SparkStarter extends Logging {

  var viewTableMap: Map[String, Dataset[Row]] = Map[String, Dataset[Row]]()
  var master: String = _

  def main(args: Array[String]): Unit = {

    CommandLineUtils.parser.parse(args, CommandLineArgs()) match {
      case Some(cmdArgs) => {
        Common.setDeployMode(cmdArgs.deployMode)
        master = cmdArgs.master
        val configFilePath = getConfigFilePath(cmdArgs)

        cmdArgs.testConfig match {
          case true => {
            new ConfigBuilder(configFilePath).checkConfig
            println("config OK !")
          }
          case false => {
            Try(entrypoint(configFilePath)) match {
              case Success(_) => {}
              case Failure(exception) => {
                exception match {
                  case e @ (_: ConfigRuntimeException | _: UserRuntimeException) => SparkStarter.showConfigError(e)
                  case e: Exception => throw new Exception(e)
                }
              }
            }
          }
        }
      }
      case None =>
    }
  }

  private[quantum] def getConfigFilePath(cmdArgs: CommandLineArgs): String = {
    Common.getDeployMode match {
      case Some(m) => {
        if (m.equals("cluster")) {
          // only keep filename in cluster mode
          new Path(cmdArgs.configFile).getName
        } else {
          cmdArgs.configFile
        }
      }
    }
  }

  private[quantum] def showConfigError(throwable: Throwable): Unit = {
    println("\n\n===============================================================================\n\n")
    val errorMsg = throwable.getMessage
    println("Config Error:\n")
    println("Reason: " + errorMsg + "\n")
    println("\n===============================================================================\n\n\n")
    throw new ConfigRuntimeException(throwable)
  }

  private def entrypoint(configFile: String): Unit = {

    val configBuilder = new ConfigBuilder(configFile)
    println("[INFO] loading SparkConf: ")
    val sparkConf = createSparkConf(configBuilder)
    sparkConf.getAll.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " => " + value)
    })

    StringUtils.isNotBlank(master) match {
      case true => sparkConf.setMaster(master)
      case false =>
    }

    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
    sparkSession.sparkContext.setLogLevel("error")

    val staticInputs = configBuilder.createStaticInputs("batch")
    val streamingInputs = configBuilder.createStreamingInputs("batch")
    val transforms = configBuilder.createTransforms
    val outputs = configBuilder.createOutputs[BaseOutput]("batch")

    baseCheckConfig(staticInputs, streamingInputs, transforms, outputs)

    if (streamingInputs.nonEmpty) {
      streamingProcessing(sparkSession, configBuilder, staticInputs, streamingInputs, transforms, outputs)
    } else {
      batchProcessing(sparkSession, configBuilder, staticInputs, transforms, outputs)
    }

  }

  /**
   * Batch Processing
   * */
  private[quantum] def batchProcessing(
                               sparkSession: SparkSession,
                               configBuilder: ConfigBuilder,
                               staticInputs: List[BaseStaticInput],
                               transforms: List[BaseTransform],
                               outputs: List[BaseOutput]): Unit = {

    basePrepare(sparkSession, staticInputs, transforms, outputs)

    // when you see this ASCII logo, quantum is really started.
    showquantumAsciiLogo()
    // let static input register as table for later use if needed
    val headDs = registerInputTempViewWithHead(staticInputs, sparkSession)

    if (staticInputs.nonEmpty) {
      var ds = headDs

      for (f <- transforms) {
        showConfig(f.getConfig())
        ds = transformProcess(sparkSession, f, ds)
        registerFilterTempView(f, ds)
      }
      outputs.foreach(p => {
        showConfig(p.getConfig())
        outputProcess(sparkSession, p, ds)
      })

//      sparkSession.stop()
    } else {
      throw new ConfigRuntimeException("Input must be configured at least once.")
    }
  }

  /**
   * Streaming Processing
   */
  private[quantum] def streamingProcessing(sparkSession: SparkSession,
                                          configBuilder: ConfigBuilder,
                                          staticInputs: List[BaseStaticInput],
                                          streamingInputs: List[BaseStreamingInput[Any]],
                                          transforms: List[BaseTransform],
                                          outputs: List[BaseOutput]
                                         ): Unit = {
    val batchDuration = configBuilder.getSparkConfigs.getLong("batchDuration")
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(batchDuration))

    basePrepare(sparkSession, staticInputs, streamingInputs, outputs)

    // let static input register as table for later use if needed
    registerInputTempView(staticInputs, sparkSession)

    // when you see this ASCII logo, quantum is really started.
    showquantumAsciiLogo()

    val streamingInput = streamingInputs(0)
    streamingInput.start(
      sparkSession,
      ssc,
      dataset => {
        var ds = dataset

        if (ds.count() > 0) {
          val tableName = streamingInput.getConfig().getString("result_table_name")

          viewTableMap += (tableName -> ds)
          ds.persist(StorageLevel.MEMORY_AND_DISK)

          transforms.foreach(transform => {
            ds = transform.process(sparkSession, ds)
          })

          ds.unpersist()

          outputs.foreach(output => {
            outputProcess(sparkSession, output, ds)
          })
        } else {
          logInfo(s"consumer 0 record!")
        }
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }

  private[quantum] def showquantumAsciiLogo(): Unit = {
    println("===================================================================")
    AsciiArt.printAsciiArt("quantum")
    println("===================================================================")
    println("")
  }

  /**
  * print config properties
  */
  private[quantum] def showConfig(config: Config) = {
    println(s">>>[INFO] ${config.getString("plugin_name")} plugin options: ")
    val options: ConfigRenderOptions = ConfigRenderOptions.concise.setFormatted(true)
    println(config.root().render(options))
  }

  private[quantum] def transformProcess(
                                        sparkSession: SparkSession,
                                        transform: BaseTransform,
                                        ds: Dataset[Row]): Dataset[Row] = {
    val config = transform.getConfig()
    val fromDs = config.hasPath("source_table_name") match {
      case true => {
        val sourceTableName = config.getString("source_table_name")
        sparkSession.read.table(sourceTableName)
      }
      case false => ds
    }
    transform.process(sparkSession, fromDs)
  }

  private[quantum] def outputProcess(sparkSession: SparkSession, output: BaseOutput, ds: Dataset[Row]): Unit = {
    val config = output.getConfig()
    val fromDs = config.hasPath("source_table_name") match {
      case true => {
        val sourceTableName = config.getString("source_table_name")
        sparkSession.read.table(sourceTableName)
      }
      case false => ds
    }

    output.process(fromDs)
  }

  private[quantum] def registerFilterTempView(plugin: Plugin, ds: Dataset[Row]): Unit = {
    val config = plugin.getConfig()
    if (config.hasPath("result_table_name")) {
      val tableName = config.getString("result_table_name")
      registerTempView(tableName, ds)
    }
  }

  private[quantum] def registerStreamingFilterTempView(plugin: BaseTransform, ds: Dataset[Row]): Unit = {
    val config = plugin.getConfig()
    if (config.hasPath("result_table_name")) {
      val tableName = config.getString("result_table_name")
      registerStreamingTempView(tableName, ds)
    }
  }

  private[quantum] def registerStreamingTempView(tableName: String, ds: Dataset[Row]): Unit = {
    ds.createOrReplaceTempView(tableName)
  }

  /**
   * Return Head Static Input DataSet
   */
  private[quantum] def registerInputTempViewWithHead(
                                                        staticInputs: List[BaseStaticInput],
                                                        sparkSession: SparkSession): Dataset[Row] = {

    if (staticInputs.nonEmpty) {
      val headInput = staticInputs.head
      showConfig(headInput.getConfig())
      val ds = headInput.getDataset(sparkSession)
      registerInputTempView(headInput, ds)

      for (input <- staticInputs.slice(1, staticInputs.length)) {
        showConfig(input.getConfig())
        val ds = input.getDataset(sparkSession)
        registerInputTempView(input, ds)
      }

      ds

    } else {
      throw new ConfigRuntimeException("You must set static input plugin at least once.")
    }
  }

  private[quantum] def registerInputTempView(staticInputs: List[BaseStaticInput],
                                             sparkSession: SparkSession): Unit = {
    for (input <- staticInputs) {
      val ds = input.getDataset(sparkSession)
      registerInputTempView(input, ds)
    }
  }

  private[quantum] def registerInputTempView(input: BaseStaticInput, ds: Dataset[Row]): Unit = {
    val config = input.getConfig()
    config.hasPath("table_name") || config.hasPath("result_table_name") match {
      case true => {
        val tableName = config.hasPath("table_name") match {
          case true => {
            @deprecated
            val oldTableName = config.getString("table_name")
            oldTableName
          }
          case false => config.getString("result_table_name")
        }
        registerTempView(tableName, ds)
      }

      case false => {
        throw new ConfigRuntimeException(
          "Plugin[" + input.name + "] must be registered as dataset/table, please set \"result_table_name\" config")

      }
    }
  }

  private[quantum] def registerTempView(tableName: String, ds: Dataset[Row]): Unit = {
    viewTableMap.contains(tableName) match {
      case true =>
        throw new ConfigRuntimeException(
          "Detected duplicated Dataset["
            + tableName + "], it seems that you configured result_table_name = \"" + tableName + "\" in multiple static inputs")
      case _ => {
        ds.createOrReplaceTempView(tableName)
        viewTableMap += (tableName -> ds)
      }
    }
  }

  private[quantum] def basePrepare(sparkSession: SparkSession, plugins: List[Plugin]*): Unit = {
    for (pluginList <- plugins) {
      for (p <- pluginList) {
        p.prepare(sparkSession)
      }
    }
  }

  private[quantum] def baseCheckConfig(plugins: List[Plugin]*): Unit = {
    var configValid = true
    for (pluginList <- plugins) {
      for (p <- pluginList) {
        val checkResult: CheckResult = p.checkConfig

        if (!checkResult.isSuccess) {
          configValid = false
          printf("Plugin[%s] contains invalid config, error: %s\n", p.name, checkResult.getMsg)
        }
      }

      if (!configValid) {
        System.exit(-1) // invalid configuration
      }
    }
    deployModeCheck()
  }

  private[quantum] def deployModeCheck(): Unit = {
    Common.getDeployMode match {
      case Some(m) => {
        if (m.equals("cluster")) {

          logInfo("preparing cluster mode work dir files...")

          // plugins.tar.gz is added in local app temp dir of driver and executors in cluster mode from --files specified in spark-submit
          val workDir = new File(".")
          logWarning("work dir exists: " + workDir.exists() + ", is dir: " + workDir.isDirectory)

          workDir.listFiles().foreach(f => logWarning("\t list file: " + f.getAbsolutePath))

          // decompress plugin dir
          val compressedFile = new File("plugins.tar.gz")

          Try(CompressionUtils.unGzip(compressedFile, workDir)) match {
            case Success(tempFile) => {
              Try(CompressionUtils.unTar(tempFile, workDir)) match {
                case Success(_) => logInfo("succeeded to decompress plugins.tar.gz")
                case Failure(ex) => {
                  logError("failed to decompress plugins.tar.gz", ex)
                  sys.exit(-1)
                }
              }

            }
            case Failure(ex) => {
              logError("failed to decompress plugins.tar.gz", ex)
              sys.exit(-1)
            }
          }
        }
      }
    }
  }

  private[quantum] def createSparkConf(configBuilder: ConfigBuilder): SparkConf = {
    val sparkConf = new SparkConf()

    configBuilder.getSparkConfigs
      .entrySet()
      .foreach(entry => {
        sparkConf.set(entry.getKey, String.valueOf(entry.getValue.unwrapped()))
      })

    sparkConf
  }

}
