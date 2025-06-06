/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.connect

import java.io.File
import java.net.URI
import java.nio.file.{Files, FileSystems, Path, Paths, StandardWatchEventKinds}
import java.util.Locale
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

import com.google.common.cache.{CacheBuilder, CacheLoader}
import io.grpc.ClientInterceptor
import org.apache.arrow.memory.RootAllocator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.annotation.{DeveloperApi, Experimental, Since}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.ExecutePlanResponse
import org.apache.spark.connect.proto.ExecutePlanResponse.ObservedMetrics
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{CONFIG, PATH}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.{Column, Encoder, ExperimentalMethods, Observation, Row, SparkSessionBuilder, SparkSessionCompanion, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.{JavaTypeInference, ScalaReflection}
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{agnosticEncoderFor, BoxedLongEncoder, UnboundRowEncoder}
import org.apache.spark.sql.connect.ColumnNodeToProtoConverter.toLiteral
import org.apache.spark.sql.connect.ConnectConversions._
import org.apache.spark.sql.connect.client.{ClassFinder, CloseableIterator, SparkConnectClient, SparkResult}
import org.apache.spark.sql.connect.client.SparkConnectClient.Configuration
import org.apache.spark.sql.connect.client.arrow.ArrowSerializer
import org.apache.spark.sql.internal.{SessionState, SharedState, SqlApiConf, SubqueryExpression}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ExecutionListenerManager
import org.apache.spark.util.ArrayImplicits._

/**
 * The entry point to programming Spark with the Dataset and DataFrame API.
 *
 * In environments that this has been created upfront (e.g. REPL, notebooks), use the builder to
 * get an existing session:
 *
 * {{{
 *   SparkSession.builder().getOrCreate()
 * }}}
 *
 * The builder can also be used to create a new session:
 *
 * {{{
 *   SparkSession.builder
 *     .remote("sc://localhost:15001/myapp")
 *     .getOrCreate()
 * }}}
 */
class SparkSession private[sql] (
    private[sql] val client: SparkConnectClient,
    private val planIdGenerator: AtomicLong)
    extends sql.SparkSession
    with Logging {

  private[this] val allocator = new RootAllocator()
  private[sql] lazy val cleaner = new SessionCleaner(this)

  // a unique session ID for this session from client.
  private[sql] def sessionId: String = client.sessionId

  lazy val version: String = {
    client.analyze(proto.AnalyzePlanRequest.AnalyzeCase.SPARK_VERSION).getSparkVersion.getVersion
  }

  private[sql] val observationRegistry = new ConcurrentHashMap[Long, Observation]()

  private[sql] def hijackServerSideSessionIdForTesting(suffix: String): Unit = {
    client.hijackServerSideSessionIdForTesting(suffix)
  }

  /** @inheritdoc */
  override def sparkContext: SparkContext =
    throw ConnectClientUnsupportedErrors.sparkContext()

  /** @inheritdoc */
  val conf: RuntimeConfig = new RuntimeConfig(client)

  /** @inheritdoc */
  @transient
  val emptyDataFrame: DataFrame = emptyDataset(UnboundRowEncoder)

  /** @inheritdoc */
  def emptyDataset[T: Encoder]: Dataset[T] = createDataset[T](Nil)

  private def createDataset[T](encoder: AgnosticEncoder[T], data: Iterator[T]): Dataset[T] = {
    newDataset(encoder) { builder =>
      if (data.nonEmpty) {
        val arrowData =
          ArrowSerializer.serialize(data, encoder, allocator, timeZoneId, largeVarTypes)
        if (arrowData.size() <= conf.get(SqlApiConf.LOCAL_RELATION_CACHE_THRESHOLD_KEY).toInt) {
          builder.getLocalRelationBuilder
            .setSchema(encoder.schema.json)
            .setData(arrowData)
        } else {
          val hash = client.cacheLocalRelation(arrowData, encoder.schema.json)
          builder.getCachedLocalRelationBuilder
            .setHash(hash)
        }
      } else {
        builder.getLocalRelationBuilder
          .setSchema(encoder.schema.json)
      }
    }
  }

  /** @inheritdoc */
  def createDataFrame[A <: Product: TypeTag](data: Seq[A]): DataFrame = {
    createDataset(ScalaReflection.encoderFor[A], data.iterator).toDF()
  }

  /** @inheritdoc */
  def createDataFrame(rows: java.util.List[Row], schema: StructType): DataFrame = {
    createDataset(RowEncoder.encoderFor(schema), rows.iterator().asScala).toDF()
  }

  /** @inheritdoc */
  def createDataFrame(data: java.util.List[_], beanClass: Class[_]): DataFrame = {
    val encoder = JavaTypeInference.encoderFor(beanClass.asInstanceOf[Class[Any]])
    createDataset(encoder, data.iterator().asScala).toDF()
  }

  /** @inheritdoc */
  def createDataset[T: Encoder](data: Seq[T]): Dataset[T] = {
    createDataset(agnosticEncoderFor[T], data.iterator)
  }

  /** @inheritdoc */
  def createDataset[T: Encoder](data: java.util.List[T]): Dataset[T] = {
    createDataset(data.asScala.toSeq)
  }

  /** @inheritdoc */
  override def createDataFrame[A <: Product: TypeTag](rdd: RDD[A]): DataFrame =
    throw ConnectClientUnsupportedErrors.rdd()

  /** @inheritdoc */
  override def createDataFrame(rowRDD: RDD[Row], schema: StructType): DataFrame =
    throw ConnectClientUnsupportedErrors.rdd()

  /** @inheritdoc */
  override def createDataFrame(rowRDD: JavaRDD[Row], schema: StructType): DataFrame =
    throw ConnectClientUnsupportedErrors.rdd()

  /** @inheritdoc */
  override def createDataFrame(rdd: RDD[_], beanClass: Class[_]): DataFrame =
    throw ConnectClientUnsupportedErrors.rdd()

  /** @inheritdoc */
  override def createDataFrame(rdd: JavaRDD[_], beanClass: Class[_]): DataFrame =
    throw ConnectClientUnsupportedErrors.rdd()

  /** @inheritdoc */
  override def createDataset[T: Encoder](data: RDD[T]): Dataset[T] =
    throw ConnectClientUnsupportedErrors.rdd()

  /** @inheritdoc */
  override def sharedState: SharedState =
    throw ConnectClientUnsupportedErrors.sharedState()

  /** @inheritdoc */
  override def sessionState: SessionState =
    throw ConnectClientUnsupportedErrors.sessionState()

  /** @inheritdoc */
  override val sqlContext: SQLContext = new SQLContext(this)

  /** @inheritdoc */
  override def listenerManager: ExecutionListenerManager =
    throw ConnectClientUnsupportedErrors.listenerManager()

  /** @inheritdoc */
  override def experimental: ExperimentalMethods =
    throw ConnectClientUnsupportedErrors.experimental()

  /** @inheritdoc */
  override def baseRelationToDataFrame(baseRelation: BaseRelation): DataFrame =
    throw ConnectClientUnsupportedErrors.baseRelationToDataFrame()

  /** @inheritdoc */
  override def executeCommand(
      runner: String,
      command: String,
      options: Map[String, String]): DataFrame = {
    executeCommandWithDataFrameReturn(newCommand { builder =>
      builder.getExecuteExternalCommandBuilder
        .setRunner(runner)
        .setCommand(command)
        .putAllOptions(options.asJava)
    })
  }

  /** @inheritdoc */
  def sql(sqlText: String, args: Array[_]): DataFrame = {
    val sqlCommand = proto.SqlCommand
      .newBuilder()
      .setSql(sqlText)
      .addAllPosArguments(args.map(a => toLiteral(a)).toImmutableArraySeq.asJava)
      .build()
    sql(sqlCommand)
  }

  /** @inheritdoc */
  def sql(sqlText: String, args: Map[String, Any]): DataFrame = {
    sql(sqlText, args.asJava)
  }

  /** @inheritdoc */
  override def sql(sqlText: String, args: java.util.Map[String, Any]): DataFrame = {
    val sqlCommand = proto.SqlCommand
      .newBuilder()
      .setSql(sqlText)
      .putAllNamedArguments(args.asScala.map { case (k, v) => (k, toLiteral(v)) }.asJava)
      .build()
    sql(sqlCommand)
  }

  /** @inheritdoc */
  override def sql(query: String): DataFrame = {
    sql(query, Array.empty)
  }

  private def sql(sqlCommand: proto.SqlCommand): DataFrame = {
    // Send the SQL once to the server and then check the output.
    executeCommandWithDataFrameReturn(newCommand(_.setSqlCommand(sqlCommand)))
  }

  private def executeCommandWithDataFrameReturn(command: proto.Command): DataFrame = {
    val plan = proto.Plan.newBuilder().setCommand(command)
    val responseIter = client.execute(plan.build())

    try {
      val response = responseIter
        .find(_.hasSqlCommandResult)
        .getOrElse(throw new RuntimeException("SQLCommandResult must be present"))
      // Update the builder with the values from the result.
      newDataFrame(_.mergeFrom(response.getSqlCommandResult.getRelation))
    } finally {
      // consume the rest of the iterator
      responseIter.foreach(_ => ())
    }
  }

  /** @inheritdoc */
  def read: DataFrameReader = new DataFrameReader(this)

  /** @inheritdoc */
  def readStream: DataStreamReader = new DataStreamReader(this)

  /** @inheritdoc */
  def tvf: TableValuedFunction = new TableValuedFunction(this)

  /** @inheritdoc */
  lazy val streams: StreamingQueryManager = new StreamingQueryManager(this)

  /** @inheritdoc */
  lazy val catalog: Catalog = new Catalog(this)

  /** @inheritdoc */
  def table(tableName: String): DataFrame = {
    read.table(tableName)
  }

  /** @inheritdoc */
  def range(end: Long): Dataset[java.lang.Long] = range(0, end)

  /** @inheritdoc */
  def range(start: Long, end: Long): Dataset[java.lang.Long] = {
    range(start, end, step = 1)
  }

  /** @inheritdoc */
  def range(start: Long, end: Long, step: Long): Dataset[java.lang.Long] = {
    range(start, end, step, None)
  }

  /** @inheritdoc */
  def range(start: Long, end: Long, step: Long, numPartitions: Int): Dataset[java.lang.Long] = {
    range(start, end, step, Option(numPartitions))
  }

  /** @inheritdoc */
  lazy val udf: UDFRegistration = new UDFRegistration(this)

  // scalastyle:off
  /** @inheritdoc */
  object implicits extends SQLImplicits(this)
  // scalastyle:on

  /** @inheritdoc */
  def newSession(): SparkSession = {
    SparkSession.builder().client(client.copy()).create()
  }

  private def range(
      start: Long,
      end: Long,
      step: Long,
      numPartitions: Option[Int]): Dataset[java.lang.Long] = {
    newDataset(BoxedLongEncoder) { builder =>
      val rangeBuilder = builder.getRangeBuilder
        .setStart(start)
        .setEnd(end)
        .setStep(step)
      numPartitions.foreach(rangeBuilder.setNumPartitions)
    }
  }

  /**
   * Create a DataFrame including the proto plan built by the given function.
   *
   * @param f
   *   The function to build the proto plan.
   * @return
   *   The DataFrame created from the proto plan.
   */
  @Since("4.0.0")
  @DeveloperApi
  def newDataFrame(f: proto.Relation.Builder => Unit): DataFrame = {
    newDataset(UnboundRowEncoder)(f)
  }

  /**
   * Create a DataFrame including the proto plan built by the given function.
   *
   * Use this method when columns are used to create a new DataFrame. When there are columns
   * referring to other Dataset or DataFrame, the plan will be wrapped with a `WithRelation`.
   *
   * {{{
   *   with_relations [id 10]
   *     root: plan  [id 9]  using columns referring to other Dataset or DataFrame, holding plan ids
   *     reference:
   *          refs#1: [id 8]  plan for the reference 1
   *          refs#2: [id 5]  plan for the reference 2
   * }}}
   *
   * @param cols
   *   The columns to be used in the DataFrame.
   * @param f
   *   The function to build the proto plan.
   * @return
   *   The DataFrame created from the proto plan.
   */
  @Since("4.0.0")
  @DeveloperApi
  def newDataFrame(cols: Seq[Column])(f: proto.Relation.Builder => Unit): DataFrame = {
    newDataset(UnboundRowEncoder, cols)(f)
  }

  /**
   * Create a Dataset including the proto plan built by the given function.
   *
   * @param encoder
   *   The encoder for the Dataset.
   * @param f
   *   The function to build the proto plan.
   * @return
   *   The Dataset created from the proto plan.
   */
  @Since("4.0.0")
  @DeveloperApi
  def newDataset[T](
      encoder: AgnosticEncoder[T])(f: proto.Relation.Builder => Unit): Dataset[T] = {
    newDataset[T](encoder, Seq.empty)(f)
  }

  /**
   * Create a Dataset including the proto plan built by the given function.
   *
   * Use this method when columns are used to create a new Dataset. When there are columns
   * referring to other Dataset or DataFrame, the plan will be wrapped with a `WithRelation`.
   *
   * {{{
   *   with_relations [id 10]
   *     root: plan  [id 9]  using columns referring to other Dataset or DataFrame, holding plan ids
   *     reference:
   *          refs#1: [id 8]  plan for the reference 1
   *          refs#2: [id 5]  plan for the reference 2
   * }}}
   *
   * @param encoder
   *   The encoder for the Dataset.
   * @param cols
   *   The columns to be used in the DataFrame.
   * @param f
   *   The function to build the proto plan.
   * @return
   *   The Dataset created from the proto plan.
   */
  @Since("4.0.0")
  @DeveloperApi
  def newDataset[T](encoder: AgnosticEncoder[T], cols: Seq[Column])(
      f: proto.Relation.Builder => Unit): Dataset[T] = {
    val references: Seq[proto.Relation] = cols.flatMap(_.node.collect {
      case n: SubqueryExpression => n.ds.plan.getRoot
    })

    val builder = proto.Relation.newBuilder()
    f(builder)
    builder.getCommonBuilder.setPlanId(planIdGenerator.getAndIncrement())

    val rootBuilder = if (references.length == 0) {
      builder
    } else {
      val rootBuilder = proto.Relation.newBuilder()
      rootBuilder.getWithRelationsBuilder
        .setRoot(builder)
        .addAllReferences(references.asJava)
      rootBuilder.getCommonBuilder.setPlanId(planIdGenerator.getAndIncrement())
      rootBuilder
    }

    val plan = proto.Plan.newBuilder().setRoot(rootBuilder).build()
    new Dataset[T](this, plan, encoder)
  }

  private[sql] def newDataset[T](encoder: AgnosticEncoder[T], plan: proto.Plan): Dataset[T] = {
    new Dataset[T](this, plan, encoder)
  }

  private[sql] def newCommand[T](f: proto.Command.Builder => Unit): proto.Command = {
    val builder = proto.Command.newBuilder()
    f(builder)
    builder.build()
  }

  private[sql] def analyze(
      plan: proto.Plan,
      method: proto.AnalyzePlanRequest.AnalyzeCase,
      explainMode: Option[proto.AnalyzePlanRequest.Explain.ExplainMode] = None)
      : proto.AnalyzePlanResponse = {
    client.analyze(method, Some(plan), explainMode)
  }

  private[sql] def analyze(
      f: proto.AnalyzePlanRequest.Builder => Unit): proto.AnalyzePlanResponse = {
    val builder = proto.AnalyzePlanRequest.newBuilder()
    f(builder)
    client.analyze(builder)
  }

  private[sql] def sameSemantics(plan: proto.Plan, otherPlan: proto.Plan): Boolean = {
    client.sameSemantics(plan, otherPlan).getSameSemantics.getResult
  }

  private[sql] def semanticHash(plan: proto.Plan): Int = {
    client.semanticHash(plan).getSemanticHash.getResult
  }

  private[sql] def timeZoneId: String = conf.get(SqlApiConf.SESSION_LOCAL_TIMEZONE_KEY)
  private[sql] def largeVarTypes: Boolean =
    conf.get(SqlApiConf.ARROW_EXECUTION_USE_LARGE_VAR_TYPES).toLowerCase(Locale.ROOT).toBoolean

  private[sql] def execute[T](plan: proto.Plan, encoder: AgnosticEncoder[T]): SparkResult[T] = {
    val value = executeInternal(plan)
    new SparkResult(value, allocator, encoder, timeZoneId)
  }

  private[sql] def execute(f: proto.Relation.Builder => Unit): Unit = {
    val builder = proto.Relation.newBuilder()
    f(builder)
    builder.getCommonBuilder.setPlanId(planIdGenerator.getAndIncrement())
    val plan = proto.Plan.newBuilder().setRoot(builder).build()
    // .foreach forces that the iterator is consumed and closed
    executeInternal(plan).foreach(_ => ())
  }

  @Since("4.0.0")
  @DeveloperApi
  def execute(command: proto.Command): Seq[ExecutePlanResponse] = {
    val plan = proto.Plan.newBuilder().setCommand(command).build()
    // .toSeq forces that the iterator is consumed and closed. On top, ignore all
    // progress messages.
    executeInternal(plan).filter(!_.hasExecutionProgress).toSeq
  }

  /**
   * The real `execute` method that calls into `SparkConnectClient`.
   *
   * Here we inject a lazy map to process registered observed metrics, so consumers of the
   * returned iterator does not need to worry about it.
   *
   * Please make sure all `execute` methods call this method.
   */
  private[sql] def executeInternal(plan: proto.Plan): CloseableIterator[ExecutePlanResponse] = {
    client
      .execute(plan)
      .map { response =>
        // Note, this map() is lazy.
        processRegisteredObservedMetrics(response.getObservedMetricsList)
        response
      }
  }

  private[sql] def registerUdf(udf: proto.CommonInlineUserDefinedFunction): Unit = {
    val command = proto.Command.newBuilder().setRegisterFunction(udf).build()
    execute(command)
  }

  /** @inheritdoc */
  @Experimental
  override def addArtifact(path: String): Unit = client.addArtifact(path)

  /** @inheritdoc */
  @Experimental
  override def addArtifact(uri: URI): Unit = client.addArtifact(uri)

  /** @inheritdoc */
  @Experimental
  override def addArtifact(bytes: Array[Byte], target: String): Unit = {
    client.addArtifact(bytes, target)
  }

  /** @inheritdoc */
  @Experimental
  override def addArtifact(source: String, target: String): Unit = {
    client.addArtifact(source, target)
  }

  /** @inheritdoc */
  @Experimental
  @scala.annotation.varargs
  override def addArtifacts(uri: URI*): Unit = client.addArtifacts(uri)

  /**
   * Register a ClassFinder for dynamically generated classes.
   * @since 3.5.0
   */
  @Experimental
  def registerClassFinder(finder: ClassFinder): Unit = client.registerClassFinder(finder)

  /**
   * This resets the plan id generator so we can produce plans that are comparable.
   *
   * For testing only!
   */
  private[sql] def resetPlanIdGenerator(): Unit = {
    planIdGenerator.set(0)
  }

  /**
   * Interrupt all operations of this session currently running on the connected server.
   *
   * @return
   *   sequence of operationIds of interrupted operations. Note: there is still a possibility of
   *   operation finishing just as it is interrupted.
   *
   * @since 3.5.0
   */
  override def interruptAll(): Seq[String] = {
    client.interruptAll().getInterruptedIdsList.asScala.toSeq
  }

  /**
   * Interrupt all operations of this session with the given operation tag.
   *
   * @return
   *   sequence of operationIds of interrupted operations. Note: there is still a possibility of
   *   operation finishing just as it is interrupted.
   *
   * @since 3.5.0
   */
  override def interruptTag(tag: String): Seq[String] = {
    client.interruptTag(tag).getInterruptedIdsList.asScala.toSeq
  }

  /**
   * Interrupt an operation of this session with the given operationId.
   *
   * @return
   *   sequence of operationIds of interrupted operations. Note: there is still a possibility of
   *   operation finishing just as it is interrupted.
   *
   * @since 3.5.0
   */
  override def interruptOperation(operationId: String): Seq[String] = {
    client.interruptOperation(operationId).getInterruptedIdsList.asScala.toSeq
  }

  /**
   * Close the [[SparkSession]].
   *
   * Release the current session and close the GRPC connection to the server. The API will not
   * error if any of these operations fail. Closing a closed session is a no-op.
   *
   * Close the allocator. Fail if there are still open SparkResults.
   *
   * @since 3.4.0
   */
  override def close(): Unit = {
    if (releaseSessionOnClose) {
      try {
        client.releaseSession()
      } catch {
        case e: Exception => logWarning("session.stop: Failed to release session", e)
      }
    }
    try {
      client.shutdown()
    } catch {
      case e: Exception => logWarning("session.stop: Failed to shutdown the client", e)
    }
    allocator.close()
    SparkSession.onSessionClose(this)
    SparkSession.server.synchronized {
      if (SparkSession.server.isDefined) {
        // When local mode is in use, follow the regular Spark session's
        // behavior by terminating the Spark Connect server,
        // meaning that you can stop local mode, and restart the Spark Connect
        // client with a different remote address.
        new ProcessBuilder(SparkSession.maybeConnectStopScript.get.toString)
          .start()
        SparkSession.server = None
      }
    }
  }

  /** @inheritdoc */
  override def addTag(tag: String): Unit = client.addTag(tag)

  /** @inheritdoc */
  override def removeTag(tag: String): Unit = client.removeTag(tag)

  /** @inheritdoc */
  override def getTags(): Set[String] = client.getTags()

  /** @inheritdoc */
  override def clearTags(): Unit = client.clearTags()

  /**
   * We cannot deserialize a connect [[SparkSession]] because of a class clash on the server side.
   * We null out the instance for now.
   */
  private def writeReplace(): Any = null

  /**
   * Set to false to prevent client.releaseSession on close() (testing only)
   */
  private[sql] var releaseSessionOnClose = true

  private[sql] def registerObservation(planId: Long, observation: Observation): Unit = {
    observation.markRegistered()
    observationRegistry.putIfAbsent(planId, observation)
  }

  private def processRegisteredObservedMetrics(metrics: java.util.List[ObservedMetrics]): Unit = {
    metrics.asScala.map { metric =>
      // Here we only process metrics that belong to a registered Observation object.
      // All metrics, whether registered or not, will be collected by `SparkResult`.
      val observationOrNull = observationRegistry.remove(metric.getPlanId)
      if (observationOrNull != null) {
        observationOrNull.setMetricsAndNotify(SparkResult.transformObservedMetrics(metric))
      }
    }
  }

  override private[sql] def isUsable: Boolean = client.isSessionValid
}

// The minimal builder needed to create a spark session.
// TODO: implements all methods mentioned in the scaladoc of [[SparkSession]]
object SparkSession extends SparkSessionCompanion with Logging {
  override private[sql] type Session = SparkSession

  private val MAX_CACHED_SESSIONS = 100
  private val planIdGenerator = new AtomicLong
  private var server: Option[Process] = None
  private val maybeConnectStartScript =
    Option(System.getenv("SPARK_HOME")).map(Paths.get(_, "sbin", "start-connect-server.sh"))
  private val maybeConnectStopScript =
    Option(System.getenv("SPARK_HOME")).map(Paths.get(_, "sbin", "stop-connect-server.sh"))
  private[sql] val sparkOptions = sys.props.filter { p =>
    p._1.startsWith("spark.") && p._2.nonEmpty
  }.toMap

  private val sessions = CacheBuilder
    .newBuilder()
    .weakValues()
    .maximumSize(MAX_CACHED_SESSIONS)
    .build(new CacheLoader[Configuration, SparkSession] {
      override def load(c: Configuration): SparkSession = create(c)
    })

  private def waitUntilFileExists(file: File): Unit = {
    val deadline = 30.seconds.fromNow
    val watchService = FileSystems.getDefault.newWatchService()
    try {
      file.toPath.getParent.register(watchService, StandardWatchEventKinds.ENTRY_CREATE)
      while (!file.exists() && deadline.hasTimeLeft()) {
        Option(watchService.poll(deadline.timeLeft.toSeconds + 1, TimeUnit.SECONDS)) match {
          case Some(key) =>
            key.pollEvents().forEach { event =>
              val kind = event.kind()
              val filename = event.context().asInstanceOf[Path]

              if (kind == StandardWatchEventKinds.ENTRY_CREATE
                && filename.toString == file.toPath.getFileName.toString) {
                key.cancel()
                return
              }
            }
            key.reset()
          case None =>
        }
      }
    } finally {
      watchService.close()
    }
  }

  /**
   * Create a new Spark Connect server to connect locally.
   */
  private[sql] def withLocalConnectServer[T](f: => T): T = {
    lazy val isAPIModeConnect =
      Option(System.getProperty(org.apache.spark.sql.SparkSessionBuilder.API_MODE_KEY))
        .getOrElse("classic")
        .toLowerCase(Locale.ROOT) == "connect" || System.getenv("SPARK_CONNECT_MODE") == "1"
    val remoteString = sparkOptions
      .get("spark.remote")
      .orElse(Option(System.getProperty("spark.remote"))) // Set from Spark Submit
      .orElse(sys.env.get(SparkConnectClient.SPARK_REMOTE))
      .orElse {
        if (isAPIModeConnect) {
          sparkOptions.get("spark.master").orElse(sys.env.get("MASTER"))
        } else {
          None
        }
      }

    lazy val serverId = UUID.randomUUID().toString

    server.synchronized {
      if (server.isEmpty &&
        (remoteString.exists(_.startsWith("local")) ||
          (remoteString.isDefined && isAPIModeConnect)) &&
        maybeConnectStartScript.exists(Files.exists(_))) {
        val token = java.util.UUID.randomUUID().toString()
        server = Some {
          val args =
            Seq(
              maybeConnectStartScript.get.toString,
              "--master",
              remoteString.get) ++ (sparkOptions ++ Map(
              "spark.sql.artifact.isolation.enabled" -> "true",
              "spark.sql.artifact.isolation.alwaysApplyClassloader" -> "true"))
              .filter(p => !p._1.startsWith("spark.remote"))
              .filter(p => !p._1.startsWith("spark.api.mode"))
              .flatMap { case (k, v) => Seq("--conf", s"$k=$v") }
          val pb = new ProcessBuilder(args: _*)
          // So don't exclude spark-sql jar in classpath
          pb.environment().remove(SparkConnectClient.SPARK_REMOTE)
          pb.environment().put("SPARK_CONNECT_MODE", "0")
          pb.environment().put("SPARK_IDENT_STRING", serverId)
          pb.environment().put("HOSTNAME", "local")
          pb.environment().put("SPARK_CONNECT_AUTHENTICATE_TOKEN", token)
          pb.start()
        }

        // Let the server start, and wait until the log file is created.
        Option(System.getenv("SPARK_LOG_DIR"))
          .orElse(Option(System.getenv("SPARK_HOME")).map(p => Paths.get(p, "logs").toString))
          .foreach { p =>
            Files.createDirectories(Paths.get(p))
            val logFile = Paths
              .get(
                p,
                s"spark-$serverId-" +
                  s"org.apache.spark.sql.connect.service.SparkConnectServer-1-local.out")
              .toFile
            waitUntilFileExists(logFile)
            if (logFile.exists()) {
              logInfo(log"Spark Connect server started with the log file: ${MDC(PATH, logFile)}")
            } else {
              logWarning(log"Spark Connect server log not found at ${MDC(PATH, logFile)}")
            }
          }

        // Let the server fully start to make less noise from retrying.
        Thread.sleep(1000L)

        System.setProperty("spark.remote", s"sc://localhost/;token=$token")

        // scalastyle:off runtimeaddshutdownhook
        Runtime.getRuntime.addShutdownHook(new Thread() {
          override def run(): Unit = server.synchronized {
            if (server.isDefined) {
              val builder = new ProcessBuilder(maybeConnectStopScript.get.toString)
              builder.environment().put("SPARK_IDENT_STRING", serverId)
              builder.start()
            }
          }
        })
        // scalastyle:on runtimeaddshutdownhook
      }
    }

    f
  }

  /**
   * Create a new [[SparkSession]] based on the connect client [[Configuration]].
   */
  private[sql] def create(configuration: Configuration): SparkSession = {
    new SparkSession(configuration.toSparkConnectClient, planIdGenerator)
  }

  /**
   * Creates a [[SparkSession.Builder]] for constructing a [[SparkSession]].
   *
   * @since 3.4.0
   */
  def builder(): Builder = new Builder()

  class Builder() extends SparkSessionBuilder {
    import SparkSessionBuilder._

    // Initialize the connection string of the Spark Connect client builder from SPARK_REMOTE
    // by default, if it exists. The connection string can be overridden using
    // the remote() function, as it takes precedence over the SPARK_REMOTE environment variable.
    private var connectionString: Option[String] = None
    private var interceptor: Option[ClientInterceptor] = None
    private var client: SparkConnectClient = _
    private lazy val builder = {
      val b = SparkConnectClient.builder().loadFromEnvironment()
      connectionString.foreach(b.connectionString)
      interceptor.foreach(b.interceptor)
      b
    }

    /** @inheritdoc */
    @deprecated("sparkContext does not work in Spark Connect")
    override private[spark] def sparkContext(sparkContext: SparkContext): this.type = {
      this
    }

    /**
     * Add an interceptor to be used during channel creation.
     *
     * Note that interceptors added last are executed first by gRPC.
     *
     * @since 3.5.0
     */
    def interceptor(interceptor: ClientInterceptor): this.type = {
      this.interceptor = Some(interceptor)
      this
    }

    private[sql] def client(client: SparkConnectClient): this.type = {
      this.client = client
      this
    }

    /** @inheritdoc */
    override def config(key: String, value: String): this.type = super.config(key, value)

    /** @inheritdoc */
    override def config(key: String, value: Long): this.type = super.config(key, value)

    /** @inheritdoc */
    override def config(key: String, value: Double): this.type = super.config(key, value)

    /** @inheritdoc */
    override def config(key: String, value: Boolean): this.type = super.config(key, value)

    /** @inheritdoc */
    override def config(map: Map[String, Any]): this.type = super.config(map)

    /** @inheritdoc */
    override def config(map: java.util.Map[String, Any]): this.type = super.config(map)

    /** @inheritdoc */
    override def config(conf: SparkConf): Builder.this.type = super.config(conf)

    /** @inheritdoc */
    @deprecated("enableHiveSupport does not work in Spark Connect")
    override def enableHiveSupport(): this.type = super.enableHiveSupport()

    /** @inheritdoc */
    @deprecated("master does not work in Spark Connect, please use remote instead")
    override def master(master: String): this.type = super.master(master)

    /** @inheritdoc */
    @deprecated("appName does not work in Spark Connect")
    override def appName(name: String): this.type = super.appName(name)

    /** @inheritdoc */
    override def remote(connectionString: String): Builder.this.type =
      super.remote(connectionString)

    override protected def handleBuilderConfig(key: String, value: String): Boolean = key match {
      case CONNECT_REMOTE_KEY =>
        connectionString = Some(value)
        true
      case APP_NAME_KEY | MASTER_KEY | CATALOG_IMPL_KEY | API_MODE_KEY =>
        logWarning(log"${MDC(CONFIG, key)} configuration is not supported in Connect mode.")
        true
      case _ =>
        false
    }

    /** @inheritdoc */
    @deprecated("withExtensions does not work in Spark Connect")
    override def withExtensions(f: SparkSessionExtensions => Unit): this.type = this

    private def tryCreateSessionFromClient(): Option[SparkSession] = {
      if (client != null && client.isSessionValid) {
        Option(new SparkSession(client, planIdGenerator))
      } else {
        None
      }
    }

    private def applyOptions(session: SparkSession): Unit = {
      // Only attempts to set Spark SQL configurations.
      // If the configurations are static, it might throw an exception so
      // simply ignore it for now.
      sparkOptions
        .filter { case (k, _) =>
          k.startsWith("spark.sql.")
        }
        .foreach { case (key, value) =>
          Try(session.conf.set(key, value))
        }
      options.foreach { case (key, value) =>
        session.conf.set(key, value)
      }
    }

    /**
     * Build the [[SparkSession]].
     *
     * This will always return a newly created session.
     */
    @deprecated(message = "Please use create() instead.", since = "3.5.0")
    def build(): SparkSession = create()

    /**
     * Create a new [[SparkSession]].
     *
     * This will always return a newly created session.
     *
     * This method will update the default and/or active session if they are not set.
     *
     * @since 3.5.0
     */
    def create(): SparkSession = withLocalConnectServer {
      val session = tryCreateSessionFromClient()
        .getOrElse(SparkSession.this.create(builder.configuration))
      setDefaultAndActiveSession(session)
      applyOptions(session)
      session
    }

    /**
     * Get or create a [[SparkSession]].
     *
     * If a session exist with the same configuration that is returned instead of creating a new
     * session.
     *
     * This method will update the default and/or active session if they are not set. This method
     * will always set the specified configuration options on the session, even when it is not
     * newly created.
     *
     * @since 3.5.0
     */
    def getOrCreate(): SparkSession = withLocalConnectServer {
      val session = tryCreateSessionFromClient()
        .getOrElse({
          var existingSession = sessions.get(builder.configuration)
          if (!existingSession.client.isSessionValid) {
            // If the cached session has become invalid, e.g., due to a server restart, the cache
            // entry is invalidated.
            sessions.invalidate(builder.configuration)
            existingSession = sessions.get(builder.configuration)
          }
          existingSession
        })
      setDefaultAndActiveSession(session)
      applyOptions(session)
      session
    }
  }

  /** @inheritdoc */
  override def getActiveSession: Option[SparkSession] = super.getActiveSession

  /** @inheritdoc */
  override def getDefaultSession: Option[SparkSession] = super.getDefaultSession

  /** @inheritdoc */
  override def active: SparkSession = super.active

  override protected def tryCastToImplementation(
      session: sql.SparkSession): Option[SparkSession] = session match {
    case impl: SparkSession => Some(impl)
    case _ => None
  }
}
