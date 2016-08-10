package mesosphere.marathon.integration.setup

import java.io.File
import java.nio.file.Files

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Get
import akka.stream.ActorMaterializer
import mesosphere.marathon.util.Retry
import mesosphere.util.PortAllocator
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.sys.process.{Process, ProcessLogger}
import scala.util.Try

/**
  * Runs a mesos-local on a ephemeral port
  *
  * close() should be called when the server is no longer necessary
  */
case class MesosLocal(numSlaves: Int = 1, autoStart: Boolean = true,
                 containerizers: Option[String] = None,
                 logStdout: Boolean = false,
                 waitForStart: Duration = 5.seconds) extends AutoCloseable {
  private implicit lazy val system = ActorSystem()
  private implicit lazy val materializer = ActorMaterializer()
  private implicit lazy val scheduler = system.scheduler
  private implicit lazy val ctx = ExecutionContext.global

  lazy val port = PortAllocator.ephemeralPort()

  private def defaultContainerizers: String = {
    if (sys.env.getOrElse("RUN_DOCKER_INTEGRATION_TESTS", "true") == "true") {
      "docker,mesos"
    } else {
      "mesos"
    }
  }

  private def write(dir: File, fileName: String, content: String): String = {
    val file = File.createTempFile(fileName, "", dir)
    file.deleteOnExit()
    FileUtils.write(file, content)
    file.setReadable(true)
    file.getAbsolutePath
  }

  private lazy val mesosWorkDir = {
    val tmp = Files.createTempDirectory("mesos-local").toFile
    tmp.deleteOnExit()
    tmp
  }

  private lazy val mesosEnv = {
    val credentialsPath = write(mesosWorkDir, fileName = "credentials", content = "principal1 secret1")
    val aclsPath = write(mesosWorkDir, fileName = "acls.json", content =
      """
        |{
        |  "run_tasks": [{
        |    "principals": { "type": "ANY" },
        |    "users": { "type": "ANY" }
        |  }],
        |  "register_frameworks": [{
        |    "principals": { "type": "ANY" },
        |    "roles": { "type": "ANY" }
        |  }],
        |  "reserve_resources": [{
        |    "roles": { "type": "ANY" },
        |    "principals": { "type": "ANY" },
        |    "resources": { "type": "ANY" }
        |  }],
        |  "create_volumes": [{
        |    "roles": { "type": "ANY" },
        |    "principals": { "type": "ANY" },
        |    "volume_types": { "type": "ANY" }
        |  }]
        |}
      """.stripMargin)

    Seq(
      "MESOS_WORK_DIR" -> mesosWorkDir.getAbsolutePath,
      "MESOS_LAUNCHER" -> "posix",
      "MESOS_CONTAINERIZERS" -> containerizers.getOrElse(defaultContainerizers),
      "MESOS_ROLES" -> "public,foo",
      "MESOS_ACLS" -> s"file://$aclsPath",
      "MESOS_CREDENTIALS" -> s"file://$credentialsPath")
  }

  private def create(): Process = {
    val process = Process(s"mesos-local --ip=127.0.0.1 --port=$port", cwd = None, mesosEnv: _*)
    if (logStdout) {
      process.run()
    } else {
      process.run(ProcessLogger(_ => (), _ => ()))
    }
  }

  private var mesosLocal = Option.empty[Process]

  if (autoStart) {
    start()
  }

  def start(): Unit = if (mesosLocal.isEmpty) {
    mesosLocal = Some(create())
    val mesosUp = Retry(s"mesos-local-$port") {
      Http(system).singleRequest(Get(s"http://localhost:$port/version"))
    }
    Await.result(mesosUp, waitForStart)
  }

  def stop(): Unit = {
    mesosLocal.foreach { process =>
      process.destroy()
    }
    mesosLocal = Option.empty[Process]
  }

  override def close(): Unit = {
    Try(stop())
    Try(FileUtils.deleteDirectory(mesosWorkDir))
    Await.result(system.terminate(), waitForStart)
  }
}

trait MesosLocalTest extends BeforeAndAfterAll { this: Suite =>
  val mesosLocalServer = MesosLocal(autoStart = true)
  val port = mesosLocalServer.port

  override def afterAll(): Unit = {
    mesosLocalServer.close()
    super.afterAll()
  }
}