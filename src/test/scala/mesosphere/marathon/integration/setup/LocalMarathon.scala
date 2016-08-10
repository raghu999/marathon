package mesosphere.marathon.integration.setup

import java.io.File
import java.nio.file.Files
import java.util.concurrent.Semaphore

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Get
import akka.stream.ActorMaterializer
import mesosphere.marathon.util.Retry
import mesosphere.marathon.{AllConf, MarathonApp}
import mesosphere.util.PortAllocator
import org.apache.commons.io.FileUtils

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.util.Try

/**
  * Runs marathon _in process_ with all of the dependencies wired up.
  */
class LocalMarathon(autoStart: Boolean = true,
                    conf: Map[String, String] = Map.empty) extends AutoCloseable {
  lazy val zk = ZookeeperServer(autoStart = true)
  lazy val mesos = MesosLocal(autoStart = true)
  private val semaphore = new Semaphore(0)
  private var started = false
  private implicit lazy val system = ActorSystem()
  private implicit lazy val materializer = ActorMaterializer()
  private implicit lazy val scheduler = system.scheduler
  private implicit lazy val ctx = ExecutionContext.global


  lazy val httpPort = PortAllocator.ephemeralPort()
  private val workDir = {
    val f = Files.createTempDirectory(s"marathon-$httpPort").toFile
    f.deleteOnExit()
    f
  }
  private def write(dir: File, fileName: String, content: String): String = {
    val file = File.createTempFile(fileName, "", dir)
    file.deleteOnExit()
    FileUtils.write(file, content)
    file.setReadable(true)
    file.getAbsolutePath
  }

  private val secretPath = write(workDir, fileName = "marathon-secret", content = "secret1")

  lazy val config = conf ++ Map(
    "mesos_authentication_principal" -> "principal",
    "mesos_role" -> "foo",
    "http_port" -> httpPort.toString,
    "zk" -> s"zk://${zk.connectUri}/marathon",
    "mesos_authentication_secret_file" -> s"$secretPath"
  )

  private var closing = false
  private val marathon = new MarathonApp() with AutoCloseable {
    override val conf: AllConf = new AllConf(config.flatMap { case (k,v) => Seq(s"--$k", v)}(collection.breakOut))
    override def close(): Unit = super.shutdown()
  }

  private val thread = new Thread(new Runnable {
    override def run(): Unit = {
      while (!closing) {
        marathon.runDefault()
        semaphore.acquire()
      }
    }
  }, s"marathon-$httpPort")

  if (autoStart) {
    start()
  }

  def start(): Unit = {
    if (!started) {
      if (thread.getState == Thread.State.NEW) {
        thread.start()
      }
      started = true
      semaphore.release()
      Retry(s"marathon-$httpPort") {
        Http(system).singleRequest(Get(s"http://localhost:$httpPort/version"))
      }
    }
  }

  override def close(): Unit = {
    Try(zk.close())
    Try(mesos.close())
    Await.result(system.terminate(), 5.seconds)
  }
}
