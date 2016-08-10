package mesosphere.util.state.mesos

import java.util.UUID
import java.util.concurrent.TimeUnit

import mesosphere.marathon.integration.setup.ZookeeperServerTest
import mesosphere.marathon.storage.repository.legacy.store.MesosStateStore
import mesosphere.util.state.PersistentStoreTest
import org.apache.mesos.state.ZooKeeperState
import org.scalatest.Matchers

import scala.concurrent.duration._

class MesosStateStoreTest extends PersistentStoreTest with ZookeeperServerTest with Matchers {

  //
  // See PersistentStoreTests for general store tests
  //

  lazy val persistentStore: MesosStateStore = {
    val duration = 30.seconds
    val state = new ZooKeeperState(
      s"localhost:${zkServer.port}",
      duration.toMillis,
      TimeUnit.MILLISECONDS,
      s"/${UUID.randomUUID}"
    )
    new MesosStateStore(state, duration)
  }
}
