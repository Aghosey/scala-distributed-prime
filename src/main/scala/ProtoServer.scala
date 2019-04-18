import io.grpc.{Server, ServerBuilder}
import org.apache.log4j.Logger
import protocols.Sieve._

import scala.collection.immutable.SortedSet
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

object ProtoServer {
  private val logger: Logger = Logger.getLogger("Protobuf Sever")
  private val port: Int = 9999

  def main(args: Array[String]): Unit = {
    val server: ProtoServer = new ProtoServer(ExecutionContext.global)
    server start()
    server blockUntilShutdown()
  }
}

class ProtoServer(executionContext: ExecutionContext) {
  self =>
  private var server: Server = _

  private def start(): Unit = {
    server = ServerBuilder.
      forPort(ProtoServer.port).
      addService(SieveServerGrpc.bindService(new SieveServerImp(), executionContext)).
      build.start
    ProtoServer.logger.info("Server started, listening on " + ProtoServer.port)
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        ProtoServer.logger.info("*** shutting down gRPC server since JVM is shutting down")
        self.stop()
        ProtoServer.logger.error("*** server shut down")
      }
    })
  }

  private def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  private def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  private class NumRecord(val num: BigInt) {
    val _num: BigInt = num
  }

  private class SieveServerImp extends SieveServerGrpc.SieveServer {
//    private val outPrimes: PrintWriter = new PrintWriter(new File("primes.txt"))
    println(2)
    println(3)
    println(5)

    private var pi: BigInt = 6
    private var p: BigInt = 5
    private var wheelInitSize: BigInt = 2
    private var assignedTasks: BigInt = 0
    private var wheelRemain = SortedSet[BigInt](1, 5)
    private var nextWheel = SortedSet[BigInt]()
    private var removeLocked = SortedSet[BigInt]()
    private val assignedClients = mutable.Map[Long, AssignmentHealthCheck]()

    override def getTask(request: TaskRequest): Future[Task] = {
      println(nextWheel)
      val taskResponse: SievedWheel = request.prevTaskResponse.get
      if (taskResponse.quasiPrimes.nonEmpty) {
        nextWheel ++= taskResponse.quasiPrimes.map(BigIntUtils.read)
        removeLocked += BigIntUtils.read(taskResponse.removeLock.get)
      }
      var wheelMod: BigInt = null
      this.synchronized {
        if (wheelRemain.isEmpty) {
          if (assignedTasks != wheelInitSize) {
            return Future.successful(Task(pending = true))
          } else {
            nextWheel --= removeLocked
            wheelRemain = nextWheel

            wheelInitSize = wheelRemain.size
            assignedTasks = 0

            pi *= p
            p = (nextWheel - 1).head

            nextWheel = SortedSet[BigInt]()
            removeLocked = SortedSet[BigInt]()

            println(p)
          }
        }
      }
      if (request.getTask) {
        wheelMod = wheelRemain.head
        wheelRemain -= wheelMod
        assignedTasks += 1
        var clientId: Long = Random.nextLong()
        while (assignedClients.contains(clientId)) {
          clientId = Random.nextLong()
        }
        assignedClients += (clientId -> new AssignmentHealthCheck(wheelMod))
        val reply: Task = Task(
          pending = false,
          Option(BigIntUtils.write(pi)),
          Option(BigIntUtils.write(p)),
          Option(BigIntUtils.write(wheelMod)),
          clientId
        )
        Future.successful(reply)
      } else {
        Future.successful(Task())
      }
    }

    override def healthCheck(sieveRequest: HealthCheckRequest): Future[HealthCheckResponse] = {
      if (assignedClients.contains(sieveRequest.clientId)) {
        assignedClients(sieveRequest.clientId).healthCheck()
        Future.successful(HealthCheckResponse())
      } else {
        Future.successful(HealthCheckResponse(stop = true))
      }
    }

    class AssignmentHealthCheck(var _assignedMod: BigInt) {
      private var lastHealthCheck: Long = System.currentTimeMillis()
      var assignedMod: BigInt = _assignedMod

      def healthCheck(): Unit = {
        lastHealthCheck = System.currentTimeMillis()
      }

      def getLastHealthCheck: Long = lastHealthCheck
    }
  }
}
