import java.io._
import java.nio.ByteBuffer
import java.util.Scanner
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import io.grpc.{Server, ServerBuilder}
import org.apache.log4j.Logger
import protocols.Sieve._
import boopickle.Default._

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
    private var outPrimes: PrintWriter = _

    private var checkPoint: CheckPoint = _
    private val checkPointFile: File = new File("files/checkPoint.bin")
    private var lastPrime: BigInt = 5
    private val primesFile: File = new File("files/primes.txt")

    if (checkPointFile.exists() && primesFile.exists()) {
      val fip = new FileInputStream(checkPointFile)
      val bytes: Array[Byte] = new Array[Byte](checkPointFile.length().toInt)
      fip.read(bytes)

      checkPoint = Unpickle[CheckPoint].fromBytes(ByteBuffer.wrap(bytes))
      setStateFromCheckPoint()
      val primesScanner = new Scanner(new FileInputStream(primesFile))
      var lastLine = "5"
      while (primesScanner.hasNextLine) {
        lastLine = primesScanner.nextLine()
      }
      lastPrime = BigInt(lastLine)
      primesScanner.close()
      outPrimes = new PrintWriter(new FileOutputStream(primesFile, true))

    } else {
      initState()
    }

    private val clientsLastHealthCheck = mutable.Map[Long, Long]()

    var pi: BigInt = _
    var p: BigInt = _
    var wheelInitSize: BigInt = _
    var assignedTasksCount: BigInt = _
    var wheelRemain: SortedSet[BigInt] = _
    var nextWheel: SortedSet[BigInt] = _
    var removeLocked: SortedSet[BigInt] = _

    def setStateFromCheckPoint(): Unit = {
      pi = checkPoint.pi
      p = checkPoint.p
      wheelInitSize = checkPoint.wheelInitSize
      assignedTasksCount = checkPoint.assignedTasks
      wheelRemain = SortedSet[BigInt]() ++ checkPoint.wheelRemain
      nextWheel = SortedSet[BigInt]() ++ checkPoint.nextWheel
      removeLocked = SortedSet[BigInt]() ++ checkPoint.removeLocked
    }

    def initState(): Unit = {
      pi = 6
      p = 5
      wheelInitSize = 2
      assignedTasksCount = 0
      wheelRemain = SortedSet[BigInt](1, 5)
      nextWheel = SortedSet[BigInt]()
      removeLocked = SortedSet[BigInt]()

      outPrimes = new PrintWriter(new FileOutputStream(primesFile))
      outPrimes println 2
      outPrimes println 3
      outPrimes println 5
      outPrimes.flush()
    }

    override def getTask(request: TaskRequest): Future[Task] = {
      val taskResponse = request.prevTaskResponse
      if (taskResponse.nonEmpty && taskResponse.get.quasiPrimes.nonEmpty) {
        nextWheel ++= taskResponse.get.quasiPrimes.map(BigIntUtils.read)
        removeLocked += BigIntUtils.read(taskResponse.get.removeLock.get)
        clientsLastHealthCheck -= taskResponse.get.clientId
      }
      var wheelMod: BigInt = null
      this.synchronized {
        if (wheelRemain.isEmpty) {
          if (assignedTasksCount != wheelInitSize) {
            return Future.successful(Task(pending = true))
          } else {
            prepareNextWheel()
          }
        }
      }

      if (request.getTask) {
        wheelMod = wheelRemain.head
        wheelRemain -= wheelMod
        var clientId: Long = Random.nextLong()
        if (taskResponse.nonEmpty) {
          clientId = taskResponse.get.clientId
        } else {
          while (clientsLastHealthCheck.contains(clientId)) {
            clientId = Random.nextLong()
          }
        }
        clientsLastHealthCheck += (clientId -> System.currentTimeMillis())
        Future.successful(getNewTask(wheelMod, clientId))
      } else {
        Future.successful(Task())
      }
    }

    override def healthCheck(sieveRequest: HealthCheckRequest): Future[HealthCheckResponse] = {
      if (clientsLastHealthCheck.contains(sieveRequest.clientId)) {
        clientsLastHealthCheck(sieveRequest.clientId) = System.currentTimeMillis()
        Future.successful(HealthCheckResponse())
      } else {
        Future.successful(HealthCheckResponse(stop = true))
      }
    }

    def printWheelPrimes(): Unit = {
      val it: Iterator[BigInt] = wheelRemain.iterator
      var break = false
      while (!break && it.hasNext) {
        val prime = it.next()
        println(prime)
        if (prime > lastPrime) {
          if (prime < p * p) {
            outPrimes println prime
            lastPrime = prime
          } else {
            break = true
          }
        }
      }
      outPrimes flush()


      checkPoint = CheckPoint(pi, p, wheelInitSize, assignedTasksCount, wheelRemain.toArray, nextWheel.toArray, removeLocked.toArray)
      val fop = new FileOutputStream(checkPointFile)
      fop.write(Pickle.intoBytes(checkPoint).array())
      fop.close()
    }

    def prepareNextWheel(): Unit = {
      nextWheel --= removeLocked
      wheelRemain = nextWheel

      wheelInitSize = wheelRemain.size
      assignedTasksCount = 0

      pi *= p
      p = (nextWheel - 1).head

      nextWheel = SortedSet[BigInt]()
      removeLocked = SortedSet[BigInt]()

      printWheelPrimes()
    }

    private val healthCheckPeriod = 5
    private val healthCheckThreshold = healthCheckPeriod * 2
    private val ex = new ScheduledThreadPoolExecutor(8)

    def getNewTask(wheelMod: BigInt, clientId: Long): Task = {
      val healthCheck = new Runnable {
        def run(): Unit = {
          if (System.currentTimeMillis() - clientsLastHealthCheck(clientId) > TimeUnit.SECONDS.toMillis(healthCheckPeriod)) {
            clientsLastHealthCheck -= clientId
            wheelRemain += wheelMod
            assignedTasksCount -= 1
          } else {
            ex.schedule(this, healthCheckThreshold, TimeUnit.SECONDS)
          }
        }
      }
      assignedTasksCount += 1
      ex.schedule(healthCheck, healthCheckThreshold, TimeUnit.SECONDS)
      Task(
        pending = false,
        Option(BigIntUtils.write(pi)),
        Option(BigIntUtils.write(p)),
        Option(BigIntUtils.write(wheelMod)),
        clientId
      )
    }

    case class CheckPoint(pi: BigInt,
                          p: BigInt,
                          wheelInitSize: BigInt,
                          assignedTasks: BigInt,
                          wheelRemain: Array[BigInt],
                          nextWheel: Array[BigInt],
                          removeLocked: Array[BigInt],
                         ) extends Serializable

  }

}
