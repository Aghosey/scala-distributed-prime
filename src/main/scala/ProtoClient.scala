import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}
import java.util.logging.{Level, Logger}

import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import protocols.Sieve.SieveServerGrpc.SieveServerBlockingStub
import protocols.Sieve._

object ProtoClient {
  def apply(host: String, port: Int): ProtoClient = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    val blockingStub = SieveServerGrpc.blockingStub(channel)
    new ProtoClient(channel, blockingStub)
  }

  def main(args: Array[String]): Unit = {
    val client = ProtoClient("localhost", 9999)
    try {
      client.send()
    } finally {
      client.shutdown()
    }

  }
}

class ProtoClient private(
                           private val channel: ManagedChannel,
                           private val blockingStub: SieveServerBlockingStub
                         ) {
  private[this] val logger = Logger.getLogger(classOf[ProtoClient].getName)

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  def send(): Unit = {
    val healthCheckPeriod = 5
    val ex = new ScheduledThreadPoolExecutor(1)

    var request = TaskRequest(Option(null), getTask = true)
    var response = blockingStub.getTask(request)

    var i = 1
    try {
      while (i < 500) {
        i += 1
        if (!response.pending) {
          val thread = new Runnable {
            override def run(): Unit = {
              val healthCheckResponse = blockingStub.healthCheck(HealthCheckRequest(response.clientId))
              logger.info("healthCheck stop: " + healthCheckResponse.stop)
              if (!healthCheckResponse.stop) {
                ex.schedule(this, healthCheckPeriod, TimeUnit.SECONDS)
              }
            }
          }
          ex.schedule(thread, healthCheckPeriod, TimeUnit.SECONDS)

          val wheelMod: BigInt = BigIntUtils.read(response.wheelMod.get)
          val p: BigInt = BigIntUtils.read(response.prime.get)
          val pi: BigInt = BigIntUtils.read(response.pi.get)

          println(
            s"Got task: \n" +
              s"wheelMod = $wheelMod \n" +
              s"p        = $p \n" +
              s"pi       = $pi \n"
          )

          val quasiPrimes: Array[BInteger] = (wheelMod until (p * pi) by pi).map(BigIntUtils.write).toArray
          val removeLock: BInteger = BigIntUtils.write(wheelMod *p)

          logger.info("Getting a task ...")
          request = TaskRequest(Option(SievedWheel(quasiPrimes, Option(removeLock), clientId = response.clientId)), getTask = true)
          response = blockingStub.getTask(request)

        } else {
          logger.info("Getting a task ...")

          request = TaskRequest(Option(null), getTask = true)
          response = blockingStub.getTask(request)
        }
      }
    }
    catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
    }
  }
}