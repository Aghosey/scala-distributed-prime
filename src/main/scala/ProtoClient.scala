import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}

import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import protocols.Sieve.SieveServerGrpc.SieveServerBlockingStub
import protocols.Sieve.{BInteger, SieveServerGrpc, SievedWheel, TaskRequest}

object ProtoClient {
  def apply(host: String, port: Int): ProtoClient = {
    val channel = ManagedChannelBuilder.forAddress(host, port).build
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
    logger.info("Getting a task ...")
    val request = TaskRequest(Option(SievedWheel()), getTask = true)
    try {
      val response = blockingStub.getTask(request)

      if (!response.pending) {
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
        val removeLock: BInteger = BigIntUtils.write(wheelMod * p)

        blockingStub.getTask(TaskRequest(Option(SievedWheel(quasiPrimes, Option(removeLock)))))
      }
    }
    catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
    }
  }
}