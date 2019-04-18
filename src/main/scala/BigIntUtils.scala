import com.google.protobuf.ByteString
import protocols.Sieve._

object BigIntUtils {
  def write(bigInteger: BigInt): BInteger = {
    val bytes: ByteString = ByteString.copyFrom(bigInteger.toByteArray)
    BInteger(bytes)
  }

  def read(message: BInteger): BigInt = {
    val bytes: ByteString = message.value
    BigInt(bytes.toByteArray)
  }
}
