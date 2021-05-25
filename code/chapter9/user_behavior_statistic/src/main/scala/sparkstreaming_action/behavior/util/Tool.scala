package sparkstreaming_action.behavior.util

import com.sun.xml.internal.messaging.saaj.util.{ByteInputStream, ByteOutputStream}

import java.io.{DataInputStream, DataOutputStream}
import scala.collection.mutable.ArrayBuffer

object Tool {
  def encode(newTable: Array[Array[Long]]) = {
    val bos = new ByteOutputStream()
    val dos = new DataOutputStream(bos)
    dos.write(newTable.length)
    for (i <- 0 until newTable.length) {
      for (j <- 0 until Conf.ENCODE_SIZE) {
        dos.writeLong(newTable(i)(j))
      }
    }
    bos.getBytes
  }

  def decode(encodeArr: Array[Byte]) = {
    val bis = new ByteInputStream(encodeArr, encodeArr.length)
    val dis = new DataInputStream(bis)
    val arrLength = dis.readInt()
    val decodeArr = new ArrayBuffer[Array[Long]](arrLength)
    var arr: Array[Long] = null
    for (i <- 0 until arrLength) {
      arr = new Array[Long](Conf.ENCODE_SIZE)
      for (j <- 0 until Conf.ENCODE_SIZE) {
        arr(j) = dis.readLong()
      }
      decodeArr += arr
    }
    decodeArr.toArray
  }

  def isInvalidDate(time: Long, duration: Long): Boolean = {
    val threshold = System.currentTimeMillis() - duration
    threshold > time
  }

}
