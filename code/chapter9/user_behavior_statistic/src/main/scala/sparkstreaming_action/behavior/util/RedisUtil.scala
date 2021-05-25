package sparkstreaming_action.behavior.util

import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable.ArrayBuffer

object RedisUtil {
  var readRedis: Jedis = _
  var writeRedis: Jedis = _


  def getConnect(host: String, port: Int, password: String)  = {
    val jedis = new Jedis(host, port)
    jedis.connect()
    if (password.length > 0) {
      jedis.auth(password)
    }
    jedis
  }

  def closeConnect(jedis: Jedis) = {
    if (jedis != null) {
      jedis.close()
    }
  }

  def isConnected(jedis: Jedis) = {
    if (jedis == null) {
      false
    } else {
      jedis.isConnected
    }
  }

  def reconnect(jedis: Jedis) = {
    println("reconnecting...")
    closeConnect(jedis)
    getConnect(Conf.redisHost, Conf.redisPort, Conf.redisPW)
  }


  def keepConnected() = {
    if (!isConnected(readRedis)) {
      readRedis = reconnect(readRedis)
    }
    if (!isConnected(writeRedis)) {
      readRedis = reconnect(writeRedis)
    }
  }

  def batchSet(kvs: Seq[(Array[Byte], Array[Byte])]) = {
    try {
      var i = 1;
      while (i < kvs.length) {
        val target = i + Conf.redisBatchSize
        val pipeline = writeRedis.pipelined()
        while (i < kvs.length && i < target) {
          pipeline.set(kvs(i)._1, kvs(i)._2)
          pipeline.expire(kvs(i)._1, Conf.EXPIRE_DURATION)
          i += 1
        }
        pipeline.sync()
      }
    } catch {
      case connEx: JedisConnectionException => writeRedis = reconnect(writeRedis)
      case ex: Exception => ex.printStackTrace()
    }
  }

  def getBatchSet(keys: Array[String]) = {
    val valueBuffer = new ArrayBuffer[Array[Byte]]()
    try {
      keepConnected
      var i = 0;
      while (i < keys.length) {
        val target = i + Conf.redisBatchSize
        while (i < target && i < keys.length) {
          valueBuffer += readRedis.get(keys(i).getBytes)
          i += 1
        }
      }
    } catch {
      case connEx: JedisConnectionException => readRedis = reconnect(readRedis)
      case ex: Exception => ex.printStackTrace
    }
    valueBuffer

  }

  def retry[T](n: Int)(fn: => T): T = {
    scala.util.Try { fn } match {
      case util.Success(x) => x
      case _ if n > 1 => retry(n - 1)(fn)
      case util.Failure(exception) => {
        print("retry many times fail!!!!! T^T召唤神龙吧！\t" + exception)
        throw exception
      }
    }
  }


}
