package sparkstreaming_action.behavior.util

import redis.clients.jedis.Jedis

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

  def closeConne(jedis: Jedis) = {
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

  def reConnected(jedis: Jedis) = {
    println("reconnecting...")
    closeConne(jedis)
    getConnect(Conf.redisHost, Conf.redisPort, Conf.redisPW)
  }


  def keepConnected() = {
    if (!isConnected(readRedis)) {
      readRedis = reConnected(readRedis)
    }
    if (!isConnected(writeRedis)) {
      readRedis = reConnected(writeRedis)
    }
  }


}
