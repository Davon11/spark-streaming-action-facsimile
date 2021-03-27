package word.frequence.dao

import com.mchange.v2.c3p0.ComboPooledDataSource
import org.apache.log4j.{LogManager, Logger}
import word.frequence.util.Conf

import java.sql.Connection

class MysqlPool extends Serializable {
/*  lazy val log: Logger = LogManager.getLogger(this.getClass)

  private val conf: Map[String, String] = Conf.mysqlConf
  private val cpds = new ComboPooledDataSource(true)

  try {
    /*cpds.setJdbcUrl(conf.get("url").getOrElse("jdbc:mysql://10.95.144.204:3306/log_analysis?serverTimezone=Asia/Shanghai"))
    cpds.setUser(conf.getOrElse("user", "root"))
    cpds.setPassword(conf.getOrElse("password", "root"))
    cpds.setInitialPoolSize(Conf.initialPoolSize)
    cpds.setMaxPoolSize(Conf.maxPoolSize)
    cpds.setMinPoolSize(Conf.minPoolSize)*/

    cpds.setJdbcUrl("jdbc:mysql://10.95.144.204:3306/log_analysis?serverTimezone=Asia/Shanghai")
    cpds.setUser("root")
    cpds.setPassword("987654")
    cpds.setInitialPoolSize(Conf.initialPoolSize)
    cpds.setMaxPoolSize(Conf.maxPoolSize)
    cpds.setMinPoolSize(Conf.minPoolSize)

    cpds.setAcquireIncrement(5)
    cpds.setMaxStatements(180)
    /* 最大空闲时间,25000秒内未使用则连接被丢弃。若为0则永不丢弃。Default: 0 */
    cpds.setMaxIdleTime(25000)
    // 检测连接配置

    cpds.setPreferredTestQuery("SELECT `id` FROM user_words limit 1")
    cpds.setIdleConnectionTestPeriod(18000)
  }catch {
    case e: Exception =>
      log.error("[MysqlPoolError]", e)
  }

  def getConnection: Connection = {
    try {
      cpds.getConnection
    }catch {
      case e: Exception =>
        log.error("[MysqlPoolConnectError]", e)
        null
    }finally {
      cpds.close()
    }
  }

}

object MysqlManager {
  var mysqlManager: MysqlPool = _
  def getMysqlManager = {
    synchronized{
      if (mysqlManager == null) {
        mysqlManager = new MysqlPool
      }
    }
    mysqlManager
  }
}*/

  @transient lazy val log = LogManager.getLogger(this.getClass)

  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)
  private val conf = Conf.mysqlConf
  try {
    cpds.setJdbcUrl("jdbc:mysql://localhost:3306/log_analysis?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai")
    cpds.setDriverClass("com.mysql.cj.jdbc.Driver")
    cpds.setUser("root")
    cpds.setPassword("987654")
    cpds.setInitialPoolSize(3)
    cpds.setMaxPoolSize(Conf.maxPoolSize)
    cpds.setMinPoolSize(Conf.minPoolSize)
    cpds.setAcquireIncrement(2)
    cpds.setMaxStatements(180)
    /* 最大空闲时间,25000秒内未使用则连接被丢弃。若为0则永不丢弃。Default: 0 */
    cpds.setMaxIdleTime(25000)
    // 检测连接配置
    cpds.setPreferredTestQuery("select id from user_words limit 1")
    cpds.setIdleConnectionTestPeriod(18000)
  } catch {
    case e: Exception =>
      log.error("[MysqlPoolError]", e)
  }
  def getConnection: Connection = {
    try {
      cpds.getConnection
    } catch {
      case e: Exception =>
        log.error("[MysqlPoolGetConnectionError]", e)
        null
    }
  }
}
object MysqlManager {
  var mysqlManager: MysqlPool = _
  def getMysqlManager: MysqlPool = {
    synchronized {
      if (mysqlManager == null) {
        mysqlManager = new MysqlPool
      }
    }
    mysqlManager
  }
}
