package word.frequence.util

object Conf extends Serializable {
  val code01Host = "10.95.144.204"

  val mysqlConf = Map("url" -> f"jdbc:mysql://$code01Host:3306/log_analysis?serverTimezone=Asia/Shanghai",
                      "user"-> "root","password"-> "987654")
  val initialPoolSize = 3
  val maxPoolSize = 5
  val minPoolSize = 2
}
