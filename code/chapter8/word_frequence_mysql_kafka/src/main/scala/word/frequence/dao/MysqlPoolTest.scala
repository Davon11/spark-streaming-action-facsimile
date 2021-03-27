package word.frequence.dao

object MysqlPoolTest {
  def main(args: Array[String]): Unit = {
    val connection = MysqlManager.getMysqlManager.getConnection
    connection.setAutoCommit(false)
    val statement = connection.createStatement()
    val sql = "INSERT INTO `user_words` VALUE ('2', '书籍', '2018-07-31 21:28:32')"
    statement.addBatch(sql)
    statement.executeBatch()
    connection.commit()
    statement.close()
    connection.close()
  }

}
