import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory

/**
  * Created by Administrator on 2017/10/21.
  */
object HBaseTest {
  val str="232"
  str.diff()
val conf=HBaseConfiguration.create();
  val connection=ConnectionFactory.createConnection(conf)
  val table=connection.getTable(TableName.valueOf("tableName"));
}
