import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by Administrator on 2017/10/21.
 */
public class HbaseTest {
    public static void test() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin=connection.getAdmin();

        HBaseAdmin admin2=new HBaseAdmin(connection);

//        HTable htable=new HTable(conf);
        Table table = connection.getTable(TableName.valueOf("TableName"));
        Put put = new Put(Bytes.toBytes("rowkey"));

        put.addColumn(Bytes.toBytes("family"), Bytes.toBytes("column"), Bytes.toBytes("value"));
Scan scan=new Scan();
Filter ccf=new ColumnCountGetFilter(2);
scan.setStartRow(Bytes.toBytes("rowkey0"));
        scan.setStopRow(Bytes.toBytes("rowkey100"));
        scan.setFilter(ccf);
        scan.setRowPrefixFilter(Bytes.toBytes("time"));
        ResultScanner scanner=table.getScanner(scan);
        for(Result res:scanner){
            for (Cell cell:res.rawCells()){
                System.out.println("KV: " + cell + ", Value: " + Bytes.toString(CellUtil.cloneValue(cell)));

            }
        }
        table.close();
        connection.close();
    }
    public String concatenetedString(String s1, String s2) {
        // write your code here
        char[] char1s=s1.toCharArray();
        char[] char2s=s2.toCharArray();
        StringBuffer sb1=new StringBuffer("");
        StringBuffer sb2=new StringBuffer("");
        for(int i=0;i<char1s.length;i++){
            Boolean flag=false;//默认第二个中没有重复的
            for(int j=0;j<char2s.length;j++){
                if(char1s[i]==char2s[j]){
                    flag=true;
                }
            }
            if(flag==false){ sb1.append(String.valueOf(char1s[i]));   }
        }

        for(int i=0;i<char2s.length;i++){
            Boolean flag=false;//默认第二个中没有重复的
            for(int j=0;j<char1s.length;j++){
                if(char2s[i]==char1s[j]){
                    flag=true;
                }
            }
            if(flag==false) {sb2.append(String.valueOf(char2s[i])); }
        }
        return sb1.toString()+sb2.toString();
    }
}
