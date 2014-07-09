package ict.wde.domino.id;

import ict.wde.domino.common.DominoConst;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class CheckPreAllocTid {
  public static void main(String[] args) throws Throwable {
    // System.setProperty(TidDef.PROP_STANDALONE, "true");
    Configuration conf = new Configuration();
    conf.set(DominoConst.ZK_PROP, "p145:2181");
    HTable table = new HTable(conf, DominoConst.TID_EP_TABLE);
    long tid = Bytes.toLong(table.get(new Get(TidDef.EP_ROW)).getValue(
        TidDef.EP_FAMILY, TidDef.EP_COLUMN));
    System.out.println(tid);
    table.close();
  }
}
