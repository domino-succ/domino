package ict.wde.domino.client.test;

import ict.wde.domino.client.Domino;
import ict.wde.domino.common.DominoConst;

public class CreateTable {
  public static void main(String[] args) throws Exception {
    Domino dom = new Domino("processor145:2181");
    dom.dropTable(DominoConst.TID_EP_TABLE);
    // dom.createTable(DominoConst.TRANSACTION_META_DESCRIPTOR);
    // HTableDescriptor table = new HTableDescriptor("dom-test");
    // table.addFamily(new HColumnDescriptor("test-cf1"));
    // dom.createTable(table);
    dom.close();
  }
}
