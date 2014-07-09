package ict.wde.domino.test;

public class Env {
  public static final String HBASE_SITE = //
  "F:/wdse/hbase/src/java/coprocessor-test/hbase-site.xml";
  static final String TABLE = "dom-test";
  static final String[] CF = { "cf1" };
  static final String[][] COL = { { "col1", "col2" } };
  static final String[] SPL = { "1", "2", "3", "4", "5", "6" };
  static final byte[][] SPLB;
  static {
    SPLB = new byte[SPL.length][];
    for (int i = 0; i < SPL.length; ++i) {
      SPLB[i] = SPL[i].getBytes();
    }
  }
}
