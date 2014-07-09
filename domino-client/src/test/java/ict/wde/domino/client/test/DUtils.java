package ict.wde.domino.client.test;

import ict.wde.domino.common.DominoConst;

import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class DUtils {
  public static void printResult(Result r) {
    System.out.println("-----" + new String(r.getRow()) + "-----");
    for (byte[] cf : r.getMap().keySet()) {
      for (byte[] col : r.getMap().get(cf).keySet()) {
        for (Long ver : r.getMap().get(cf).get(col).keySet()) {
          String value;
          byte[] val = r.getMap().get(cf).get(col).get(ver);
          if (Bytes.equals(cf, DominoConst.INNER_FAMILY)) {
            if (Bytes.equals(col, DominoConst.STATUS_COL)) {
              value = val[0] + "+" + Bytes.toLong(val, 1);
            }
            else if (Bytes.equals(col, DominoConst.COLUMNS_COL)) {
              value = new Columns(val).toString();
            }
            else {
              value = val[0] + "+" + Bytes.toLong(val, 1);
            }
          }
          else {
            value = new String(val);
          }
          System.out.println(new String(cf) + " - " + new String(col) + " - "
              + ver + " - " + value);
        }
      }
    }
  }

  public static void printTransaction(Result r) {
    System.out.print(DominoConst.getTidFromTMetaKey(r.getRow()) + ": ");
    byte[] status = r.getValue(DominoConst.TRANSACTION_META_FAMILY,
        DominoConst.TRANSACTION_STATUS);
    switch (status[0]) {
    case DominoConst.TRX_ACTIVE:
      System.out.println("Active");
      return;
    case DominoConst.TRX_ABORTED:
      System.out.println("Aborted");
      return;
    case DominoConst.TRX_COMMITTED:
      byte[] commitId = r.getValue(DominoConst.TRANSACTION_META_FAMILY,
          DominoConst.TRANSACTION_COMMIT_ID);
      System.out.println("Committed @ " + Bytes.toLong(commitId));
    }
  }

  private static class Column implements Comparable<Column> {
    final byte[] family;
    final byte[] qualifier;

    public Column(byte[] family, byte[] qualifier) {
      this.family = family;
      this.qualifier = qualifier;
    }

    @Override
    public int compareTo(Column o) {
      int cmpFamily = Bytes.compareTo(family, o.family);
      if (cmpFamily != 0) return cmpFamily;
      return Bytes.compareTo(qualifier, o.qualifier);
    }

    public static Column fromBytes(byte[] bytes, int pos, int length) {
      int familyLen = Bytes.toInt(bytes, pos);
      pos += Bytes.SIZEOF_INT;
      byte[] family = new byte[familyLen];
      Bytes.putBytes(family, 0, bytes, pos, familyLen);
      pos += familyLen;
      int qualifierLen = length - Bytes.SIZEOF_INT - familyLen;
      byte[] qualifier = new byte[qualifierLen];
      Bytes.putBytes(qualifier, 0, bytes, pos, qualifierLen);
      return new Column(family, qualifier);
    }

  }

  private static class Columns {

    private final NavigableSet<Column> cols = new TreeSet<Column>();

    Columns(byte[] old) {
      if (old == null || old.length == 0) {
        return;
      }
      int pos = 0;
      int count = Bytes.toInt(old, pos);
      pos += Bytes.SIZEOF_INT;
      for (int i = 0; i < count; ++i) {
        int columnLen = Bytes.toInt(old, pos);
        pos += Bytes.SIZEOF_INT;
        cols.add(Column.fromBytes(old, pos, columnLen));
        pos += columnLen;
      }
    }

    public String toString() {
      StringBuffer sb = new StringBuffer(128);
      for (Column col : cols) {
        sb.append(new String(col.family)).append('.')
            .append(new String(col.qualifier));
        sb.append(", ");
      }
      return sb.toString();
    }
  }
}
