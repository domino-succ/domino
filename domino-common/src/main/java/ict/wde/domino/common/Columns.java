/**
 *  Domino, A Transaction Engine Based on Apache HBase
 *  Copyright (C) 2014  Zhen Zhao
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package ict.wde.domino.common;

import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * A class used to store column family and qualifier information and provide
 * serialization support.
 * 
 * @author Zhen Zhao, ICT, CAS
 * 
 */
public class Columns {

  public final NavigableSet<Column> cols = new TreeSet<Column>();
  private int len = 0;

  public Columns(byte[] old) {
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
      len += Bytes.SIZEOF_INT + columnLen;
    }
  }

  public int length() {
    return len;
  }

  public void add(byte[] family, byte[] qualifier) {
    if (family == null || qualifier == null) return;
    Column col = new Column(family, qualifier);
    if (cols.contains(col)) return;
    len += Bytes.SIZEOF_INT + col.length;
    cols.add(col);
  }

  public boolean contains(byte[] family, byte[] qualifier) {
    return cols.contains(new Column(family, qualifier));
  }

  public byte[] toByteArray() {
    byte[] ret = new byte[Bytes.SIZEOF_INT + len];
    int pos = 0;
    pos = Bytes.putInt(ret, pos, cols.size());
    for (Column col : cols) {
      pos = Bytes.putInt(ret, pos, col.length);
      pos = col.toBytes(ret, pos);
    }
    return ret;
  }

  public static class Column implements Comparable<Column> {
    public final byte[] family;
    public final byte[] qualifier;
    public final int length;

    public Column(byte[] family, byte[] qualifier) {
      this.family = family;
      this.qualifier = qualifier;
      this.length = Bytes.SIZEOF_INT + family.length + qualifier.length;
    }

    @Override
    public int compareTo(Column o) {
      int cmpFamily = Bytes.compareTo(family, o.family);
      if (cmpFamily != 0) return cmpFamily;
      return Bytes.compareTo(qualifier, o.qualifier);
    }

    public int toBytes(byte[] bytes, int pos) {
      pos = Bytes.putInt(bytes, pos, family.length);
      pos = Bytes.putBytes(bytes, pos, family, 0, family.length);
      return Bytes.putBytes(bytes, pos, qualifier, 0, qualifier.length);
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
}
