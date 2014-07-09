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

package ict.wde.domino.common.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.io.Writable;

/**
 * Writable class for Endpoint result transport.
 * 
 * @author Zhen Zhao, ICT, CAS
 * 
 */
public class DResult implements Writable {

  private Result result;
  private String errMsg;

  /**
   * For RPC call. DO NOT USE.
   */
  public DResult() {
  }

  public DResult(Result result, String errMsg) {
    this(result, errMsg, false);
  }

  public DResult(Result result, String errMsg, boolean isNumeric) {
    this.result = result;
    this.errMsg = errMsg;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    byte b = in.readByte();
    switch (b) {
    case 1: {
      result = new Result();
      result.readFields(in);
      break;
    }
    default:
    }
    b = in.readByte();
    if (b == 1) {
      errMsg = in.readUTF();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (result == null) {
      out.writeByte(0);
    }
    else {
      out.writeByte(1);
      result.write(out);
    }
    if (errMsg == null) {
      out.writeByte(0);
    }
    else {
      out.writeByte(1);
      out.writeUTF(errMsg);
    }
  }

  public Result getResult() {
    return result;
  }

  public void setResult(Result result) {
    this.result = result;
  }

  public String getErrMsg() {
    return errMsg;
  }

  public void setErrMsg(String errMsg) {
    this.errMsg = errMsg;
  }

}
