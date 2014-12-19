/**
 *  Copyright 2014 ZhenZhao and Tieying Zhang. 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
