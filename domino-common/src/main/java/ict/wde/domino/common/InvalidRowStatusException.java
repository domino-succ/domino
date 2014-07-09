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

import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * Exception class for conflict detection.
 * 
 * @author Zhen Zhao, ICT, CAS
 * 
 */
public class InvalidRowStatusException extends DoNotRetryIOException {

  private static final long serialVersionUID = 1131951357818835479L;

  public InvalidRowStatusException() {
    super();
  }

  public InvalidRowStatusException(String msg) {
    super(msg);
  }

  public InvalidRowStatusException(String msg, Throwable t) {
    super(msg, t);
  }

}
