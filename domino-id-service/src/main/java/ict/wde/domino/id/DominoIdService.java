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

package ict.wde.domino.id;

import ict.wde.domino.id.cop.TidEPClient;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * DTO Client generator.
 * 
 * @author Zhen Zhao, ICT, CAS
 * 
 */
public class DominoIdService {

  public static DominoIdIface getClient(Configuration config) throws IOException {
    return new TidEPClient(config);
  }

}
