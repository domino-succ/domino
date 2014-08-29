import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import ict.wde.domino.client.Domino;
import ict.wde.domino.client.Transaction;



public class testCon {

	private Domino domino;
	private Transaction transaction;
	
	public testCon(String addressPort) throws IOException{
		domino = new Domino(addressPort);
		transaction = domino.startTransaction();
	}
	public void createTable(String tablename){
		
		try {
			System.out.println("start create table ......");
			if( false == domino.tableExists(tablename) ){
				HTableDescriptor tableDescriptor = new HTableDescriptor(tablename);  
				tableDescriptor.addFamily(new HColumnDescriptor("c1"));  
				tableDescriptor.addFamily(new HColumnDescriptor("c2"));  
				tableDescriptor.addFamily(new HColumnDescriptor("c3"));
				domino.createTable(tableDescriptor);
				System.out.println("create table successfully......");
			}else{
				System.out.println("the table exists ......");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void listTable(String tableName) throws IOException{
		if ( true == domino.tableExists(tableName) ){
			Scan scan = new Scan();
			ResultScanner rs = transaction.scan(scan, tableName.getBytes());
			transaction.commit();
			for(Result r : rs){
				System.out.printf("the row is " + new String(r.getRow()) );
				for(KeyValue keyValue : r.raw() ){
					System.out.printf( " | the cloumnfamily : " + new String(keyValue.getFamily())
							+ " the value : " + new String(keyValue.getValue()) );
				}
				System.out.println("\nscan all");
			}
		}else{
			System.out.println("the table " + tableName + "does not exist");
		}
	}
	
	public void put(String[] args) throws IOException{
		if ( true == domino.tableExists(args[1]) ){
			Put put = new Put(args[2].getBytes());
			put.add("c1".getBytes(),"name".getBytes(),args[3].getBytes());
			put.add("c2".getBytes(),"courses".getBytes(),args[4].getBytes());
			put.add("c3".getBytes(),"scores".getBytes(),args[5].getBytes());
			transaction.put(put, args[1].getBytes());
			transaction.commit();
			System.out.println("put into table " + args[1]  + "successfully");
		}else{
			System.out.println("the table " + args[1] + "does not exist");
		}
	}
	
	public void deleteTable(String tableName) throws IOException{
		if ( true == domino.tableExists(tableName) ){
			domino.dropTable(tableName);
			System.out.println("delete table " + tableName + "successfully");
		}else{
			System.out.println("the table " + tableName + "does not exist");
		}
	}
	
	public void getValue(String tableName, String rowKey) throws IOException{
		if ( true == domino.tableExists(tableName) ){
			Get get = new Get(rowKey.getBytes());
			Result rs = transaction.get(get, tableName.getBytes());
			transaction.commit();
			System.out.printf( tableName + "-" + rowKey + ":" ); 
			for (KeyValue keyValue : rs.raw() ){
				System.out.printf("cloumn:" + new String(keyValue.getFamily()) + " value"
						+ new String(keyValue.getValue()) + "|" );
			}
			System.out.println("\nget all");
		}else{
			System.out.println("the table " + tableName + "does not exist");
		}
	}
	
	public void deleteValue(String tableName, String rowKey) throws IOException{
		if ( true == domino.tableExists(tableName) ){
			transaction.delete(rowKey.getBytes(), tableName.getBytes());
			transaction.commit();
			System.out.println("delete row " + rowKey);
		}else{
			System.out.println("the table " + tableName + "does not exist");
		}
	}
	
	public static void main(String[] args) throws IOException {
		
		testCon testCon = new testCon("processor018:2181");
		switch( args[0] ){
				case "createTable":testCon.createTable(args[1]);break;
				case "list"  :testCon.listTable(args[1]);break;
				case "put"	 :testCon.put(args);break;
				case "deleteTable":testCon.deleteTable(args[1]);break;
				case "get"   :testCon.getValue(args[1] ,args[2]);break;
				case "deleteRow":testCon.deleteValue(args[1] ,args[2]);break;
		}
	}
}
