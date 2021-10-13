package tn.insat.tp4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

import java.io.IOException;

public class HelloHBase {
	private Table table1;
    private String tableName = "fartas";
    private String family1 = "loc";
    private String family2 = "name";
	private String family3 = "reg";
	private String family4	= "measure";
    public void createHbaseTable() throws IOException {
      Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();

        HTableDescriptor ht = new HTableDescriptor(TableName.valueOf(tableName));
        ht.addFamily(new HColumnDescriptor(family1));
        ht.addFamily(new HColumnDescriptor(family2));
		ht.addFamily(new HColumnDescriptor(family3));
		ht.addFamily(new HColumnDescriptor(family4));
        System.out.println("connecting");

        System.out.println("Creating Table");
        createOrOverwrite(admin, ht);
        System.out.println("Done......");
		table1 = connection.getTable(TableName.valueOf(tableName));
		BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(
                    "/root/taki.txt"));
            String line = reader.readLine();
            line = reader.readLine();
            StringTokenizer st1 ;
            int i=0;
            int j=0;
            Put p;
			String kk;
            while (line != null) {

                
                byte[] row1 = Bytes.toBytes(j);
                p = new Put(row1);
                j++;
                st1= new StringTokenizer(line,",");
                i=0;
                while(st1.hasMoreTokens()){
					
					

                    if(i==0){
						
                        p.addColumn(family3.getBytes(), "country".getBytes(), Bytes.toBytes(kk));
                        i++;
						continue;
                    }
                    if(i==1){
						
                        p.addColumn(family2.getBytes(), "short".getBytes(), Bytes.toBytes(kk.substring(0,2)));
                        i++;
						continue;
                    }
                    if(i==2){
					
                        i++;
                        continue;
                    }
                    if(i==3){
					
                        p.addColumn(family3.getBytes(), "code".getBytes(), Bytes.toBytes(kk));
                        i++;
						continue;
                    }
                    if(i==4){
					
                        p.addColumn(family4.getBytes(), "pop".getBytes(), Bytes.toBytes(kk));
                        i++;
						continue;
                    }
                    if(i==5){
					
                        p.addColumn(family1.getBytes(), "x".getBytes(), Bytes.toBytes(kk));
                        i++;
						continue;
                    }
                    if(i==6){
					
                        p.addColumn(family1.getBytes(), "y".getBytes(), Bytes.toBytes(kk));
						
                    i++;
					continue;
                    }
                    kk=st1.nextToken();
                    
                }
				
                // read next line
				table1.put(p);
				System.out.println("line added");
                line = reader.readLine();
				
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            table1.close();
            connection.close();
        }
      
    }
	public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }
	
	
	


 

    public static void main(String[] args) throws IOException {
        HelloHBase admin = new HelloHBase();
        admin.createHbaseTable();
    }
}