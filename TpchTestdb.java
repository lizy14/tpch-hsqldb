
package org.hsqldb.sample;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * Title:        TPC Benchmark H
 * Description:  
 *
 * Author: Zhaoyang Li
 */
public class TpchTestdb {

    Connection conn;

    public TpchTestdb(String db_file_name_prefix) throws Exception {
        Class.forName("org.hsqldb.jdbc.JDBCDriver");
        conn = DriverManager.getConnection("jdbc:hsqldb:" + db_file_name_prefix, "SA", "");
    }

    
    public void execute_load(String table_type) throws SQLException, IOException{
    	Statement st = conn.createStatement();
        st.execute("set autocommit false;");
    	executeFile("tpch/create-table-" + table_type + ".sql");
    	executeFile("tpch/add-key.sql");
    	st.execute("commit;");
    	
        if(table_type == "text"){
        	executeFile("tpch/set-source.sql");
        	st.execute("commit;");
        }
        
        executeFile("tpch/data.sql");
        st.execute("commit;");

        
        
        st.execute("set autocommit true");
    }
    public void execute_refresh_function_1() throws SQLException, IOException{
    	executeFile("tpch/rf1.sql");
    }
    public void execute_refresh_function_2() throws SQLException, IOException{
    	executeFile("tpch/rf2.sql");
    }
    
    public void executeFile(String filename) throws SQLException, IOException {
    	
    	BufferedReader br = new BufferedReader(new FileReader(filename));  
    	String line = null;
    	Statement st = conn.createStatement();
    	
    	while ((line = br.readLine()) != null)  {
    		st.execute(line);
    	} 
    	br.close();
    }
    
    public void shutdown() throws SQLException {
    	Statement st = conn.createStatement();
        st.execute("SHUTDOWN");
        conn.close();
    }
    
    
    // total run time, in milliseconds
    public long load(String table_type) throws SQLException, IOException{
    	Timer t = new Timer();
    	execute_load(table_type);
        return t.end();
    }

    // geometric mean of run time, in milliseconds
    public double power() throws SQLException, IOException{
    	
    	double sum = 0;
    	Timer t;

    	t = new Timer();
    	execute_refresh_function_1();
    	sum += Math.log(t.end()); //TODO: log of zero
    	
    	//execute_query_set
    	BufferedReader br = new BufferedReader(new FileReader("tpch/test.sql"));  
    	String line = null;
    	Statement st = conn.createStatement();
    	while ((line = br.readLine()) != null)  {
    		t = new Timer();
    		for(String subline : line.split(";")){
    			st.executeQuery(subline);
    		}
    		sum += Math.log(t.end());
    	} 
    	br.close();
    	
    	t = new Timer();
    	execute_refresh_function_2();
    	sum += Math.log(t.end());
    	
    	return Math.exp(- sum / 24.);
    }
    
    // TODO
    public double throughput(){
    	return 0;
    }
    
    class Timer{
    	long start;
    	public Timer(){
    		start = System.currentTimeMillis();
    	}
    	public long end(){
    		long time = System.currentTimeMillis() - start;
    		System.out.print(time);
    		System.out.println(" milliseconds");
    		return time;
    	}
    }
    
    public static void test(String table_type) throws IOException{
    	
    	System.out.print(table_type);
    	System.out.println(" table: ");
    	
        TpchTestdb db = null;

        try {
            db = new TpchTestdb("tpch-test-"+ table_type);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        try {
        	System.out.println("Load");
        	System.out.println(db.load(table_type));
        	System.out.println("Power");
        	System.out.println(db.power());
        	System.out.println("Throughput");
        	System.out.println(db.throughput());
        	
        	System.out.println("Done");
            db.shutdown();
            
        } catch (SQLException e) {
        	System.out.println("Try removing exisiting test db.");
            e.printStackTrace();
        };
    }
    public static void main(String[] args) throws IOException {
    	
    	test("memory");
    	test("text");
    	test("cache");
    }
    
} // class TpchTestdb

