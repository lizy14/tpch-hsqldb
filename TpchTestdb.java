package org.hsqldb.sample;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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

    	executeFile("tpch/create-table-" + table_type + ".sql");
    	executeFile("tpch/add-key.sql");
    	
        if(table_type == "text"){
        	executeFile("tpch/set-source.sql");
        }
        
        executeFile("tpch/data.sql");

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
    public double power(FileWriter fw) throws SQLException, IOException{
    	
    	
    	double sum = 0;
    	Timer t;

    	t = new Timer();
    	execute_refresh_function_1();
    	long subsum = t.end();
    	System.out.println(subsum); // milliseconds
    	sum += Math.log(subsum); //TODO: log of zero
    	
    	//execute_query_set
    	// CHANGED: skip q7, q20
    	BufferedReader br = new BufferedReader(new FileReader("tpch/test-skipping.sql"));  
    	String line = null;
    	Statement st = conn.createStatement();
    	while ((line = br.readLine()) != null)  {
    		subsum = 0;
    		
    		for(String subline : line.split(";")){
    			
    			t = new Timer();
    			ResultSet result = st.executeQuery(subline);
    			subsum += t.end();
    			
    			if(fw != null){
    				dump(result, fw);
    			}
    		}
    		
    		System.out.println(subsum); // milliseconds
    		sum += Math.log(subsum);
    		
    	} 
    	br.close();
    	
    	t = new Timer();
    	execute_refresh_function_2();
    	subsum = t.end();
    	System.out.println(subsum); // milliseconds
    	sum += Math.log(subsum); //TODO: log of zero
    	
    	return Math.exp(- sum / 22.); // CHANGED: skip q7, q20 
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
//    		System.out.print(time);
//    		System.out.println(" milliseconds");
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
        	FileWriter fw = new FileWriter("tpch-test-"+ table_type + "-output.txt", false);
        	System.out.println(db.power(fw)); 
        	
        	// CHANGED: skip Throughput test
//        	System.out.println("Throughput");
//        	System.out.println(db.throughput());
        	
        	System.out.println("Done");
            db.shutdown();
            
        } catch (SQLException e) {
            e.printStackTrace();
        };
    }

    public static void main(String[] args) throws IOException {

    	test("cached");
    	// CHANGED: cached table only
//    	test("text");
//    	test("memory");
    	 
    	
    }


	// the following function is borrowed from src.org.hsqldb.sample.Testdb

/* Copyright (c) 2001-2011, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


/**
 * Title:        Testdb
 * Description:  simple hello world db example of a
 *               standalone persistent db application
 *
 *               every time it runs it adds four more rows to sample_table
 *               it does a query and prints the results to standard out
 *
 * Author: Karl Meissner karl@meissnersd.com
 */

    public static void dump(ResultSet rs, FileWriter fw) throws SQLException {
    	
        ResultSetMetaData meta   = rs.getMetaData();
        int               colmax = meta.getColumnCount();
        int               i;
        Object            o = null;

        for (; rs.next(); ) {
            for (i = 0; i < colmax; ++i) {
                o = rs.getObject(i + 1);
                fw.write(o.toString() + " ");
            }
            fw.write(" \n");
        }
        
        fw.write("\n");
    }     


} // class TpchTestdb

