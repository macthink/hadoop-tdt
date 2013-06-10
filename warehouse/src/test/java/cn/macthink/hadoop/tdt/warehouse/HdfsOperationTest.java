/**
 * Project:hadoop-tdt-warehouse
 * File Created at 2013-5-1
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.warehouse;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * HdfsOperationTest
 * 
 * @author Macthink
 */
public class HdfsOperationTest {

	/**
	 * Test method for
	 * {@link cn.macthink.hadoop.tdt.warehouse.HdfsOperation#list(org.apache.hadoop.conf.Configuration, java.lang.String)}
	 * .
	 */
	@Test
	public final void testList() {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://192.168.2.10:9000");
		HdfsOperation.list(conf, "/");
	}

	/**
	 * Test method for
	 * {@link cn.macthink.hadoop.tdt.warehouse.HdfsOperation#get(org.apache.hadoop.conf.Configuration, java.lang.String, java.lang.String, boolean)}
	 * .
	 */
	@Test
	public final void testGet() {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://192.168.2.10:9000");
		String hdfsFilePathString = "/inputFile";
		String localFilePathString = "D:/inputFile";
		HdfsOperation.get(conf, hdfsFilePathString, localFilePathString, false);
	}

	/**
	 * Test method for
	 * {@link cn.macthink.hadoop.tdt.warehouse.HdfsOperation#put(org.apache.hadoop.conf.Configuration, java.io.File, java.lang.String, boolean, boolean)}
	 * .
	 */
	@Test
	public final void testPut() {
		File inputFile = new File("D:/inputFile");
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://192.168.2.10:9000");
		HdfsOperation.put(conf, inputFile, "hdfs://192.168.2.10:9000/", false, true);
	}

}
