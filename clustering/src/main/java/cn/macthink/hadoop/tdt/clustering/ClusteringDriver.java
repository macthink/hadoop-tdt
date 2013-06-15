/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-6-6
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.clustering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import cn.macthink.hadoop.tdt.clustering.agenes.preprocessing.GenerateInitClustersDriver;
import cn.macthink.hadoop.tdt.clustering.agenes.step1.PartitionGenerateClustersDistanceDriver;
import cn.macthink.hadoop.tdt.vectorizer.DocumentVectorizationDriver;
import cn.macthink.hadoop.tdt.vectorizer.termlist.GenerateTermListDriver;
import cn.macthink.hadoop.tdt.vectorizer.tfidf.DocumentTFIDFDriver;

/**
 * ClusteringDriver
 * 
 * @author Macthink
 */
public class ClusteringDriver {

	/**
	 * Main
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int code = ToolRunner.run(new Configuration(), new GenerateTermListDriver(), args);
		if (code != 0) {
			System.exit(code);
		}
		code = ToolRunner.run(new Configuration(), new DocumentTFIDFDriver(), args);
		if (code != 0) {
			System.exit(code);
		}
		code = ToolRunner.run(new Configuration(), new DocumentVectorizationDriver(), args);
		if (code != 0) {
			System.exit(code);
		}
		code = ToolRunner.run(new Configuration(), new GenerateInitClustersDriver(), args);
		if (code != 0) {
			System.exit(code);
		}
		code = ToolRunner.run(new Configuration(), new PartitionGenerateClustersDistanceDriver(), args);
		System.exit(code);
	}

}
