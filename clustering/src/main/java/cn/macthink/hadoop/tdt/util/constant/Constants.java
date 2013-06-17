/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-2
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.util.constant;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Clustering Constants
 * 
 * @author Macthink
 */
public class Constants {

	private static final Log LOG = LogFactory.getLog(Constants.class);
	private static final String propertiesFile = "constants.properties";
	private static Configuration configuration = new PropertiesConfiguration();
	static {
		try {
			configuration = new PropertiesConfiguration(propertiesFile);
		} catch (Exception e) {
			LOG.error("error to load properties file \"" + propertiesFile + "\"");
		}
	}

	/**
	 * common
	 */
	public static String INPUT_FILE_CHARSET_NAME = configuration.getString("input.file.charset.name");
	public static final String MAPRED_JOB_TRACKER_KEY = "mapred.job.tracker";
	public static String MAPRED_JOB_TRACKER = configuration.getString(MAPRED_JOB_TRACKER_KEY);
	public static final String FS_DEFAULT_NAME_KEY = "fs.default.name";
	public static String FS_DEFAULT_NAME = configuration.getString(FS_DEFAULT_NAME_KEY);

	/**
	 * cn.macthink.hadoop.tdt.vectorizer.termlist
	 */
	public static String TERM_LIST_INPUT_PATH = configuration.getString("term.list.input.path");
	public static String TERM_LIST_OUTPUT_PATH = configuration.getString("term.list.output.path");
	public static int TERM_LIST_MIN_DF = configuration.getInt("term.list.min.df", 1);
	public static int TERM_LIST_MAX_DF = configuration.getInt("term.list.max.df");

	/**
	 * cn.macthink.hadoop.tdt.vectorizer.tfidf
	 */
	public static String TERM_LIST_PATH = configuration.getString("term.list.path");
	public static String DOCUMENT_TFIDF_INPUT_PATH = configuration.getString("document.tfidf.input.path");
	public static String DOCUMENT_TFIDF_OUTPUT_PATH = configuration.getString("document.tfidf.output.path");
	/**
	 * cn.macthink.hadoop.tdt.vectorizer
	 */
	public static String DOCUMENT_VECTORIZER_INPUT_PATH = configuration.getString("document.vectorizer.input.path");
	public static String DOCUMENT_VECTORIZER_OUTPUT_PATH = configuration.getString("document.vectorizer.output.path");

	/**
	 * cn.macthink.hadoop.tdt.clustering.preprocessing
	 */
	public static String GENERATE_INIT_CLUSTERS_INPUT_PATH = configuration
			.getString("generate.init.clusters.input.path");
	public static String GENERATE_INIT_CLUSTERS_OUTPUT_PATH = configuration
			.getString("generate.init.clusters.output.path");

	/**
	 * cn.macthink.hadoop.tdt.clustering.agenes.step1
	 */
	public static final String CLUSTERING_AGENES_PROCESSOR_NUM_KEY = "cn.macthink.hadoop.tdt.clustering.agenes.processorNum";
	public static final String CLUSTER_DISTANCE_MEASURE_KEY = "cn.macthink.hadoop.tdt.distance.cluster.ClusterDistanceMeasure";
	public static final String VECTOR_DISTANCE_MEASURE_KEY = "cn.macthink.hadoop.tdt.distance.vector.VectorDistanceMeasure";
	public static final String CLUSTER_MAX_DISTANCE_KEY = "cn.macthink.hadoop.tdt.clustering.agenes.maxClusterDistance";

	public static int CLUSTERING_AGENES_PROCESSOR_NUM = configuration.getInt(CLUSTERING_AGENES_PROCESSOR_NUM_KEY);
	public static String CLUSTER_DISTANCE_MEASURE = configuration.getString(CLUSTER_DISTANCE_MEASURE_KEY);
	public static String VECTOR_DISTANCE_MEASURE = configuration.getString(VECTOR_DISTANCE_MEASURE_KEY);
	public static String CLUSTER_MAX_DISTANCE = configuration.getString(CLUSTER_MAX_DISTANCE_KEY);

	public static String PARTITION_GENERATE_CLUSTERS_DISTANCE_INPUT_PATH = configuration
			.getString("partition.generate.clusters.distance.input.path");
	public static String PARTITION_GENERATE_CLUSTERS_DISTANCE_OUTPUT_PATH = configuration
			.getString("partition.generate.clusters.distance.output.path");

	/**
	 * cn.macthink.hadoop.tdt.clustering.agenes.step2
	 */
	public static final String DISTANCE_THRESHOLD_KEY = "cn.macthink.hadoop.tdt.clustering.agenes.distanceThreshold";
	public static String MERGE_CLUSTERS_INPUT_PATH = configuration.getString("merge.clusters.input.path");
	public static String MERGE_CLUSTERS_OUTPUT_PATH = configuration.getString("merge.clusters.output.path");

	public static String DISTANCE_THRESHOLD = configuration.getString(DISTANCE_THRESHOLD_KEY);

}
