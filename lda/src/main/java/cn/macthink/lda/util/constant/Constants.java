/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-2
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.lda.util.constant;

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

}
