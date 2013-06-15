package cn.macthink.hadoop.tdt.batchpass;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.macthink.hadoop.tdt.util.constant.Constants;

public class PreHadoopLDA extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(PreHadoopLDA.class);

	/**
	 * PreHadoopLDAMapper
	 * 
	 * @author Macthink
	 */
	public static class PreHadoopLDAMapper extends Mapper<Text, Text, NullWritable, Text> {

		/**
		 * Mapper函数
		 * 
		 * @param key
		 *            文件名称
		 * @param values
		 *            文件正文
		 * @param context
		 */
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			// GBK转码
			String content = new String(value.getBytes(), 0, value.getLength(), Constants.INPUT_FILE_CHARSET_NAME);
			context.write(NullWritable.get(), new Text(content));
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		// 构造Configuration
		Configuration conf = getConf();
		conf.set(Constants.MAPRED_JOB_TRACKER_KEY, Constants.MAPRED_JOB_TRACKER);
		conf.set(Constants.FS_DEFAULT_NAME_KEY, Constants.FS_DEFAULT_NAME);

		// 获得文件系统及输入输出路径
		FileSystem fileSystem = FileSystem.get(conf);
		String fsDefaultName = conf.get(Constants.FS_DEFAULT_NAME_KEY);
		Path inputPath = new Path(fsDefaultName, "/batch-pass-input");
		Path outputPath = new Path(fsDefaultName, "/hadoop-lda-input");

		// 打印输入信息
		LOG.info(inputPath);
		LOG.info(outputPath);

		// 如果输出文件存在，则移除
		if (fileSystem.exists(outputPath)) {
			fileSystem.delete(outputPath, true);
		}

		// 配置Job
		Job job = new Job(conf, PreHadoopLDA.class.getSimpleName());
		job.setJarByClass(PreHadoopLDA.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapperClass(PreHadoopLDAMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int code = ToolRunner.run(new Configuration(), new PreHadoopLDA(), args);
		System.exit(code);
	}

}
