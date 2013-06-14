package cn.macthink.hadoop.tdt.entity.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Writable;

import cn.macthink.hadoop.tdt.entity.Cluster;
import cn.macthink.hadoop.tdt.entity.Vector;
import cn.macthink.hadoop.tdt.util.StatisticUtils;

public class ClusterWritable implements Writable {

	/**
	 * Cluster
	 */
	private Cluster cluster;

	/**
	 * 构造函数
	 */
	public ClusterWritable() {
		super();
	}

	/**
	 * 构造函数
	 * 
	 * @param cluster
	 */
	public ClusterWritable(Cluster cluster) {
		super();
		this.cluster = cluster;
	}

	/**
	 * FIXME 获得将被划分到的分区编号
	 * 
	 * @param processorNum
	 * @return
	 */
	public int getPartitionNum(int processorNum) {
		// 获得类别的均值质心
		Vector meanCentroid = cluster.getMeanCentroid();

		// 分块
		int blockNum = processorNum; // 块数
		int dimensionOfBlock = meanCentroid.getSize() / processorNum; // 每个块中的维数，最后一块可能比较小
		double[][] blocks = new double[blockNum][dimensionOfBlock];
		int pointer = 0;
		for (int i = 0; i < blockNum; i++) {
			blocks[i] = Arrays.copyOfRange(meanCentroid.getEigenvalues(), pointer, pointer + dimensionOfBlock);
			pointer += dimensionOfBlock;
		}

		// 统计各块的非零元素和方差
		int nonZeroNums = 0;
		double variances = 0;
		int partitionNum = 1; // 将被划分到的分区编号
		double fitness = 0.0d;
		double maxFitness = 0.0d; // 最大适应度
		for (int i = 0; i < blocks.length; i++) {
			double[] block = blocks[i];
			// 统计非零元素
			nonZeroNums = 0;
			for (int j = 0; j < block.length; j++) {
				if (block[j] != 0) {
					nonZeroNums++;
				}
			}
			// 统计方差
			variances = StatisticUtils.getVariance(block);
			// 计算适应度并比较
			fitness = nonZeroNums / variances;
			if (fitness > maxFitness) {
				maxFitness = fitness;
				partitionNum = i + 1;
			}
		}

		return partitionNum;
	}

	/**
	 * @return the cluster
	 */
	public Cluster get() {
		return cluster;
	}

	/**
	 * @param cluster
	 *            the cluster to set
	 */
	public void set(Cluster cluster) {
		this.cluster = cluster;
	}

	/**
	 * 序列化写方法
	 * 
	 * @param out
	 * @param cluster
	 * @throws IOException
	 */
	public static void writeCluster(DataOutput out, Cluster cluster) throws IOException {
		out.writeUTF(cluster.getId());
		out.writeInt(cluster.getSize());
		out.writeInt(cluster.getVectorNum());
		for (Vector vector : cluster.getVectors()) {
			VectorWritable.writeVector(out, vector);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		writeCluster(out, this.cluster);
	}

	/**
	 * 序列化读方法
	 * 
	 * @param in
	 * @return
	 * @throws IOException
	 */
	public static Cluster readCluster(DataInput in) throws IOException {
		Cluster cluster = new Cluster();
		cluster.setId(in.readUTF());
		cluster.setSize(in.readInt());
		cluster.setVectorNum(in.readInt());
		List<Vector> vectors = new ArrayList<Vector>();
		for (int i = 0; i < cluster.getVectorNum(); i++) {
			Vector vector = VectorWritable.readVector(in);
			vectors.add(vector);
		}
		cluster.setVectors(vectors);
		return cluster;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.cluster = readCluster(in);
	}

	@Override
	public int hashCode() {
		return cluster.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof ClusterWritable && cluster.equals(((ClusterWritable) obj).get());
	}

	@Override
	public String toString() {
		return cluster.toString();
	}

}
