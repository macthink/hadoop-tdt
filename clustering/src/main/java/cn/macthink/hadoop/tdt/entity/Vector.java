/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-12
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.entity;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import cn.macthink.hadoop.tdt.distance.vector.VectorDistanceMeasure;

import com.google.common.base.Preconditions;

/**
 * Vector
 * 
 * @author Macthink
 */
public class Vector {

	/**
	 * 标识（如果不赋值，则使用UUID）
	 */
	private String id;

	/**
	 * 类别标识（如果不赋值，则使用UUID）
	 */
	private String clusterId;

	/**
	 * 特征向量
	 */
	private double[] eigenvalues;

	/**
	 * 特征向量维数
	 */
	private int size;

	/**
	 * 特征向量长度的平方（特征向量长度定义为特征向量所有元素的平方的总和的开方）
	 */
	private double lengthSquared = -1.0;

	/**
	 * 构造函数：只用于序列化
	 */
	public Vector() {
		super();
	}

	/**
	 * 构造函数
	 * 
	 * @param id
	 * @param clusterId
	 */
	public Vector(String id, String clusterId) {
		super();
		this.id = id;
		this.clusterId = clusterId;
	}

	/**
	 * 构造函数
	 * 
	 * @param eigenvalues
	 */
	public Vector(double[] eigenvalues) {
		this(UUID.randomUUID().toString(), UUID.randomUUID().toString(), eigenvalues);
	}

	/**
	 * 构造函数
	 * 
	 * @param id
	 * @param eigenvalues
	 */
	public Vector(String id, double[] eigenvalues) {
		this(id, UUID.randomUUID().toString(), eigenvalues);
	}

	/**
	 * 构造函数
	 * 
	 * @param eigenvalueList
	 */
	public Vector(List<Double> eigenvalueList) {
		this(UUID.randomUUID().toString(), eigenvalueList);
	}

	/**
	 * 构造函数
	 * 
	 * @param id
	 * @param clusterId
	 * @param eigenvalues
	 */
	public Vector(String id, String clusterId, double[] eigenvalues) {
		super();
		this.id = id;
		this.clusterId = clusterId;
		this.size = eigenvalues.length;
		this.eigenvalues = eigenvalues;
	}

	/**
	 * 构造函数
	 * 
	 * @param id
	 * @param eigenvalueList
	 */
	public Vector(String id, List<Double> eigenvalueList) {
		this(id, UUID.randomUUID().toString());
		double[] eigenvalues = new double[eigenvalueList.size()];
		for (int i = 0; i < eigenvalueList.size(); i++) {
			Double d = eigenvalueList.get(i);
			eigenvalues[i] = d.doubleValue();
		}
		this.size = eigenvalues.length;
		this.eigenvalues = eigenvalues;
	}

	/**
	 * 计算特征向量的所有元素的平方的总和（平方根就是特征向量的长度）。
	 * 
	 * @return
	 */
	public double getLengthSquared() {
		if (lengthSquared >= 0.0) {
			return lengthSquared;
		}
		double result = 0.0;
		for (double value : eigenvalues) {
			result += value * value;
		}
		lengthSquared = result;
		return result;
	}

	/**
	 * 计算与给定样本点的特征向量的点积
	 * 
	 * @param other
	 * @return
	 */
	public double getDotProduct(Vector other) {
		Preconditions.checkNotNull(eigenvalues);
		Preconditions.checkNotNull(other.getEigenvalues());
		Preconditions.checkArgument(size == other.size);

		double result = 0.0d;
		int index = 0;
		while (index < size && eigenvalues[index] != 0.0 && other.eigenvalues[index] != 0.0) {
			result += eigenvalues[index] * other.eigenvalues[index];
			index++;
		}

		return result;
	}

	/**
	 * 计算和另一向量间的距离的平方
	 * 
	 * @param other
	 * @return
	 */
	public double getDistanceSquared(Vector other) {
		Preconditions.checkNotNull(eigenvalues);
		Preconditions.checkNotNull(other.getEigenvalues());
		Preconditions.checkArgument(size == other.size);

		double result = 0.0d;
		int index = 0;
		double difference = 0.0d;
		while (index < size && eigenvalues[index] != 0.0 && other.eigenvalues[index] != 0.0) {
			difference = eigenvalues[index] - other.eigenvalues[index];
			result += difference * difference;
			index++;
		}

		return Math.abs(result);
	}

	/**
	 * 计算与另一个类别的距离
	 * 
	 * @param cluster
	 * @param distanceMeasure
	 * @return
	 */
	public double getClusterDistance(Cluster cluster, VectorDistanceMeasure distanceMeasure) {
		Preconditions.checkNotNull(cluster);
		Preconditions.checkNotNull(cluster.getVectors());

		Collection<Vector> samplePoints = cluster.getVectors();
		double result = 0.0d;
		for (Vector samplePoint : samplePoints) {
			result += distanceMeasure.distance(this, samplePoint);
		}

		return result / samplePoints.size();
	}

	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @return the clusterId
	 */
	public String getClusterId() {
		return clusterId;
	}

	/**
	 * @return the eigenvalues
	 */
	public double[] getEigenvalues() {
		return eigenvalues;
	}

	/**
	 * @return the size
	 */
	public int getSize() {
		return size;
	}

	/**
	 * @param id
	 *            the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @param clusterId
	 *            the clusterId to set
	 */
	public void setClusterId(String clusterId) {
		this.clusterId = clusterId;
	}

	/**
	 * @param size
	 *            the size to set
	 */
	public void setSize(int size) {
		this.size = size;
	}

	/**
	 * @param lengthSquared
	 *            the lengthSquared to set
	 */
	public void setLengthSquared(double lengthSquared) {
		this.lengthSquared = lengthSquared;
	}

	/**
	 * @param eigenvalues
	 *            the eigenvalues to set
	 */
	public void setEigenvalues(double[] eigenvalues) {
		this.size = eigenvalues.length;
		this.eigenvalues = eigenvalues;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((clusterId == null) ? 0 : clusterId.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Vector other = (Vector) obj;
		if (clusterId == null) {
			if (other.clusterId != null)
				return false;
		} else if (!clusterId.equals(other.clusterId))
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}

}
