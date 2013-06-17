/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-12
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.entity;

import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Cluster：簇
 * 
 * @author Macthink
 */
public class Cluster {

	/**
	 * 标识
	 */
	private String id;

	/**
	 * 特征向量维数
	 */
	private int size;

	/**
	 * 类别中的向量数目
	 */
	private int vectorNum = 0;

	/**
	 * 类别中的向量集合
	 */
	private List<Vector> vectors;

	/**
	 * 构造函数
	 */
	public Cluster() {
		super();
		this.id = UUID.randomUUID().toString();
	}

	/**
	 * 构造函数
	 * 
	 * @param vector
	 */
	public Cluster(Vector vector) {
		super();
		this.id = UUID.randomUUID().toString();
		this.size = vector.getSize();
		this.vectors = Lists.newArrayList();
		vector.setClusterId(this.id);
		this.vectors.add(vector);
		this.vectorNum = 1;
	}

	/**
	 * 构造函数
	 * 
	 * @param other
	 */
	public Cluster(Cluster other) {
		super();
		this.id = UUID.randomUUID().toString();
		this.vectors = other.vectors;
		this.size = other.size;
		this.vectorNum = other.vectorNum;
	}

	/**
	 * 根据SamplePoint[]获得Category[]
	 * 
	 * @param samplePoints
	 * @return
	 */
	public static Cluster[] getCategories(Vector[] samplePoints) {
		Preconditions.checkNotNull(samplePoints);

		Cluster[] categories = new Cluster[samplePoints.length];
		for (int i = 0; i < samplePoints.length; i++) {
			categories[i] = new Cluster(samplePoints[i]);
		}
		return categories;
	}

	/**
	 * 添加样本点
	 * 
	 * @param vector
	 */
	public void addSamplePoint(Vector vector) {
		Preconditions.checkNotNull(vector);
		Preconditions.checkArgument(vector.getSize() == size);

		if (this.vectors == null) {
			this.vectors = Lists.newArrayList();
		}
		vector.setClusterId(this.id);
		this.vectors.add(vector);
		this.vectorNum = this.vectors.size();
	}

	/**
	 * 将另一类别的样本点合并到当前类别
	 * 
	 * @param other
	 */
	public void combineSamplePoints(Cluster other) {
		Preconditions.checkNotNull(other);

		if (other.getVectors() != null) {
			if (this.vectors == null) {
				this.vectors = Lists.newArrayList();
			}
			for (Vector vector : other.getVectors()) {
				vector.setClusterId(this.id);
				this.vectors.add(vector);
			}
			this.vectorNum = this.vectors.size();
		}
	}

	/**
	 * 获得类别的均值质心（各维的均值特征向量）
	 * 
	 * @return
	 */
	public Vector getMeanCentroid() {
		Preconditions.checkNotNull(vectors);

		if (this.vectors.size() == 1) {
			return this.vectors.get(0);
		}
		int samplePointNum = vectors.size();
		double dimensionSum = 0.0d;
		double[] meanEigenvalues = new double[size];

		for (int i = 0; i < size; i++) {
			dimensionSum = 0.0d;
			for (Vector samplePoint : vectors) {
				dimensionSum += samplePoint.getEigenvalues()[i];
			}
			meanEigenvalues[i] = dimensionSum / samplePointNum;
		}

		return new Vector(meanEigenvalues);
	}

	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @return the size
	 */
	public int getSize() {
		return size;
	}

	/**
	 * @return the vectorNum
	 */
	public int getVectorNum() {
		return vectorNum;
	}

	/**
	 * @return the vectors
	 */
	public List<Vector> getVectors() {
		return vectors;
	}

	/**
	 * @param id
	 *            the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @param size
	 *            the size to set
	 */
	public void setSize(int size) {
		this.size = size;
	}

	/**
	 * @param vectorNum
	 *            the vectorNum to set
	 */
	public void setVectorNum(int vectorNum) {
		this.vectorNum = vectorNum;
	}

	/**
	 * @param vectors
	 *            the vectors to set
	 */
	public void setVectors(List<Vector> vectors) {
		this.vectors = vectors;
		if (vectors != null) {
			this.vectorNum = vectors.size();
			this.size = vectors.get(0).getSize();
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + size;
		result = prime * result + ((vectors == null) ? 0 : vectors.hashCode());
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
		Cluster other = (Cluster) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (size != other.size)
			return false;
		if (vectors == null) {
			if (other.vectors != null)
				return false;
		} else if (!vectors.equals(other.vectors))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}

}
