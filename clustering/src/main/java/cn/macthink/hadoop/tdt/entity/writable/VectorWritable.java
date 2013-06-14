/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-12
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.entity.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import cn.macthink.hadoop.tdt.entity.Vector;

/**
 * VectorWritable
 * 
 * @author Macthink
 */
public class VectorWritable implements Writable {

	/**
	 * Vector
	 */
	private Vector vector;

	/**
	 * 构造函数
	 */
	public VectorWritable() {
		super();
	}

	/**
	 * 构造函数
	 * 
	 * @param vector
	 */
	public VectorWritable(Vector vector) {
		super();
		this.vector = vector;
	}

	/**
	 * @return the vector
	 */
	public Vector get() {
		return vector;
	}

	/**
	 * @param vector
	 *            the vector to set
	 */
	public void set(Vector vector) {
		this.vector = vector;
	}

	/**
	 * 序列化写方法
	 * 
	 * @param out
	 * @param vector
	 * @throws IOException
	 */
	public static void writeVector(DataOutput out, Vector vector) throws IOException {
		out.writeUTF(vector.getId());
		out.writeUTF(vector.getClusterId());
		out.writeInt(vector.getSize());
		out.writeDouble(vector.getLengthSquared());
		for (double eigenvalue : vector.getEigenvalues()) {
			out.writeDouble(eigenvalue);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		writeVector(out, this.vector);
	}

	/**
	 * 序列化读方法
	 * 
	 * @param in
	 * @return
	 * @throws IOException
	 */
	public static Vector readVector(DataInput in) throws IOException {
		Vector vector = new Vector();
		vector.setId(in.readUTF());
		vector.setClusterId(in.readUTF());
		vector.setSize(in.readInt());
		vector.setLengthSquared(in.readDouble());
		int size = vector.getSize();
		double[] eigenvalues = new double[size];
		for (int i = 0; i < size; i++) {
			eigenvalues[i] = in.readDouble();
		}
		vector.setEigenvalues(eigenvalues);
		return vector;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.vector = readVector(in);
	}

	@Override
	public int hashCode() {
		return vector.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof VectorWritable && vector.equals(((VectorWritable) obj).get());
	}

	@Override
	public String toString() {
		return vector.toString();
	}
}
