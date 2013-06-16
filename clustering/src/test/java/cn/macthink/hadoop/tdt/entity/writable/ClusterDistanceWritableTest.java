/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-6-16
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.entity.writable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import cn.macthink.hadoop.tdt.entity.Cluster;
import cn.macthink.hadoop.tdt.entity.ClusterDistance;
import cn.macthink.hadoop.tdt.entity.Vector;

/**
 * ClusterDistanceWritableTest
 * 
 * @author Macthink
 */
public class ClusterDistanceWritableTest {

	public static byte[] serialize(Writable writable) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);
		writable.write(dataOut);
		dataOut.close();
		return out.toByteArray();
	}

	public static byte[] deserialize(Writable writable, byte[] bytes) throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream dataIn = new DataInputStream(in);
		writable.readFields(dataIn);
		dataIn.close();
		return bytes;
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		double[] eigenvalues = { 1, 2, 3, 4, 5, 2, 3, 4, 5, 2, 3, 4, 5, 2, 3, 4, 5, 2, 3, 4, 5, 2, 3, 4, 5 };
		Vector vector = new Vector(eigenvalues);
		Cluster sourceCluster = new Cluster(vector);
		Cluster targetCluster = new Cluster(sourceCluster);
		ClusterDistance clusterDistance = new ClusterDistance(sourceCluster, targetCluster, 0.5d);
		ClusterDistanceWritable clusterDistanceWritable = new ClusterDistanceWritable(clusterDistance);
		byte[] bytes = serialize(clusterDistanceWritable);
		System.out.println(bytes.length);

		System.out.println("=================");

		ClusterDistanceWritable newClusterDistanceWritable = new ClusterDistanceWritable();
		deserialize(newClusterDistanceWritable, bytes);
		System.out.println(clusterDistanceWritable);
	}
}
