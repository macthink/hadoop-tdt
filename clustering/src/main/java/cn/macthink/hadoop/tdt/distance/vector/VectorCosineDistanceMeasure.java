/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-5-13
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.hadoop.tdt.distance.vector;

import cn.macthink.hadoop.tdt.entity.Vector;

import com.google.common.base.Preconditions;

/**
 * CosineDistanceMeasure
 * 
 * @author Macthink
 */
public class VectorCosineDistanceMeasure implements VectorDistanceMeasure {

	@Override
	public double distance(Vector samplePoint1, Vector samplePoint2) {
		Preconditions.checkArgument(samplePoint1.getSize() == samplePoint2.getSize());

		// 计算分子：点积
		double numerator = samplePoint1.getDotProduct(samplePoint2);

		// 计算分母：乘积
		double lengthSquared1 = samplePoint1.getLengthSquared();
		double lengthSquared2 = samplePoint2.getLengthSquared();
		double denominator = Math.sqrt(lengthSquared1 * lengthSquared2);

		// 纠正小数点舍入误差
		if (denominator < numerator) {
			denominator = numerator;
		}

		// 纠正零向量的情况
		if (denominator == 0) {
			return 0;
		}

		// 计算相似度
		double costheta = numerator / denominator;

		// 计算距离
		double distance = Math.acos(costheta) * 2 / Math.PI;

		return distance;
	}
}
