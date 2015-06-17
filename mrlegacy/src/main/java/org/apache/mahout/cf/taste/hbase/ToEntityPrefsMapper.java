/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.mahout.cf.taste.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.cf.taste.hbase.item.RecommenderJob;
import org.apache.mahout.math.VarLongWritable;

import java.io.IOException;
import java.util.NavigableMap;

public abstract class ToEntityPrefsMapper extends
		TableMapper<VarLongWritable, VarLongWritable> {

	public static final String TRANSPOSE_USER_ITEM = ToEntityPrefsMapper.class
			+ "transposeUserItem";
	public static final String RATING_SHIFT = ToEntityPrefsMapper.class
			+ "shiftRatings";

	private boolean booleanData;
	private float ratingShift;
	
	private byte[] fcRatings;

	@Override
	protected void setup(Context context) {
		Configuration jobConf = context.getConfiguration();
		booleanData = jobConf.getBoolean(RecommenderJob.BOOLEAN_DATA, false);
		ratingShift = Float.parseFloat(jobConf.get(RATING_SHIFT, "0.0"));
		
		fcRatings = Bytes.toBytes(jobConf.get(RecommenderJob.PARAM_CF_RATINGS));
	}

	@Override
	protected void map(ImmutableBytesWritable key, Result columns, Context context)
			throws IOException, InterruptedException {

		long userID = Long.valueOf(Bytes.toString(key.get()));

		NavigableMap<byte[], byte[]> colum_map = columns.getFamilyMap(fcRatings);
		for(byte[] colum: colum_map.keySet()){
			long itemID = Long.valueOf(Bytes.toString(colum));
			
			if (booleanData) {
				context.write(new VarLongWritable(userID), new VarLongWritable(itemID));
			} else {
				float prefValue;
				
				try{
					byte[] pref = columns.getValue(fcRatings, colum);
					
					prefValue = Float.parseFloat(Bytes.toString(pref)) + ratingShift;
				}catch(Exception ex){
					System.out.println("WARN: the preference valur could not be parsed to float");
					
					prefValue = 1.0f;
				}
				
				context.write(new VarLongWritable(userID), new EntityPrefWritable(itemID, prefValue));
			}
		}
	}

}
