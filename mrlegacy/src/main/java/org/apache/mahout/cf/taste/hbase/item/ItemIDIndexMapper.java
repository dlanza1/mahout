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

package org.apache.mahout.cf.taste.hbase.item;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;

public final class ItemIDIndexMapper extends
		TableMapper<VarIntWritable, VarLongWritable> {

	private final VarIntWritable indexWritable = new VarIntWritable();
	private final VarLongWritable itemIDWritable = new VarLongWritable();
	
	private byte[] fcRatings;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		
		fcRatings = Bytes.toBytes(conf.get(RecommenderJob.PARAM_CF_RATINGS));
	}

	@Override
	protected void map(ImmutableBytesWritable rowKey, Result columns, Context context)
			throws IOException, InterruptedException {
		
		NavigableMap<byte[], byte[]> colum_map = columns.getFamilyMap(fcRatings);
		for(byte[] colum: colum_map.keySet()){
			long itemID = Long.valueOf(Bytes.toString(colum));
			int index = TasteHadoopUtils.idToIndex(itemID);
			indexWritable.set(index);
			itemIDWritable.set(itemID);

			context.write(indexWritable, itemIDWritable);
		}
	}

}
