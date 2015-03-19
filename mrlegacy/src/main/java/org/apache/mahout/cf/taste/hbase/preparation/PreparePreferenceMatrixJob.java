/**
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
 * 
 * 
 */

package org.apache.mahout.cf.taste.hbase.preparation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.cf.taste.hadoop.ToEntityPrefsMapper;
import org.apache.mahout.cf.taste.hbase.ToItemPrefsMapper;
import org.apache.mahout.cf.taste.hbase.item.ItemIDIndexMapper;
import org.apache.mahout.cf.taste.hbase.item.RecommenderJob;
import org.apache.mahout.cf.taste.hadoop.item.ItemIDIndexReducer;
import org.apache.mahout.cf.taste.hadoop.item.ToUserVectorsReducer;
import org.apache.mahout.cf.taste.hadoop.preparation.ToItemVectorsMapper;
import org.apache.mahout.cf.taste.hadoop.preparation.ToItemVectorsReducer;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;

import java.util.List;
import java.util.Map;

public class PreparePreferenceMatrixJob extends AbstractJob {

  public static final String NUM_USERS = "numUsers.bin";
  public static final String ITEMID_INDEX = "itemIDIndex";
  public static final String USER_VECTORS = "userVectors";
  public static final String RATING_MATRIX = "ratingMatrix";

  private static final int DEFAULT_MIN_PREFS_PER_USER = 1;

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PreparePreferenceMatrixJob(), args);
  }

  @Override
  public int run(String[] args) throws Exception {

    addInputOption();
    addOutputOption();
    addOption("minPrefsPerUser", "mp", "ignore users with less preferences than this "
            + "(default: " + DEFAULT_MIN_PREFS_PER_USER + ')', String.valueOf(DEFAULT_MIN_PREFS_PER_USER));
    addOption("booleanData", "b", "Treat input as without pref values", Boolean.FALSE.toString());
    addOption("ratingShift", "rs", "shift ratings by this value", "0.0");

    Map<String, List<String>> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }

    int minPrefsPerUser = Integer.parseInt(getOption("minPrefsPerUser"));
    boolean booleanData = Boolean.valueOf(getOption("booleanData"));
    float ratingShift = Float.parseFloat(getOption("ratingShift"));
    String workingTable = getConf().get(RecommenderJob.PARAM_WORKING_TABLE);
    
    //convert items to an internal index
    Configuration mapred_config = HBaseConfiguration.create();
    mapred_config.set("hbase.zookeeper.quorum", "nodo1"); 
    mapred_config.set("hbase.zookeeper.property.clientPort", "2181");
    mapred_config.setBoolean("mapred.compress.map.output", true);
    
	Job itemIDIndex = Job.getInstance(mapred_config);
    itemIDIndex.setJobName(HadoopUtil.getCustomJobName(getClass().getSimpleName(), itemIDIndex, ItemIDIndexMapper.class, ItemIDIndexReducer.class));
    itemIDIndex.setJarByClass(ItemIDIndexMapper.class);     // class that contains mapper and reducer
    
    Scan scan = new Scan();
    scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
    scan.setCacheBlocks(false);  // don't set to true for MR jobs
    // set other scan attrs
    
    TableMapReduceUtil.initTableMapperJob(
    		workingTable,        // input table
        	scan,               		// Scan instance to control CF and attribute selection
        	ItemIDIndexMapper.class,    // mapper class
        	VarIntWritable.class,       // mapper output key
        	VarLongWritable.class,  	// mapper output value
        	itemIDIndex);
    
    itemIDIndex.setReducerClass(ItemIDIndexReducer.class);    // reducer class
    
    itemIDIndex.setOutputKeyClass(VarIntWritable.class);
    itemIDIndex.setOutputValueClass(VarLongWritable.class);
    itemIDIndex.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    FileOutputFormat.setOutputPath(itemIDIndex, getOutputPath(ITEMID_INDEX));  // adjust directories as required

    if (!itemIDIndex.waitForCompletion(true)) return -1;
    //////////////////////////////////////////////////////////////////////////
    
    //convert user preferences into a vector per user
    mapred_config.setBoolean(RecommenderJob.BOOLEAN_DATA, booleanData);
    mapred_config.setInt(ToUserVectorsReducer.MIN_PREFERENCES_PER_USER, minPrefsPerUser);
    mapred_config.set(ToEntityPrefsMapper.RATING_SHIFT, String.valueOf(ratingShift));
    
	Job toUserVectors_hb = Job.getInstance(mapred_config);
	toUserVectors_hb.setJobName(HadoopUtil.getCustomJobName(getClass().getSimpleName(), toUserVectors_hb, ToItemPrefsMapper.class, ToUserVectorsReducer.class));
    toUserVectors_hb.setJarByClass(ToItemPrefsMapper.class);     // class that contains mapper and reducer
    
    TableMapReduceUtil.initTableMapperJob(
    		workingTable,        // input table
        	scan,               		// Scan instance to control CF and attribute selection
        	ToItemPrefsMapper.class,    // mapper class
        	VarLongWritable.class,       // mapper output key
        	booleanData ? VarLongWritable.class : EntityPrefWritable.class,  	// mapper output value
        	toUserVectors_hb);
    
    toUserVectors_hb.setReducerClass(ToUserVectorsReducer.class);    // reducer class
    toUserVectors_hb.setNumReduceTasks(1);    // at least one, adjust as required
    
    toUserVectors_hb.setOutputKeyClass(VarLongWritable.class);
    toUserVectors_hb.setOutputValueClass(VectorWritable.class);
    toUserVectors_hb.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    FileOutputFormat.setOutputPath(toUserVectors_hb, getOutputPath(USER_VECTORS));  // adjust directories as required

    if (!toUserVectors_hb.waitForCompletion(true)) return -1;
    //////////////////////////////////////////////////////////////////////////
    
    //we need the number of users later
    int numberOfUsers = (int) toUserVectors_hb.getCounters().findCounter(ToUserVectorsReducer.Counters.USERS).getValue();
    HadoopUtil.writeInt(numberOfUsers, getOutputPath(NUM_USERS), getConf());
    //build the rating matrix
    Job toItemVectors = prepareJob(getOutputPath(USER_VECTORS), getOutputPath(RATING_MATRIX),
            ToItemVectorsMapper.class, IntWritable.class, VectorWritable.class, ToItemVectorsReducer.class,
            IntWritable.class, VectorWritable.class);
    toItemVectors.setCombinerClass(ToItemVectorsReducer.class);

    if (!toItemVectors.waitForCompletion(true)) return -1;

    return 0;
  }
}
