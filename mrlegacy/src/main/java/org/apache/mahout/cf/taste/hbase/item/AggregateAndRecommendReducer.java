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
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.cf.taste.hadoop.item.IDReader;
import org.apache.mahout.cf.taste.hadoop.item.PrefAndSimilarityColumnWritable;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.function.Functions;
import org.apache.mahout.math.map.OpenIntLongHashMap;

import es.unex.silice.smallshi.recommender.hbase.HBaseClient;
import es.unex.silice.smallshi.recommender.hbase.grouping.JoinGroupsJob;

/**
 * <p>
 * computes prediction values for each user
 * </p>
 * 
 * <pre>
 * u = a user
 * i = an item not yet rated by u
 * N = all items similar to i (where similarity is usually computed by pairwisely comparing the item-vectors
 * of the user-item matrix)
 * 
 * Prediction(u,i) = sum(all n from N: similarity(i,n) * rating(u,n)) / sum(all n from N: abs(similarity(i,n)))
 * </pre>
 */
public final class AggregateAndRecommendReducer
		extends TableReducer<VarLongWritable, PrefAndSimilarityColumnWritable, RecommendedItemsWritable> {

	public static final String ITEMID_INDEX_PATH = "itemIDIndexPath";
	public static final String NUM_RECOMMENDATIONS = "numRecommendations";
	public static final int DEFAULT_NUM_RECOMMENDATIONS = 10;
	public static final String ITEMS_FILE = "itemsFile";

	private boolean booleanData;
	private IDReader idReader;
	private FastIDSet itemsToRecommendFor;
	private OpenIntLongHashMap indexItemIDMap;
	private HBaseClient hbaseClient;
	private String workingTable;
	private boolean trainingEnviorement;
	private String recommendationsCf;

	private static final float BOOLEAN_PREF_VALUE = 1.0f;

	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();

		booleanData = conf.getBoolean(RecommenderJob.BOOLEAN_DATA, false);
		indexItemIDMap = TasteHadoopUtils.readIDIndexMap(conf.get(ITEMID_INDEX_PATH), conf);

		idReader = new IDReader(conf);
		idReader.readIDs();
		itemsToRecommendFor = idReader.getItemIds();
		
		workingTable = conf.get(RecommenderJob.PARAM_WORKING_TABLE);
		
		trainingEnviorement = conf.getBoolean(RecommenderJob.PARAM_TRAINING_ENVIOREMENT, false);
		recommendationsCf = conf.get(RecommenderJob.PARAM_CF_RECOMMENDATIONS);
		
		hbaseClient = new HBaseClient(conf);
	}

	@Override
	protected void reduce(VarLongWritable userID,
			Iterable<PrefAndSimilarityColumnWritable> values, Context context)
			throws IOException, InterruptedException {
		if (booleanData) {
			reduceBooleanData(userID, values, context);
		} else {
			reduceNonBooleanData(userID, values, context);
		}
	}

	private void reduceBooleanData(VarLongWritable userID,
			Iterable<PrefAndSimilarityColumnWritable> values, Context context)
			throws IOException, InterruptedException {
		/*
		 * having boolean data, each estimated preference can only be 1, however
		 * we can't use this to rank the recommended items, so we use the sum of
		 * similarities for that.
		 */
		Iterator<PrefAndSimilarityColumnWritable> columns = values.iterator();
		Vector predictions = columns.next().getSimilarityColumn();
		while (columns.hasNext()) {
			predictions.assign(columns.next().getSimilarityColumn(),
					Functions.PLUS);
		}
		writeRecommendedItems(userID, predictions, context);
	}

	private void reduceNonBooleanData(VarLongWritable userID,
			Iterable<PrefAndSimilarityColumnWritable> values, Context context)
			throws IOException, InterruptedException {
		/* each entry here is the sum in the numerator of the prediction formula */
		Vector numerators = null;
		/*
		 * each entry here is the sum in the denominator of the prediction
		 * formula
		 */
		Vector denominators = null;
		/*
		 * each entry here is the number of similar items used in the prediction
		 * formula
		 */
		Vector numberOfSimilarItemsUsed = new RandomAccessSparseVector(
				Integer.MAX_VALUE, 100);

		for (PrefAndSimilarityColumnWritable prefAndSimilarityColumn : values) {
			Vector simColumn = prefAndSimilarityColumn.getSimilarityColumn();
			float prefValue = prefAndSimilarityColumn.getPrefValue();
			/* count the number of items used for each prediction */
			for (Element e : simColumn.nonZeroes()) {
				int itemIDIndex = e.index();
				numberOfSimilarItemsUsed.setQuick(itemIDIndex,
						numberOfSimilarItemsUsed.getQuick(itemIDIndex) + 1);
			}

			if (denominators == null) {
				denominators = simColumn.clone();
			} else {
				denominators.assign(simColumn, Functions.PLUS_ABS);
			}

			if (numerators == null) {
				numerators = simColumn.clone();
				if (prefValue != BOOLEAN_PREF_VALUE) {
					numerators.assign(Functions.MULT, prefValue);
				}
			} else {
				if (prefValue != BOOLEAN_PREF_VALUE) {
					simColumn.assign(Functions.MULT, prefValue);
				}
				numerators.assign(simColumn, Functions.PLUS);
			}

		}
		
		if (numerators == null) {
			return;
		}

		Vector recommendationVector = new RandomAccessSparseVector(
				Integer.MAX_VALUE, 100);
		for (Element element : numerators.nonZeroes()) {
			int itemIDIndex = element.index();
			/* preference estimations must be based on at least 2 datapoints */
			if (numberOfSimilarItemsUsed.getQuick(itemIDIndex) > 1) {
				/* compute normalized prediction */
				double prediction = element.get()
						/ denominators.getQuick(itemIDIndex);
				recommendationVector.setQuick(itemIDIndex, prediction);
			}
		}
		writeRecommendedItems(userID, recommendationVector, context);
	}

	/**
	 * find the top entries in recommendationVector, map them to the real
	 * itemIDs and write back the result
	 */
	private void writeRecommendedItems(VarLongWritable userID,
			Vector recommendationVector, Context context) throws IOException,
			InterruptedException {
		
		Put put = new Put(Bytes.toBytes(String.valueOf(userID.get())));
		
		FastIDSet itemsForUser = null;
		if(trainingEnviorement){
			itemsForUser = getItemsToRecommend(userID.get());
		}

		for (Element element : recommendationVector.nonZeroes()) {
			int index = element.index();
			long itemID;
			if (indexItemIDMap != null && !indexItemIDMap.isEmpty()) {
				itemID = indexItemIDMap.get(index);
			} else { // we don't have any mappings, so just use the original
				itemID = index;
			}
			
			if (shouldIncludeItemIntoRecommendations(itemID, itemsToRecommendFor, itemsForUser)) {
				float value = (float) element.get();
				System.out.println("-");
				if (!Float.isNaN(value)) {
					put.add(Bytes.toBytes(recommendationsCf), 
							Bytes.toBytes(String.valueOf(itemID)), 
							Bytes.toBytes(String.valueOf(value)));
				}
			}
		}

		if(!put.isEmpty())
			context.write(null, put);
	}


	private FastIDSet getItemsToRecommend(long idUser) throws IOException {
		
		List<Cell> columns = hbaseClient.getFamilyColumn(idUser+"", workingTable, JoinGroupsJob.CF_PREFERENCES_TEST);
		
		FastIDSet itemIds = new FastIDSet();
		if(columns == null)
			return itemIds;
		
		for (Cell cell : columns)
			itemIds.add(Long.valueOf(Bytes.toString(CellUtil.cloneQualifier(cell))));
		
		return itemIds;
	}

	private boolean shouldIncludeItemIntoRecommendations(long itemID,
			FastIDSet allItemsToRecommendFor, FastIDSet itemsForUser) {
		if (allItemsToRecommendFor == null && itemsForUser == null) {
			return true;
		} else if (itemsForUser != null) {
			return itemsForUser.contains(itemID);
		} else if (allItemsToRecommendFor != null) {
			return allItemsToRecommendFor.contains(itemID);
		} else {
			return false;
		}
	}

}
