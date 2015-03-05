REGISTER ./hbase-client-0.98.6-hadoop2.jar;
REGISTER ./hbase-common-0.98.6-hadoop2.jar;
REGISTER ./hbase-server-0.98.6-hadoop2.jar;
REGISTER ./Pigitos-1.0-SNAPSHOT.jar;

DEFINE MapKeysToBag pl.ceon.research.pigitos.pig.udf.MapKeysToBag;
set hbase.zookeeper.quorum 'nodo1' 

users = LOAD 'hbase://r_users' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
	'pref:*', '-loadKey true -limit 2')
    AS (user:int, preferences:map[(rating:int)]);
items = LOAD 'hbase://r_items' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
	'pref:*', '-loadKey true -limit 2')
    AS (item:int, preferences:map[(rating:int)]);

user_items = FOREACH users GENERATE (int) user, (bag{tuple(int)}) MapKeysToBag(preferences) AS items:bag{item:tuple(id:int)};

-- RM1: Numero de items que ha votado u
	num_pref_per_user = FOREACH users GENERATE user, SIZE(preferences) AS num_preferences;
	
	STORE num_pref_per_user INTO 'hbase://r_users' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('param:RM1');

-- RM2: Numero de usuarios que han votado a i
	num_pref_per_item = FOREACH items GENERATE (int) item, SIZE(preferences) AS num_preferences;

	STORE num_pref_per_item INTO 'hbase://r_items' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('param:RM2');

-- RM3: Numero de usuarios que han votado los items, que u ha votado
	-- Add number of preferences of each item
	user_item = FOREACH user_items GENERATE user, FLATTEN(items) AS item;
	user_item_num_pref_join = JOIN user_item BY item, num_pref_per_item BY item;
	user_item_num_pref = FOREACH user_item_num_pref_join GENERATE $0 AS user, $3 AS sum;
    
	-- Sum number of preferences
	users_rm3_grouped = GROUP user_item_num_pref BY user;	
	users_rm3 = FOREACH users_rm3_grouped GENERATE group AS user, SUM(user_item_num_pref.sum);
   
	STORE users_rm3 INTO 'hbase://r_users' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('param:RM3');

-- RM4: Numero de usuarios que han votado >= x items, que u ha votado
	user_items_aux = FOREACH user_items GENERATE user, items;
	users_crossed = CROSS user_items, user_items_aux;
	users_crossed_named = FOREACH users_crossed GENERATE user_items::user AS user, user_items::items AS pref, user_items_aux::user AS user_aux, user_items_aux::items AS pref2;
	users_crossed_without_themself = FILTER users_crossed_named BY user != user_aux;
	users_pref_sizes = FOREACH users_crossed_without_themself GENERATE user, SIZE(DIFF(pref, pref2)) AS size_dif, SIZE(pref) AS size_perf, SIZE(pref2) AS size_perf2;
	users_num_common = FOREACH users_pref_sizes  GENERATE user, ((size_perf + size_perf2 - size_dif) / 2) AS num_common_prefs;
	users_common_ge_x = FILTER users_num_common BY num_common_prefs >= 3;
	users_grouped = GROUP users_common_ge_x BY user;
	users_rm4 = FOREACH users_grouped GENERATE group AS user, SIZE(users_common_ge_x);

	STORE users_rm4 INTO 'hbase://r_users' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('param:RM4');