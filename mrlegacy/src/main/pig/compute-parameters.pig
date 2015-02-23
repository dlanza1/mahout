REGISTER hdfs:///user/root/dependencies/Pigitos-1.0-SNAPSHOT.jar;
DEFINE MapKeysToBag pl.ceon.research.pigitos.pig.udf.MapKeysToBag;

users = LOAD 'hbase://r_users' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
	'pref:*', '-loadKey true -caster HBaseBinaryConverter -limit 2')
    AS (id_user:chararray, preferences:map[(rating:int)]);
items = LOAD 'hbase://r_items' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
	'pref:*', '-loadKey true -caster HBaseBinaryConverter -limit 2')
    AS (id_item:chararray, preferences:map[(rating:int)]);

-- RM1: Numero de items que ha votado u
	num_pref_per_user = FOREACH users GENERATE id_user, SIZE(preferences) AS num_preferences;
	
	-- Show example
	tmp = LIMIT num_pref_per_user 10;
	DUMP tmp;

-- RM2: Numero de usuarios que han votado a i
	num_pref_per_item = FOREACH items GENERATE id_item, SIZE(preferences) AS num_preferences;
	
	-- Show example
	tmp = LIMIT num_pref_per_item 10;
	-- DUMP tmp;

-- RM3: Numero de usuarios que han votado los items, que u ha votado
	-- Map to bag and flatten -> http://hakunamapdata.com/pigitos-in-action-reading-hbase-column-family-content-in-a-real-world-application/
    user_item = FOREACH users GENERATE id_user, FLATTEN(MapKeysToBag(preferences)) AS id_item;

    -- Add number of preferences of each item
    user_item_num_pref = JOIN user_item BY id_item, num_pref_per_item BY id_item;
    user_item_num_pref = FOREACH user_item_num_pref GENERATE $0 AS id_user, $3 AS sum;
    
    -- Sum number of preferences
    users_rm3 = GROUP user_item_num_pref BY id_user;
	users_rm3 = FOREACH users_rm3 GENERATE group AS id_user, SUM(user_item_num_pref.sum);
    
    -- Show example
    tmp = LIMIT users_rm3 40;
	-- DUMP tmp;

-- RM4: Numero de usuarios que han votado >= x items, que u ha votado
DEFINE DUPLICATE(in) RETURNS out{$out = FOREACH $in GENERATE *;};

-- users = LOAD 'hbase://r_users' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
--	'pref:*', '-loadKey true -caster HBaseBinaryConverter -limit 2')
--    AS (id_user:chararray, preferences:map[(rating:int)]);

user_item = LOAD 'hdfs://nodo2:8020/user/hdfs/pig/datos_prueba.txt'
				 using PigStorage(' ') 
                 AS (user:long, item:long);
users_pref = GROUP user_item BY user;
users_pref = FOREACH users_pref {
			 	items = FOREACH user_item GENERATE item;
                GENERATE group AS user, items AS items; 
             }

users_pref_aux = DUPLICATE(users_pref);
users_crossed = CROSS users_pref, DUPLICATE(users_pref_aux);
users_crossed_named = FOREACH user_rm4 
					 GENERATE users_pref::user AS user, 
					 		  users_pref::items AS pref, 
					 		  users_pref2::user AS user_aux, 
					 		  users_pref2::items AS pref2;
users_crossed_without_themself = FILTER users_crossed_named BY user != user_aux;
users_pref_sizes = FOREACH users_crossed_without_themself 
				  GENERATE user, 
				  		   SIZE(DIFF(pref, pref2)) AS size_dif, 
				  		   SIZE(pref) AS size_perf, 
				  		   SIZE(pref2) AS size_perf2;
users_num_common = FOREACH users_pref_sizes 
				  GENERATE user, 
				  		   ((size_perf + size_perf2 - size_dif) / 2) AS num_common_prefs;
users_common_ge_x = FILTER users_num_common BY num_common_prefs >= 3;
users_grouped = GROUP users_common_ge_x BY user;
user_rm4 = FOREACH users_grouped 
		  GENERATE group AS user, 
		  		   SIZE(users_common_ge_x);



