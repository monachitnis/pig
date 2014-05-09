register 'piggybank.jar';
register 'hive-exec-0.13.0.jar';

A = load 'page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader() as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = foreach A generate user, (int)action as action, (int)timespent as timespent, query_term, (long)ip_addr as ip_addr, timestamp, estimated_revenue, (map[])page_info as page_info, flatten((bag{tuple(map[])})page_links) as page_links;
C = foreach B generate user, (action == 1 ? page_info#'a' : page_links#'b') as header;
D = group C by user parallel $PARALLEL;
E = foreach D generate group, COUNT(C) as cnt;
store E into '$PIGMIX_OUTPUT/L1out';

A = load 'page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader() as (user:chararray, action:int, timespent:int, query_term:chararray, ip_addr:long, timestamp:long,
        estimated_revenue:double, page_info:map[], page_links:bag{tuple(map[])});
store A into 'page_views_orc' using OrcStorage();

A= load 'page_views_orc' using OrcStorage();
B = foreach A generate user, (int)action as action, (int)timespent as timespent, query_term, (long)ip_addr as ip_addr, timestamp, estimated_revenue, (map[])page_info as page_info, flatten((bag{tuple(map[
])})page_links) as page_links;
C = foreach B generate user, (action == 1 ? page_info#'a' : page_links#'b') as header;
D = group C by user parallel $PARALLEL;
E = foreach D generate group, COUNT(C) as cnt;
store E into '$PIGMIX_OUTPUT/L1out_orc';

-- compare results in L1out and L1out_orc to be same
