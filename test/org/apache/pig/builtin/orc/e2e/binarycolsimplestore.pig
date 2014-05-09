register 'piggybank.jar';
register 'hive-exec-0.13.0.jar';

A = load 'page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader() as (user:chararray, action:int, timespent:int, query_term:chararray, ip_addr:long, timestamp:long,
        estimated_revenue:double, page_info:map[], page_links:bag{tuple(map[])});
store A into 'page_views_orc' using OrcStorage();
