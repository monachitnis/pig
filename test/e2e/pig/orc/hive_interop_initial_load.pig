register '/home/y/libexec/hive13/lib/hive-exec.jar';

A = load 'student_tab' as (name:chararray, age:int, gpa:float);
store A into 'student_tab_orc' using OrcStorage();
store A into 'monadb.student_tab' using org.apache.hive.hcatalog.pig.HCatStorer();

A = load 'student_null_tab' as (name:chararray, age:int, gpa:float);
store A into 'student_null_tab_orc' using OrcStorage();
store A into 'monadb.student_null_tab' using org.apache.hive.hcatalog.pig.HCatStorer();

B = load 'student_complex_tab' as (nameagegpamap:map[], nameagegpatuple:tuple(tname:chararray, tage:int, tgpa:float), nameagegpabag:bag{t:tuple(bname:chararray, bage:int, bgpa:float)}, nameagegpamap_name:chararray, nameagegpamap_age:int, nameagegpamap_gpa:float);
store B into 'student_complex_tab_orc' using OrcStorage();
store B into 'monadb.student_complex_tab' using org.apache.hive.hcatalog.pig.HCatStorer();

-- disable multi_query
B = load 'student_tab_orc' using OrcStorage();
store B into 'monadb.student_tab_orc_to_hive' using org.apache.hive.hcatalog.pig.HCatStorer();

C = load 'monadb.student_null_tab' using org.apache.hive.hcatalog.pig.HCatLoader();
store C into 'student_null_tab_hive_to_orc' using OrcStorage();

D = load 'monadb.student_complex_tab' using org.apache.hive.hcatalog.pig.HCatLoader();
store D into 'student_complex_tab_hive_to_orc' using OrcStorage();


#cmd
#./pig -Dmapreduce.job.queuename=grideng -Dpig.additional.jars=/home/y/libexec/hive13/lib/hive-exec.jar:/home/y/libexec/hive13/lib/hive-metastore.jar:/home/y/libexec/hive13/lib/libfb303.jar:/home/y/libexec/hive13/lib/hcatalog-core.jar:/home/y/libexec/hive13/lib/hcatalog-pig-adapter.jar:/home/y/libexec/hive13/auxlib/jdo-api-3.0.1.jar -f hive_interop.pig