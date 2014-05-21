-- data inter-readable

-- generate data
./e2e/pig/tools/generate/generate_data.pl studenttab 10000 student_tab
hive -e "use monadb;load data local inpath 'student_tab' overwrite into table student_tab"
X = load 'monadb.student_tab' using org.apache.hive.hcatalog.pig.HCatLoader();
describe X;
store X into 'student_tab_hive_to_orc' using OrcStorage();

Y = load 'student_tab_hive_to_orc' using OrcStorage();
describe Y;
Z1 = foreach Y generate name, age, gpa;
Z = order Z1 by gpa;
W = limit Z 500;
store W into 'check_pig';

-- complex data 
-- load 'student_complex_tab_orc' into hive table with ORC compression
-- and check plaintext pigstorage data and hive queries
use monadb;
create table student_complex_tab_orc_to_hive (
nameagegpamap MAP<STRING, BINARY>,
nameagegpatuple STRUCT<tname:STRING, tage:INT, tgpa:FLOAT>,
nameagegpabag ARRAY<STRUCT<bname:STRING, bage:INT, bgpa:FLOAT>>,
nameagegpamap_name varchar(500),
nameagegpamap_age int,
nameagegpamap_gpa float)
row format delimited fields terminated by '\t'
map keys terminated by '#'
stored as orc TBLPROPERTIES("orc.compress"="ZLIB");

hive -e "use monadb;load data inpath '/user/chitnis/student_complex_tab_orc' overwrite into table student_tab_orc_to_hive"
hive -e "set mapreduce.job.queuename=grideng;use monadb;select count(*) from student_complex_tab_orc_to_hive"
hive -e "set mapreduce.job.queuename=grideng;use monadb;select * from student_complex_tab_orc_to_hive limit 15"

X = load 'monadb.student_tab_orc_to_hive' using org.apache.hive.hcatalog.pig.HCatLoader();
describe X;
Y1 = foreach Y generate nameagegpabag, nameagegpatuple, nameagegpamap_gpa;
store Y1 into 'student_complex_tab_hive_to_orc' using OrcStorage();

Y = load 'student_complex_tab_hive_to_orc' using OrcStorage();
describe Y;
Z = order Y by nameagegpamap_gpa;
store Z into 'check_pig_complex';


-- test hive partitions

cat student_tab | head -n 300 > student_tab_1;
cat student_tab | tail -n 600 | head -n 300 > student_tab_2;
cat student_tab | tail -n 300 > student_tab_3;

hive -e  "use monadb;load data local inpath 'student_tab_1' overwrite into table student_tab_direct partition (range=1)"
hive -e  "use monadb;load data local inpath 'student_tab_2' overwrite into table student_tab_direct partition (range=2)"
hive -e  "use monadb;load data local inpath 'student_tab_3' overwrite into table student_tab_direct partition (range=3)"

X = load 'monadb.student_tab_direct' using org.apache.hive.hcatalog.pig.HCatLoader();
Y = filter X by range == 1 || range == 2;
Z = foreach Y generate name, age, gpa;
store Z into 'check_pig';

hdfs dfs -cat check_pig/part-m-00000 | wc -l
-- check 600

