-- pigstorage results to compare against

D = load 'student_null_tab' as (name:chararray, age:int, gpa:float);
E = load 'student_tab' as (name:chararray, age:int, gpa:float);
F = join D by name, E by name;
G = group F by E::gpa;
H = order G by group;
I = limit H 1000;
store I into 'compare_join_null_tab';

-- orcstorage test

D = load 'monadb.student_tab' using org.apache.hive.hcatalog.pig.HCatLoader();
E = load 'student_null_tab_orc' using OrcStorage();
F = foreach D generate name, age, gpa;
G = foreach E generate name, age, gpa;
H = join F by name, G by name;
I = group H by (G::gpa);
J = foreach I generate group as grp;
K = order J by grp;
L = limit K 1000;
store L into 'join_null_tab';