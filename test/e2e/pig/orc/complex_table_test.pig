-- pigstorage results to compare against

D = load 'student_complex_tab' as (nameagegpamap:map[], nameagegpatuple:tuple(tname:chararray, tage:int, tgpa:float), nameagegpabag:bag{t:tuple(bname:chararray, bage:int, bgpa:float)}, nameagegpamap_name:chararray, nameagegpamap_age:int, nameagegpamap_gpa:float);
E = load 'student_tab' as (name:chararray, age:int, gpa:float);
F = foreach D generate nameagegpamap#'name' as name, nameagegpatuple.tage as tage, FLATTEN(nameagegpabag) as (bname, bage, bgpa);
G = foreach E generate name, age, gpa;
H = join F by name, G by name;
I = group H by (G::gpa, G::tage);
J = foreach I generate group as grp;
K = order J by grp;
L = limit K 1000;
store L into 'compare_join_complex_tab';

D = load 'student_complex_tab' using OrcStorage();
E = load 'monadb.student_tab' using org.apache.hive.hcatalog.pig.HCatLoader();
F = foreach D generate nameagegpamap#'name' as name, nameagegpatuple.tage as tage, FLATTEN(nameagegpabag) as (bname, bage, bgpa);
G = foreach E generate name, age, gpa;
H = join F by name, G by name;
I = group H by (G::bgpa, G::tage);
J = foreach I generate group as grp;
K = order J by grp;
L = limit K 1000;
store L into 'join_complex_tab';