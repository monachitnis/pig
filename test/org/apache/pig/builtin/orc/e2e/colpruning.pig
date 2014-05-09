register 'hive-exec-0.13.0.jar';
A = load 'actors' using PigStorage() as (id:int, name:chararray, last:chararray, type:chararray, home:chararray);
store A into 'actors_orc' using OrcStorage();

B = load 'team' using PigStorage() as (name:chararray, grade:int, post:int, type:chararray);
store B into 'team_orc' using OrcStorage();

C = load 'actors_orc' using OrcStorage();
C1 = foreach C generate name, type, home;
D = load 'team_orc' using OrcStorage();
D1 = foreach D generate name, type, post;
E = join C1 by name, D1 by name;
F = foreach E generate C1::name, C1::type, D1::post;
store F into 'joined_orc' using OrcStorage();
