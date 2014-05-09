register 'hive-exec-0.13.0.jar';
A = load 'orc-input.orc' using OrcStorage();
B = filter A by boolean1 == true;
C = foreach B generate boolean1..double1, '' as bytes1, string1..;
D = order C by $1;
E = limit D 10;
store E into 'orc-output' using OrcStorage();
