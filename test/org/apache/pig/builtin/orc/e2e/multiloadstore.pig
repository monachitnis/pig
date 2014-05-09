register 'hive-exec-0.13.0.jar';
A = load 'data' using PigStorage();
store A into 'orc-output' using OrcStorage();

B = load 'orc-output' using OrcStorage();
store B into 'orc-output2' using OrcStorage();
