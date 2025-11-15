
set -x

# stop namenode and datanode
hdfs --daemon stop namenode
hdfs --daemon stop datanode
hdfs --daemon stop dfsrouter

# delete tmp/ logs/ in hdfs
rm -rf /home/jz/hadoop-branch-3.2/hadoop-dist/target/hadoop-3.2.5-SNAPSHOT/logs/
rm -rf /home/jz/hadoop-branch-3.2/hadoop-dist/target/hadoop-3.2.5-SNAPSHOT/tmp/

# format namenode
hdfs namenode -format -clusterId testbed123

# sleep 2s
sleep 2s

# start namenode and datanode
hdfs --daemon start namenode
hdfs --daemon start datanode
hdfs --daemon start dfsrouter

# sleep 2s
# sleep 2s

# start nnbench
# hadoop org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark -op fileStatus -threads 32 -files 100000
