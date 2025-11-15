import subprocess
import os


hadoop_common_dir = os.path.expanduser('/home/jz/hadoop-branch-3.2/hadoop-common-project/hadoop-common')
hadoop_hdfs_project_dir = os.path.expanduser('/home/jz/hadoop-branch-3.2/hadoop-hdfs-project/hadoop-hdfs')
hadoop_rbf_project_dir = os.path.expanduser('/home/jz/hadoop-branch-3.2/hadoop-hdfs-project/hadoop-hdfs-rbf')
hadoop_home_dir = os.path.expanduser('/home/jz/hadoop-branch-3.2/hadoop-dist/target/hadoop-3.2.5-SNAPSHOT')


# subprocess.run(['cp', '../DNS.java', f'{hadoop_common_dir}/src/main/java/org/apache/hadoop/net'], check=True)

# subprocess.run(['mvn', 'clean', 'install', '-Pdist,native,src', '-DskipTests', '-Dtar', '-Dmaven.javadoc.skip=true'], cwd=hadoop_common_dir, check=True)
# subprocess.run(['cp', f'{hadoop_common_dir}/target/hadoop-common-3.2.5-SNAPSHOT.jar', f'{hadoop_home_dir}/share/hadoop/common'], check=True)

# # sleep 5s
# subprocess.run(['sleep', '5'], check=True)

# subprocess.run(['mvn', 'clean', 'install', '-Pdist,native,src', '-DskipTests', '-Dtar', '-Dmaven.javadoc.skip=true'], cwd=hadoop_hdfs_project_dir, check=True)
# subprocess.run(['cp', f'{hadoop_hdfs_project_dir}/target/hadoop-hdfs-3.2.5-SNAPSHOT.jar', f'{hadoop_home_dir}/share/hadoop/hdfs'], check=True)


# # sleep 5s
# subprocess.run(['sleep', '5'], check=True)

subprocess.run(['mvn', 'clean', 'install', '-Pdist,native,src', '-DskipTests', '-Dtar', '-Dmaven.javadoc.skip=true'], cwd=hadoop_rbf_project_dir, check=True)
subprocess.run(['cp', f'{hadoop_rbf_project_dir}/target/hadoop-hdfs-rbf-3.2.5-SNAPSHOT.jar', f'{hadoop_home_dir}/share/hadoop/hdfs'], check=True)
