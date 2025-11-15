import os
import subprocess


# add variables to .bashrc
home_dir = os.path.expanduser('~')

# hadoop
bashrc_content = """
# Hadoop environment variables
export HADOOP_HOME=/home/jz/hadoop-branch-3.2/hadoop-dist/target/hadoop-3.2.5-SNAPSHOT
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib:$HADOOP_HOME/lib/native"
"""

with open(os.path.join(home_dir, '.bashrc'), 'a') as bashrc_file:
    bashrc_file.write(bashrc_content)

bashrc_content = """
# Hadoop environment variables
export CLASSPATH=$(echo /home/jz/hadoop-branch-3.2/hadoop-dist/target/hadoop-3.2.5-SNAPSHOT/share/hadoop/common/*.jar | tr ' ' ':'):$CLASSPATH
export CLASSPATH=$(echo /home/jz/hadoop-branch-3.2/hadoop-dist/target/hadoop-3.2.5-SNAPSHOT/share/hadoop/common/lib/*.jar | tr ' ' ':'):$CLASSPATH
export CLASSPATH=$(echo /home/jz/hadoop-branch-3.2/hadoop-dist/target/hadoop-3.2.5-SNAPSHOT/share/hadoop/hdfs/*.jar | tr ' ' ':'):$CLASSPATH
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/jz/hadoop-branch-3.2/hadoop-dist/target/hadoop-3.2.5-SNAPSHOT/lib/native::/home/jz/environments/libhdfs3-downstream/libhdfs3/dist/lib
"""

with open(os.path.join(home_dir, '.bashrc'), 'a') as bashrc_file:
    bashrc_file.write(bashrc_content)