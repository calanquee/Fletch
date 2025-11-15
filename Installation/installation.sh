#!/bin/sh

# This script is used to install the application on the system.

# install java
python3 0_java.py
source ~/.bashrc
java -version

# install protoc v2.5.0 and ant
python3 1_protoc_ant.py
source ~/.bashrc
protoc --version
ant -version

# install hadoop
python3 2_compile_hdfs.py
python3 3_config_hdfs.py

# install hdfs client
python3 4_hdfs_client.py

# update environment variables
python3 5_update_env.py
source ~/.bashrc

# also need to update root's ~/.bashrc
sudo su
python3 5_update_env.py
source ~/.bashrc
exit

# re-compile bmv2
# python3 6_modify_bmv2_source.py

# netfetch dependencies
python3 7_netfetch_dependencies.py

# install filebench
# python3 8_filebench_installation.py

# install zookeeper
python3 10_zookeeper_installation.py
source ~/.bashrc