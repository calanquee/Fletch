import os
import subprocess


# add variables to .bashrc

home_dir = os.path.expanduser('~')

# modify hadoop configuration files
hadoop_home = os.path.join(home_dir, 'hadoop-branch-3.2', 'hadoop-dist', 'target', 'hadoop-3.2.5-SNAPSHOT')

# mkdir /home/jz/hadoop-branch-3.2/hadoop-dist/target/hadoop-3.2.5-SNAPSHOT/configs
# subprocess.run(f'mkdir {os.path.join(hadoop_home, "configs")}', shell=True, check=True)
# subprocess.run(f'mkdir {os.path.join(hadoop_home, "configs", "ns1")}', shell=True, check=True)
# subprocess.run(f'mkdir {os.path.join(hadoop_home, "configs", "ns2")}', shell=True, check=True)

# copy well-written configuration files in ../hdfs3.2/hadoop_configs to hadoop_home/etc/hadoop
hadoop_configs_dir = os.path.join(home_dir, 'In-Switch-FS-Metadata', 'hdfs3.2', 'hadoop_configs', 'multi_nn_configs', 'rbf-multi-routers', 'ns2')
hadoop_etc_dir = os.path.join(hadoop_home, 'etc', 'hadoop')
subprocess.run(f'cp -r {hadoop_configs_dir}/* {hadoop_etc_dir}', shell=True, check=True)


hadoop_configs_dir = os.path.join(home_dir, 'In-Switch-FS-Metadata', 'hdfs3.2', 'hadoop_configs', 'multi_nn_configs', 'rbf-multi-routers', 'ns1')
hadoop_etc_dir = os.path.join(hadoop_home, 'configs', 'ns1')
subprocess.run(f'cp -r {hadoop_configs_dir}/* {hadoop_etc_dir}', shell=True, check=True)


hadoop_configs_dir = os.path.join(home_dir, 'In-Switch-FS-Metadata', 'hdfs3.2', 'hadoop_configs', 'multi_nn_configs', 'rbf-multi-routers', 'ns2')
hadoop_etc_dir = os.path.join(hadoop_home, 'configs', 'ns2')
subprocess.run(f'cp -r {hadoop_configs_dir}/* {hadoop_etc_dir}', shell=True, check=True)

# copy configs in ./configs/ns1 to hadoop_home/etc/hadoop manually
# copy configs in ./configs/ns2 to hadoop_homw/etc/hadoop manually