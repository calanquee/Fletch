import subprocess
import os


# cd ~/environments
os.chdir(os.path.expanduser('~/environments'))

# wget https://archive.apache.org/dist/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz
subprocess.run(['wget', 'https://archive.apache.org/dist/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz'], check=True)
# tar zxvf zookeeper-3.4.6.tar.gz
subprocess.run(['tar', 'zxvf', 'zookeeper-3.4.6.tar.gz'], check=True)

# cd zookeeper-3.4.6
os.chdir('zookeeper-3.4.6')

# mkdir data
subprocess.run(['mkdir', 'data'], check=True)
# mkdir logs
subprocess.run(['mkdir', 'logs'], check=True)

# cp conf/zoo_sample.cfg conf/zoo.cfg
subprocess.run(['cp', 'conf/zoo_sample.cfg', 'conf/zoo.cfg'], check=True)
# vi conf/zoo.cfg
# Add the following line:
# dataDir=/home/jz/environments/zookeeper-3.4.6/data
# dataLogDir=/home/jz/environments/zookeeper-3.4.6/logs
# server.1=ip:2888:3888
conf_contents = '''
dataDir=/home/jz/environments/zookeeper-3.4.6/data
dataLogDir=/home/jz/environments/zookeeper-3.4.6/logs
server.1=10.26.43.164:2888:3888
'''
with open('conf/zoo.cfg', 'a') as conf_file:
    conf_file.write(conf_contents)


# touch data/myid
subprocess.run(['touch', 'data/myid'], check=True)
# echo 1 > data/myid
with open('data/myid', 'w') as myid_file:
    myid_file.write('1\n')


# add variables to .bashrc
home_dir = os.path.expanduser('~')
bashrc_content = """
export ZOOKEEPER_HOME=/home/jz/environments/zookeeper-3.4.6
export PATH=$PATH:$ZOOKEEPER_HOME/bin
"""
with open(os.path.join(home_dir, '.bashrc'), 'a') as bashrc_file:
    bashrc_file.write(bashrc_content)




# ./bin/zkServer.sh start
# subprocess.run(['./bin/zkServer.sh', 'start'], check=True)
# subprocess.run(['./bin/zkServer.sh', 'status'], check=True)



