import subprocess
import os


# Qingxiu: download java into environments
os.chdir('../../environments/')

jdk_url = "https://builds.openlogic.com/downloadJDK/openlogic-openjdk/8u342-b07/openlogic-openjdk-8u342-b07-linux-x64.tar.gz"
subprocess.run(['wget', jdk_url])
jdk_tar = "openlogic-openjdk-8u342-b07-linux-x64.tar.gz"
subprocess.run(['tar', 'zxvf', jdk_tar])
jdk_dir = "openlogic-openjdk-8u342-b07-linux-x64"

subprocess.run(['sudo', 'mkdir', '-p', '/usr/lib/jvm'])
subprocess.run(['sudo', 'mv', jdk_dir, '/usr/lib/jvm/jdk8u342'])


bashrc_path = os.path.expanduser('/home/jz/.bashrc')
java_home = "export JAVA_HOME=/usr/lib/jvm/jdk8u342"
path = "export PATH=$PATH:$JAVA_HOME/bin"


with open(bashrc_path, 'a') as file:
    file.write(f"\n{java_home}\n{path}\n")
    
# Qingxiu: install cmake
subprocess.run(['sudo', 'apt-get', 'install', 'cmake'], check=True)


# # need to do source ~/.bashrc manually
# subprocess.run(['source', bashrc_path], shell=True, executable='/bin/bash')
# subprocess.run(['java', '-version'])
