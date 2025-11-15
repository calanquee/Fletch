import subprocess
import os


# Important Remark
# - please first backup the original protoc before running the script
# - mv /usr/local/bin/protoc /usr/local/bin/protoc.bak
#   - here, some machines may store protoc in /usr/bin/protoc
#   - however, the make install will install protoc in /usr/local/bin/protoc
#   - thus we need cp /usr/local/bin/protoc /usr/bin/protoc 


os.chdir('../../environments/')

subprocess.run(['wget', 'https://github.com/google/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.gz'])
subprocess.run(['tar', 'zxvf', 'protobuf-2.5.0.tar.gz'])
os.chdir('protobuf-2.5.0')


subprocess.run(['./autogen.sh'])
subprocess.run(['./configure'])
subprocess.run(['make'])
subprocess.run(['sudo', 'make', 'install'])

subprocess.run(['sudo', 'ldconfig'])
# subprocess.run(['protoc', '--version'])

print("Protobuf v2.5.0 installation completed.")

# install ant via source code
os.chdir('../')
# wget https://archive.apache.org/dist/ant/source/apache-ant-1.10.7-src.tar.gz
subprocess.run(['wget', 'https://archive.apache.org/dist/ant/source/apache-ant-1.10.7-src.tar.gz'])
subprocess.run(['tar', 'zxvf', 'apache-ant-1.10.7-src.tar.gz'])
os.chdir('apache-ant-1.10.7')
subprocess.run('./bootstrap.sh')



home_dir = os.path.expanduser('~')

# ant
bashrc_content = """
export ANT_HOME=/home/jz/environments/apache-ant-1.10.7/apache-ant-1.10.7
export PATH=$PATH:$ANT_HOME/bin
"""

with open(os.path.join(home_dir, '.bashrc'), 'a') as bashrc_file:
    bashrc_file.write(bashrc_content)
