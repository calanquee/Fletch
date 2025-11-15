import subprocess
import os

# hadoop-branch-3.2.0 has bugs that lead to unsuccessful compilation
# take care to checkout the correct branch

subprocess.run(['sudo', 'apt', 'install', 'protobuf-compiler', 'protobuf-c-compiler', 'maven'])
subprocess.run(['sudo', 'apt-get', 'install', 'libssl-dev', 'doxygen', 'graphviz', 'bzip2', 'libbz2-dev', 'fuse', 'libfuse-dev', 'libzstd1-dev', 'libsnappy-dev', 'pkg-config', 'gsasl'])
subprocess.run(['sudo', 'apt-get', 'install', 'libsasl2-dev'])

os.chdir('../../')
subprocess.run(['git', 'clone', 'https://github.com/apache/hadoop.git', 'hadoop-branch-3.2'], check=True)
os.chdir('hadoop-branch-3.2')
subprocess.run(['git', 'checkout', 'branch-3.2'])
subprocess.run(['mvn', 'package', '-Pdist,native,src', '-DskipTests', '-Dtar', '-Dmaven.javadoc.skip=true'])

# mvn clean package -Pdist,native,src -DskipTests -Dtar -Dmaven.javadoc.skip=true
