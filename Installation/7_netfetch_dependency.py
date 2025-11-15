
import subprocess
import os

def install_dependencies():
    packages = [
        "libgflags-dev", "libsnappy-dev", "zlib1g-dev", "libtbb-dev",
        "libbz2-dev", "liblz4-dev", "libzstd-dev",
        "libjemalloc-dev", "libsnappy-dev"
    ]
    subprocess.run(["sudo", "apt-get", "install", "-y"] + packages, check=True)


def install_boost_1_81_0():
    target_dir = os.path.expanduser('/home/jz/In-Switch-FS-Metadata/netfetch-private')
    boost_url = "https://boostorg.jfrog.io/artifactory/main/release/1.81.0/source/boost_1_81_0.tar.gz"
    boost_tarball = "boost_1_81_0.tar.gz"
    boost_dir = "boost_1_81_0"
    os.makedirs(target_dir, exist_ok=True)
    os.chdir(target_dir)
    subprocess.run(["wget", boost_url], check=True)
    subprocess.run(["tar", "-xzvf", boost_tarball], check=True)
    os.chdir(os.path.join(target_dir, boost_dir))
    subprocess.run(["./bootstrap.sh", "--with-libraries=system,thread", "--prefix=./install"], check=True)
    subprocess.run(["sudo", "./b2", "install"], check=True)




install_dependencies()
install_boost_1_81_0()

