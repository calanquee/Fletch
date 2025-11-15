import subprocess
import os

def install_dependencies():
    dependencies = [
        "libxml2-dev",
        # "libgmock-dev",
        "libcurl4-openssl-dev",
        "uuid-dev",
        "krb5-user",
        "libkrb5-dev",
        "libpam-krb5",
        "libpam-ccreds",
        "gsasl",
        "libghc-gsasl-dev"
    ]
    for dep in dependencies:
        subprocess.run(["sudo", "apt-get", "install", "-y", dep], check=True)


def clone_and_configure():
    # download in ~/environments
    home_dir = os.path.expanduser('~')
    environment_home = os.path.join(home_dir, 'environments')
    os.chdir(environment_home)

    subprocess.run(["git", "clone", "https://github.com/ContinuumIO/libhdfs3-downstream.git"], check=True)
    subprocess.run(["mkdir", "-p", "libhdfs3-downstream/libhdfs3/build"], check=True)
    # prepare google test
    subprocess.run(["mkdir", "-p", "thirdparty"], cwd='libhdfs3-downstream',check=True)
    subprocess.run(["mkdir", "-p", "thirdparty/googletest"], cwd='libhdfs3-downstream',check=True)

    subprocess.run(["git", "clone", "https://github.com/google/googletest.git"], cwd='libhdfs3-downstream/thirdparty/googletest', check=True)
    subprocess.run(["git", "checkout","release-1.7.0"], cwd='libhdfs3-downstream/thirdparty/googletest/googletest', check=True)

    subprocess.run(["mkdir","-p","build"], cwd='libhdfs3-downstream/thirdparty/googletest/googletest',check=True)
    subprocess.run(["cmake", ".."], cwd='libhdfs3-downstream/thirdparty/googletest/googletest/build', check=True)
    subprocess.run(["make","-j24"], cwd='libhdfs3-downstream/thirdparty/googletest/googletest/build', check=True)
    # subprocess.run(["sudo", "cp","libgtest.a","/usr/lib/x86_64-linux-gnu/"], cwd='libhdfs3-downstream/thirdparty/googletest/googletest/build', check=True)
    # subprocess.run(["sudo", "cp","libgtest_main.a","/usr/lib/x86_64-linux-gnu/"], cwd='libhdfs3-downstream/thirdparty/googletest/googletest/build', check=True)

    # # prepare google mock
    # # wget https://github.com/google/googlemock/archive/refs/tags/release-1.7.0.tar.gz
    subprocess.run(["wget", "https://github.com/google/googlemock/archive/refs/tags/release-1.7.0.tar.gz"], cwd='libhdfs3-downstream/thirdparty/googletest', check=True)
    subprocess.run(["tar","-xzvf" ,"release-1.7.0.tar.gz"], cwd='libhdfs3-downstream/thirdparty/googletest', check=True)
    subprocess.run(["mv","googlemock-release-1.7.0" ,"googlemock"], cwd='libhdfs3-downstream/thirdparty/googletest', check=True)
    subprocess.run(["mkdir","-p","build"], cwd='libhdfs3-downstream/thirdparty/googletest/googlemock',check=True)
    subprocess.run(["cp", "-r", "googletest/" ,"./googlemock/gtest"],cwd="libhdfs3-downstream/thirdparty/googletest",check=True)
    subprocess.run(["cmake", ".."], cwd='libhdfs3-downstream/thirdparty/googletest/googlemock/build', check=True)
    subprocess.run(["make", "-j24"], cwd='libhdfs3-downstream/thirdparty/googletest/googlemock/build', check=True)
    # subprocess.run(["sudo", "cp","libgmock.a","/usr/lib/x86_64-linux-gnu/"], cwd='libhdfs3-downstream/thirdparty/googletest/googlemock/build', check=True)
    # subprocess.run(["sudo", "cp","libgmock_main.a","/usr/lib/x86_64-linux-gnu/"], cwd='libhdfs3-downstream/thirdparty/googletest/googlemock/build', check=True)
    # cp -r ../googlemock/build/* ./
    subprocess.run(["mkdir","-p","build"], cwd='libhdfs3-downstream/thirdparty/googletest/',check=True)
    subprocess.run(["mkdir","-p","build/googlemock"], cwd='libhdfs3-downstream/thirdparty/googletest/',check=True)
    # Qingxiu: cp -r googlemock/build/* ./build/googlemock
    subprocess.run(["cp", "googlemock/build/CMakeCache.txt", "./build/googlemock"], cwd='libhdfs3-downstream/thirdparty/googletest/',check=True)
    subprocess.run(["cp", "googlemock/build/cmake_install.cmake", "./build/googlemock"], cwd='libhdfs3-downstream/thirdparty/googletest/',check=True)
    subprocess.run(["cp", "googlemock/build/libgmock.a", "./build/googlemock"], cwd='libhdfs3-downstream/thirdparty/googletest/',check=True)
    subprocess.run(["cp", "googlemock/build/libgmock_main.a", "./build/googlemock"], cwd='libhdfs3-downstream/thirdparty/googletest/',check=True)
    subprocess.run(["cp", "googlemock/build/Makefile", "./build/googlemock"], cwd='libhdfs3-downstream/thirdparty/googletest/',check=True)
    subprocess.run(["cp", "-r", "googlemock/build/CMakeFiles", "./build/googlemock"], cwd='libhdfs3-downstream/thirdparty/googletest/',check=True)
    subprocess.run(["cp", "-r", "googlemock/build/gtest", "./build/googlemock"], cwd='libhdfs3-downstream/thirdparty/googletest/',check=True) 
    
    subprocess.run(["../bootstrap"], cwd="libhdfs3-downstream/libhdfs3/build", shell=True, check=True)
    # Qingxiu: maybe need to comment CMAKE_CHECK_FINE in ~/environments/libhdfs3-downstream/libhdfs3/CMake/FindGoogleTest.cmake

def build_and_install():
    subprocess.run(["make", "-j24"], cwd="libhdfs3-downstream/libhdfs3/build", check=True)
    subprocess.run(["sudo", "make", "install"], cwd="libhdfs3-downstream/libhdfs3/build", check=True)

def install_python_package():
    subprocess.run(["pip", "install", "hdfs3"], check=True)

if __name__ == "__main__":
    install_dependencies()
    clone_and_configure()
    build_and_install()
    install_python_package()
