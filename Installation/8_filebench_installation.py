import subprocess
import os

# change dir to ~/In-Switch-FS-Metadata
os.chdir('../../environments/')
filebench_url = "https://sourceforge.net/projects/filebench/files/1.5-alpha3/filebench-1.5-alpha3.tar.gz"
subprocess.run(['wget', filebench_url])
filebench_tar = "filebench-1.5-alpha3.tar.gz"
subprocess.run(['tar', 'zxvf', filebench_tar])

os.chdir('filebench-1.5-alpha3')
subprocess.run(['./configure'])
subprocess.run(['make'])
subprocess.run(['sudo', 'make', 'install'])
subprocess.run(['filebench', '-h'])

# change /proc/sys/kernel/randomize_va_space to 0
subprocess.run(['sudo', 'sysctl', '-w', 'kernel.randomize_va_space=0'])
