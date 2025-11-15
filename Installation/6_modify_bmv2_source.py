import subprocess
import os

BMV2_HOME = os.path.join(os.environ['P4_HOME'], 'behavioral-model')

FILE = os.path.join(BMV2_HOME, 'targets', 'simple_switch', 'primitives.cpp')
PATTERN = "hdr.reset();"
COMMENT = "// hdr.reset();"


with open(FILE, 'r') as file:
    lines = file.readlines()

with open(FILE, 'w') as file:
    for line in lines:
        if PATTERN in line:
            line = line.replace(PATTERN, COMMENT)
        file.write(line)
        
# modify /home/jz/P4/behavioral-model/src/bm_sim/checksums.cpp
# FILE = os.path.join(BMV2_HOME, 'src', 'bm_sim', 'checksums.cpp')



os.chdir(BMV2_HOME)

subprocess.run(['make'])

subprocess.run(['sudo', 'make', 'install'])
