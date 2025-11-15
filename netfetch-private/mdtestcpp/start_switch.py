import os
import subprocess
import configparser
import time
import argparse
import shutil
import threading
import multiprocessing
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# import hdfs3
from hdfs3 import HDFileSystem

# import utils.cache_gen_util.api
from utils.workload_gen_util.work_generate import *
from utils.workload_gen_util.fast_sort_lines import *
from utils.cache_gen_util.api import *
from utils.remote_util import *
from utils.dir_tree import *

method_name = "csfletch"

init_switch_connections()
stop_switch()
start_switch(method_name)