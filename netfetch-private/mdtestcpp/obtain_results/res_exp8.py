import os
import re
import numpy as np
from numpy import sort
import json

root_dir = "/home/jz/In-Switch-FS-Metadata/netfetch-private/mdtestcpp/results"

dir_pattern = re.compile(r"(?P<method>[a-zA-Z0-9\-]+)_(?P<zeta>\d+\.\d+)_(?P<datasize>\d+)_(?P<depth>\d+)_(?P<branch_factor>\d+)_(?P<gen_method>\w+)")
file_pattern = re.compile(r"tmp_mdtest_(?P<zeta>\d+\.\d+)_(?P<server1>\w+)_(?P<server2>\w+)_(?P<round>[a-zA-Z\\-]+)_(?P<server_number>\d+)_mixed\.out")
line_pattern = re.compile("Total" +r"\s+:(\d+)\s+ms,\s+BOTTLENECK\s+:\s+(\d+\.\d+)ops/sec,\s+ROTATION\s+:\s+(\d+\.\d+)ops/sec,\s+BOTTLENECK HIT\s+:\s*(\d+(?:\.\d+)?),\s+ROTATION HIT\s+:\s*(\d+(?:\.\d+)?),")

round = "round_exp8_r4_dynamic"


def fix_and_parse_json(raw_text):
    fixed_text = re.sub(r'(?<=\})"Total"', r', "Total"', raw_text)
    fixed_text = re.sub(r',\s*}', r'}', fixed_text)
    return fixed_text

copy_to_excel = []

for idx in range(3,4):
    for sub_dir in os.listdir(f"{root_dir}/{round}/{idx}"):
        match = dir_pattern.match(sub_dir)
        if match:
            parsed_info = match.groupdict()
            sub_dir_path = f"{root_dir}/{round}/{idx}/{sub_dir}"
            for filename in os.listdir(sub_dir_path):
                file_match = file_pattern.match(filename)
                if file_match:
                    file_info = file_match.groupdict()
                    file_path = os.path.join(sub_dir_path, filename)
                    server_logical_num = file_info["server_number"]
                    with open(file_path, "r") as f:
                        for line in f:
                            line = line.strip()
                            if line.startswith("{") and line.endswith("}"):
                                fixed_line = fix_and_parse_json(line)
                                try:
                                    json_data = json.loads(fixed_line)
                                    if "Total" in json_data:
                                        total_data = json_data["Total"]
                                        total_sum = sum(total_data.values())
                                        copy_to_excel.append(total_sum)
                                except json.JSONDecodeError:
                                    print(f"Invalid JSON line skipped: {fixed_line}")
                    print(parsed_info['method'],',',",".join([str(int(x)) for x in copy_to_excel]))
                    copy_to_excel = []

print(copy_to_excel)
