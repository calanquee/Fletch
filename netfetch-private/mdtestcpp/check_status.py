import glob
from exp_common import get_hits
import logging
from enum import Enum
import hydra
from omegaconf import DictConfig
import numpy as np


class CheckStatus(Enum):
    SUCCESS = 0
    

def entrypoint():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    handler = logging.FileHandler('check_status.log', mode='w')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logger.info("Starting entrypoint")
    succ_path = "/home/jz/In-Switch-FS-Metadata/netfetch-private/mdtestcpp/results/round_exp6_r4_16/2/csfletch_0.9_32000000_10_4_3"
    fail_path = "/home/jz/In-Switch-FS-Metadata/netfetch-private/mdtestcpp/results/round_exp6_r4_16/2/csfletch_1.0_32000000_10_4_3_failure"    

    files = glob.glob(f"{succ_path}/*")
    for file in files:
        status = check_status(file, False, logger)
        logger.info(f"Status: {status}: {file.split('/')[-1]}")


    files = glob.glob(f"{fail_path}/*")
    for file in files:
        status = check_status(file, False, logger)
        logger.info(f"Status: {status}: {file.split('/')[-1]}")


@hydra.main(version_base=None, config_path="configs", config_name="exp2-stat")
def check_config(cfg: DictConfig):
    #   - mixed:
    #   open_close: 52.6
    #   stat: 12.4
    #   create: 9.58
    #   delete: 11.9
    #   rename: 9.3
    #   chmod: 0.1
    #   readdir: 3.9
    #   statdir: 0.2
    #   mkdir: 0.0042
    #   rmdir: 0.0042
    workload = cfg["workloads"][0]

    ret = np.nonzero(np.array(list(workload["mixed"].values()), dtype=float))[0]
    if len(ret) == 1:
        key = list(workload["mixed"].keys())[ret[0]]
        if key in ["open_close", "stat", "statdir"]:
            return True
        else:
            return False
    else:
        return True

# if __name__ == "__main__":
#     # simple retry mechanism
#     # not working because hydra main does not return value
#     idx = 0
#     while True:
#         ret = entrypoint()
#         if ret or idx > 5:
#             break
#         idx += 1
#     print("DONE")

if __name__ == "__main__":
    # entrypoint()
    check_config()