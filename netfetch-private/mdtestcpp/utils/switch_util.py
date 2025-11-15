import time
import paramiko

# from utils.remote_util import switchSSHs, SWITCH_PWDs, SWITCH_IPs, SWITCH_HOME
SWITCH_IPs = ["10.26.43.200"]
SWITCH_PWDs = ["er1ahht41q"]
SWITCH_PORTs = ["43177"]
SWITCH_USERs = ["cyh"]
SWITCH_HOME = [f"/home/{SWITCH_USERs[0]}/jzcai/In-Switch-FS-Metadata/"]
switchSSHs = [paramiko.SSHClient()]


def stop_switch():
    for i, switch in enumerate(switchSSHs):
        try:
            shell = switch.invoke_shell()
            time.sleep(1)
            shell.send("sudo -i\n")
            time.sleep(1)
            shell.send(f"{SWITCH_PWDs[i]}\n")  # Send sudo password
            time.sleep(2)

            # Kill 'bf_switchd'
            shell.send("pkill -f 'bf_switchd'\n")
            time.sleep(1)
            print(f"[switch {SWITCH_IPs[i]}] Killed 'bf_switchd' process")

            # Kill 'bash ptf_popserver.sh'
            shell.send("pkill -f 'run_p4_tests'\n")
            time.sleep(1)
            shell.send("pkill -f 'ptf_popserver'\n")
            time.sleep(1)
            print(f"[switch {SWITCH_IPs[i]}] Killed 'bash ptf_popserver.sh' process")

            # Kill 'bash start_switch.sh'
            shell.send("pkill -f 'start_switch'\n")
            time.sleep(1)
            print(f"[switch {SWITCH_IPs[i]}] Killed 'bash start_switch.sh' process")

            # Kill './switchos'
            shell.send("pkill -f './switchos'\n")
            time.sleep(1)
            print(f"[switch {SWITCH_IPs[i]}] Killed './switchos' process")

            time.sleep(2)  # Wait for processes to be killed
            # output = shell.recv(65535).decode("utf-8")
            # print(output)
        except Exception as e:
            print(f"[switch {i}] Error: {str(e)}")
        finally:
            shell.close()


def check_port_occupied(shell, port=5006, max_retries=10, wait_time=1):
    try:
        retries = 0
        while retries <= max_retries:
            # 使用 netstat 命令检查5006端口是否被占用
            shell.send(f"netstat -an | grep {port} | grep LISTEN\n")
            time.sleep(1)
            output = shell.recv(65535).decode("utf-8")
            if f":{port}" in output and "LISTEN" in output:
                # 如果有输出，说明端口被占用
                if retries < max_retries:
                    print(
                        f"Port {port} is occupied, retrying in {wait_time} seconds..."
                    )
                    retries += 1
                    time.sleep(wait_time)  # 等待5秒再重试
                else:
                    raise Exception(
                        f"Port {port} is still occupied after {max_retries} retries. Exiting."
                    )
            else:
                print(f"Port {port} is not occupied, proceeding.")
                return  # 如果端口未被占用，继续执行后续操作
    except Exception as e:
        raise Exception(f"Error checking port {port}: {str(e)}")


def wait_for_port_to_be_listened(shell, port=5006, max_retries=10, wait_time=1):
    try:
        retries = 0
        while retries < max_retries:
            # 使用 netstat 命令检测5006端口是否监听
            shell.send(f"netstat -an | grep :{port} | grep LISTEN\n")
            time.sleep(1)
            output = shell.recv(65535).decode("utf-8")

            # 忽略命令回显，检查是否监听
            if f":{port}" in output and "LISTEN" in output:
                print(f"Port {port} is now being listened on. Proceeding.")
                return  # 端口已开始监听，退出函数
            else:
                print(
                    f"Port {port} is not yet listened on, retrying in {wait_time} seconds..."
                )
                retries += 1
                time.sleep(wait_time)  # 等待一段时间后重试
        raise Exception(
            f"Port {port} was not listened on after {max_retries} retries. Exiting."
        )
    except Exception as e:
        raise Exception(
            f"Error while waiting for port {port} to be listened on: {str(e)}"
        )


def start_switch(method):
    for i, switch in enumerate(switchSSHs):
        switch_dir = f"{SWITCH_HOME[i]}/netfetch-private/{method}"  # Assuming method is the same for all switches
        switch_dir_tofino = f"{switch_dir}/tofino"
        try:
            # Open an interactive shell session
            shell = switch.invoke_shell()
            time.sleep(1)

            # Sudo password input function
            def execute_with_sudo(command, sudo_password):
                shell.send(
                    f"echo {sudo_password} | sudo -SE {command}\n"
                )  # Preserve environment variables
                time.sleep(2)
                # shell.send(f"{sudo_password}\n")  # Send sudo password
                # time.sleep(1)
                output = shell.recv(65535).decode("utf-8")
                # print(output)
                return output

            # Function to run commands and handle sudo password manually
            def execute_command(command):
                shell.send(f"{command}\n")
                time.sleep(1)
                output = shell.recv(65535).decode("utf-8")
                # print(output)
                return output

            # Start 'bash start_switch.sh' with sudo -E and nohup for background execution
            print(f"[switch {SWITCH_IPs[i]}] Starting 'bash start_switch.sh' process")
            execute_command(f"cd {switch_dir_tofino}")
            execute_with_sudo(
                "nohup bash start_switch.sh > tmp_switch.out &", SWITCH_PWDs[i]
            )
            time.sleep(10)
            # Start 'bash configure.sh' (wait for it to complete)
            print(
                f"[switch {SWITCH_IPs[i]}] Starting 'bash configure.sh' process (waiting for completion)"
            )
            execute_command(f"cd {switch_dir_tofino}")
            execute_with_sudo(f"bash configure.sh", SWITCH_PWDs[i])
            time.sleep(6)  # Allow configure.sh to complete, adjust sleep if needed
            while not shell.recv_ready():
                time.sleep(1)  # Waiting until configure.sh completes
            output = shell.recv(65535).decode("utf-8")
            print(output)

            # If method is netcache or netfetch, start additional scripts
            if (
                method == "netcache"
                or method == "netfetch"
                or method == "netcachewithtoken"
                or method == "netfetch-design1"
                or method == "netfetch-design2"
                or method == "netfetch-design3"
                or method == "netfetch_single"
                or method == "csfletch"
                or method == "csfletchnew"
                or method == "fletchnew"
            ):
                # Start 'bash ptf_popserver.sh' with sudo -E
                print(
                    f"[switch {SWITCH_IPs[i]}] Starting 'bash ptf_popserver.sh' process"
                )
                check_port_occupied(shell, port=5006, max_retries=1, wait_time=5)
                execute_with_sudo(
                    "nohup bash ptf_popserver.sh > tmp_ptf_popserver.out &",
                    SWITCH_PWDs[i],
                )
                time.sleep(1)
                # Start './switchos' with sudo -E
                print(f"[switch {SWITCH_IPs[i]}] Starting './switchos' process")
                # if method == "netfetch_single":
                #     method = "netfetch"
                switch_dir = f"{SWITCH_HOME[i]}/netfetch-private/{method}"  # Assuming method is the same for all switches
        
                execute_command(f"cd {switch_dir}")
                execute_command("nohup stdbuf -o0 ./switchos > tmp_switchos.out &")
                # wait switchos ready
                wait_for_port_to_be_listened(
                    shell, port=5003, max_retries=5, wait_time=1
                )
                time.sleep(5)
        except Exception as e:
            print(f"[switch {SWITCH_IPs[i]}] Error: {str(e)}")
        finally:
            shell.close()  # Close the shell connection
        # while True:
        #     continue



