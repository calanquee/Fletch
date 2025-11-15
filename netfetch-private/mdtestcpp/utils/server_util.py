# from utils.remote_util import serverSSHs, SERVER_IPs, SERVER_HOME
import paramiko

SERVER_IPs = ["10.26.43.163", "10.26.43.164"]
SERVER_PWDs = ["kjy27st9rj","rg43nto98t"]
SERVER_PORTs = ["43060", "7120"]
SERVER_USERs = ["jz", "jz"]

# Qingxiu: debug with one server
# SERVER_IPs = ["10.26.43.164"]
# SERVER_PWDs = ["rg43nto98t"]
# SERVER_PORTs = ["7120"]
# SERVER_USERs = ["jz"]

SERVER_HOME = []
serverSSHs = []
for i in range(len(SERVER_IPs)):
    SERVER_HOME.append(f"/home/{SERVER_USERs[i]}/In-Switch-FS-Metadata/")
    serverSSHs.append(paramiko.SSHClient())


    
def stop_server(server, ip):
    try:
        stdin, stdout, stderr = server.exec_command("pkill -9 -f './server [0-9]+'")
        stdout.channel.recv_exit_status()
        print(f"[server {ip}] Killed './server' process")

        stdin, stdout, stderr = server.exec_command("pkill -9 -f './controller'")
        stdout.channel.recv_exit_status()
        print(f"[server {ip}] Killed './controller' process")
    except Exception as e:
        print(f"[server {ip}] Error: {str(e)}")


def stop_servers():
    for i, server in enumerate(serverSSHs):
        stop_server(server, SERVER_IPs[i])


# def start_servers(method):
#     # method is name
#     for i, server in enumerate(serverSSHs):
#         server_dir = f"{SERVER_HOME[i]}/netfetch-private/{method}"
#         try:
#             # Start the './server {x}' process, assuming {x} is provided
#             start_cmd = f"nohup ./server {i} > tmp_server{i}.out &"
#             stdin, stdout, stderr = server.exec_command(f"cd {server_dir} && {start_cmd}")
#             stdout.channel.recv_exit_status()
#             print(f"[server {i}] Started './server {i}' process")

#             if i == 0:
#             # Start the './controller' process
#                 controller_cmd = "nohup ./controller > tmp_controller.out &"
#                 stdin, stdout, stderr = server.exec_command(f"cd {server_dir} && {controller_cmd}")
#                 stdout.channel.recv_exit_status()
#                 print(f"[server {SERVER_IPs[i]}] Started './controller' process")
#         except Exception as e:
#             print(f"[server {SERVER_IPs[i]}] Error: {str(e)}")
#         while True:
#             continue
import time


def start_servers(method, depth):
    # if method == "netfetch_single":
        # method = "netfetch"
    # method is name
    for i, server in enumerate(serverSSHs):
        server_dir = f"{SERVER_HOME[i]}/netfetch-private/{method}"
        if method == "cscaching" or method == "csfletch" or method == "csfletchnew":
            reload_rocksdb(server, depth)
        # time.sleep(10)
        try:
            # Open an interactive shell session
            shell = server.invoke_shell()
            time.sleep(1)  # Allow time for the shell to open

            # Function to execute commands
            def execute_command(command):
                shell.send(f"{command}\n")
                time.sleep(1)  # Wait for command to execute
                output = shell.recv(65535).decode("utf-8")
                print(output)
                return output

            # Change directory and start the './server {i}' process
            print(
                f"[server {i}] Changing directory to {server_dir} and starting './server {i}' process"
            )
            execute_command(f"cd {server_dir}")
            start_cmd = f"nohup stdbuf -o0 ./server {i} > tmp_server{i}.out 2>&1 &"
            # if i == 0:
            #     pass
            # else:
            #     execute_command(start_cmd)
            execute_command(start_cmd)
            print(f"[server {i}] Started './server {i}' process")

            if i == 0 and (
                method == "netcache"
                or method == "netfetch"
                or method == "netfetch-design1"
                or method == "netfetch-design2"
                or method == "netfetch-design3"
                or method == "netfetch_single"
                or method == "csfletch"
                or method == "csfletchnew"
                or method == "fletchnew"
            ):
                # Start the './controller' process
                print(f"[server {i}] Starting './controller' process")
                controller_cmd = "nohup stdbuf -o0 ./controller > tmp_controller.out 2>&1 &"
                execute_command(controller_cmd)
                print(f"[server {SERVER_IPs[i]}] Started './controller' process")

        except Exception as e:
            print(f"[server {SERVER_IPs[i]}] Error: {str(e)}")

        finally:
            # Ensure the shell is closed properly
            shell.close()

def reload_rocksdb(server, depth):
    # remove /tmp/nocache and /tmp/index
    # copy ~/db_bak/nocache and ~/db_bak/index to /tmp
    if depth not in [4, 6, 8, 10]:
        raise RuntimeError(
            f"depth {depth} is not supported for cscaching or csfletch"
        )
    try:
        stdin, stdout, stderr = server.exec_command("rm -rf /tmp/nocache")
        stdout.channel.recv_exit_status()
        print(f"[server] Removed '/tmp/nocache' directory")

        stdin, stdout, stderr = server.exec_command("rm -rf /tmp/index")
        stdout.channel.recv_exit_status()
        print(f"[server] Removed '/tmp/index' directory")

        stdin, stdout, stderr = server.exec_command("rm -rf ~/db_bak/nocache")
        stdout.channel.recv_exit_status()
        print(f"[server] Removed '~/db_bak/nocache' directory")

        stdin, stdout, stderr = server.exec_command("rm -rf ~/db_bak/index")
        stdout.channel.recv_exit_status()
        print(f"[server] Removed '~/db_bak/index' directory")

        stdin, stdout, stderr = server.exec_command(f"cp -r ~/exp6_bak/{depth}/nocache ~/db_bak")
        stdout.channel.recv_exit_status()
        print(f"[server] cp -r ~/exp6_bak/{depth}/nocache ~/db_bak")

        stdin, stdout, stderr = server.exec_command(f"cp -r ~/exp6_bak/{depth}/index ~/db_bak")
        stdout.channel.recv_exit_status()
        print(f"[server] cp -r ~/exp6_bak/{depth}/index ~/db_bak")

        stdin, stdout, stderr = server.exec_command("cp -r ~/db_bak/nocache /tmp")
        stdout.channel.recv_exit_status()
        print(f"[server] Copied '~/db_bak/nocache' to '/tmp'")

        stdin, stdout, stderr = server.exec_command("cp -r ~/db_bak/index /tmp")
        stdout.channel.recv_exit_status()
        print(f"[server] Copied '~/db_bak/index' to '/tmp'")
    except Exception as e:
        print(f"[server] Error: {str(e)}")