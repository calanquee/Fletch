import paramiko
import time

from utils.client_util import *
from utils.server_util import *
from utils.switch_util import *
import os


def init_connections():
    for i, server in enumerate(serverSSHs):
        server.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        server.connect(
            SERVER_IPs[i],
            port=SERVER_PORTs[i],
            username=SERVER_USERs[i],
            password=SERVER_PWDs[i],
        )
        print(f"[server {i}] Successfully connected to server {SERVER_IPs[i]}")

    # for i, client in enumerate(clientSSHs):
    #     client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    #     client.connect(
    #         CLIENT_IPs[i],
    #         port=CLIENT_PORTs[i],
    #         username=CLIENT_USERs[i],
    #         password=CLIENT_PWDs[i],
    #     )
    #     print(f"[client {i}] Successfully connected to client {CLIENT_IPs[i]}")

    for i, switch in enumerate(switchSSHs):
        switch.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        switch.connect(
            SWITCH_IPs[i],
            port=SWITCH_PORTs[i],
            username=SWITCH_USERs[i],
            password=SERVER_PWDs[i],
        )
        print(f"[switch {i}] Successfully connected to switch {SWITCH_IPs[i]}")
def init_switch_connections():
    for i, switch in enumerate(switchSSHs):
        switch.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        switch.connect(
            SWITCH_IPs[i],
            port=SWITCH_PORTs[i],
            username=SWITCH_USERs[i],
            password=SERVER_PWDs[i],
        )
        print(f"[switch {i}] Successfully connected to switch {SWITCH_IPs[i]}")

def ssh_with_sudo(client, sudo_password):
    try:
        # 打开一个交互式shell会话
        shell = client.invoke_shell()
        # 等待shell准备好
        time.sleep(1)
        # 执行 sudo -i 命令
        shell.send("sudo -i\n")
        # 等待命令提示输入密码
        time.sleep(1)
        # 输入sudo密码
        shell.send(f"{sudo_password}\n")
        # 再等待一段时间，确保命令执行
        time.sleep(2)
        # 读取shell的输出
        output = shell.recv(65535).decode("utf-8")
        print(output)
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        # 关闭SSH连接
        # client.close()
        pass


def test_sudo():
    for i, server in enumerate(serverSSHs):
        ssh_with_sudo(server, SERVER_PWDs[i])

    for i, client in enumerate(clientSSHs):
        ssh_with_sudo(client, CLIENT_PWDs[i])

    for i, switch in enumerate(switchSSHs):
        ssh_with_sudo(switch, SWITCH_PWDs[i])


def close_all_connection():
    for i, server in enumerate(serverSSHs):
        server.close()

    for i, client in enumerate(clientSSHs):
        client.close()

    for i, switch in enumerate(switchSSHs):
        switch.close()


def stop_all():
    stop_switch()
    stop_servers()
    # stop_clients()


def start_testbed(method, depth=10):
    print(f"start {method}")
    start_servers(method, depth)
    start_switch(method)
    time.sleep(30)


def sync_file_to_server(
    toSSH, fromPath,toPath
):
    try:
        # 创建SFTP会话
        sftp = toSSH.open_sftp()
        remote_dir = os.path.dirname(toPath)
        try:
            sftp.stat(remote_dir)  
        except FileNotFoundError:
            sftp.mkdir(remote_dir) 

        # 将本地文件上传到远程服务器
        sftp.put(fromPath, toPath)
        # print(f"File {fromPath} successfully synced to {toPath}")
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        # 关闭SFTP
        sftp.close()


# init_connections()
# # test_sudo()
# # stop_servers()
# close_all_connection()
