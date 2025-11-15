# from utils.remote_util import clientSSHs, CLIENT_IPs, CLIENT_HOME
import paramiko
CLIENT_IPs = ["10.26.43.44"]
CLIENT_PWDs = [ "t9r57oh24i"]
CLIENT_PORTs = ["43053"]
CLIENT_USERs = ["jz"]
CLIENT_HOME = [
    f"/home/{CLIENT_USERs[0]}/In-Switch-FS-Metadata/"
]
clientSSHs = [paramiko.SSHClient()]

def stop_client(client, ip):
    try:
        stdin, stdout, stderr = client.exec_command("pkill -f './mdtest'")
        stdout.channel.recv_exit_status()
        print(f"[client {ip}] Killed './mdtest' process")
    except Exception as e:
        print(f"[client {ip}] Error: {str(e)}")


def stop_clients():
    for i, client in enumerate(clientSSHs):
        stop_client(client, CLIENT_IPs[i])
