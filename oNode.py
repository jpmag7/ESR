import socket
import sys
import json
import threading
from tkinter import Tk
from PIL import Image, ImageTk
from tkinter import Label
import ast
from VideoStream import VideoStream
import time
import io
import os
import signal

CONNECTIONS_LIST = []
SERVER_SOCKET = None
BOOTSTRAP_TIMEOUT = 10000
NODE_TIMEOUT = 10
MY_IP = "127.0.0.1"
BOOTSTRAP_IP = "127.0.0.1"
JSON_FILE_NAME = "connections.json"
PORT = 5298
RUN = True
FRAME_COUNTER = 0
CLIENTS = {}
CLIENTS_SOCKETS = {}
VIDEO_FILE_NAME = "movie.Mjpeg"
LABEL = None
FULL_CONNECTIONS_LIST = {}
root = None
main_thread = None
is_streaming = False



def bootstraper():
	global CONNECTIONS_LIST
	global SERVER_SOCKET
	global FULL_CONNECTIONS_LIST

	f = open(JSON_FILE_NAME, "r")
	FULL_CONNECTIONS_LIST = json.load(f)
	CONNECTIONS_LIST = FULL_CONNECTIONS_LIST[MY_IP]
	f.close()

	while RUN:
		conn, addr = SERVER_SOCKET.accept()
		data = conn.recv(1024).decode()
		#t = threading.Thread(target=server_worker, args=(data,addr))
		#t.start()
		server_worker(data, addr)
		


def server_worker(data,addr):
	global CLIENTS
	global is_streaming
	global CLIENTS_SOCKETS

	if data.startswith("give_json"):
		print("Sending json")
		send_message((data[9:], PORT),"json" + str(FULL_CONNECTIONS_LIST[data[9:]]))

	elif data.startswith("removeclient:"):
		ip = data.split(":")[1]
		print("Removing", ip)
		if ip in CLIENTS.keys():
			CLIENTS.pop(ip)
			for c in CONNECTIONS_LIST: send_message((c, PORT), "removeclient:" + ip)

	elif data.startswith("removenode:"):
		ip = data.split(":")[1]
		print("Removing", ip)
		if ip in CLIENTS_SOCKETS.keys():
			CLIENTS_SOCKETS.pop(ip)
			for c in CONNECTIONS_LIST: send_message((c, PORT), "removenode:" + ip)

	elif data.startswith("client:"):
		client = update_client(data[7:])
		CLIENTS[client]["active"] = True
		send_message_thread((CLIENTS[client]["node"],PORT), "activate_client:" + str(client))
		time.sleep(0.5)
		if not CLIENTS[client]["node"] in list(CLIENTS_SOCKETS.keys()):
			cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			cs.connect((CLIENTS[client]["node"],PORT))
			CLIENTS_SOCKETS[CLIENTS[client]["node"]] = cs
			print("[+] Starting stream to:", CLIENTS[client]["node"])
			if not is_streaming:
				is_streaming = True
				threading.Thread(target=start_stream).start()
	else:
		print("Unknown message")



def start_node():
	global CONNECTIONS_LIST
	global SERVER_SOCKET

	send_message((BOOTSTRAP_IP, PORT), "give_json" + MY_IP)

	while RUN:
		conn, addr = SERVER_SOCKET.accept()
		data = conn.recv(20480)
		t = threading.Thread(target=node_worker, args=(conn, data,addr))
		t.start()



def node_worker(conn, data,addr):
	global CONNECTIONS_LIST
	global CLIENTS_SOCKETS
	global CLIENTS
	if "activate_client:".encode() in data:
		data=data.decode()
		print("activate_client")
		activate_client_route(data[16:])

	elif "removeclient:".encode() in data:
		ip = data.decode().split(":")[1]
		print("Removing", ip)
		if ip in CLIENTS.keys():
			CLIENTS.pop(ip)
			for c in CONNECTIONS_LIST: send_message((c, PORT), "removeclient:" + ip)

	elif "removenode:".encode() in data:
		ip = data.decode().split(":")[1]
		print("Removing", ip)
		if ip in CLIENTS_SOCKETS.keys():
			CLIENTS_SOCKETS.pop(ip)
			for c in CONNECTIONS_LIST: send_message((c, PORT), "removenode:" + ip)

	elif "client:".encode() in data:
		data=data.decode()
		print("Client")
		client = update_client(data[7:])
		if not client: return
		print("start backprop_clients")
		backprop_clients(addr[0])

	elif "json".encode() in data:
		data=data.decode()
		CONNECTIONS_LIST = ast.literal_eval(data[4:])
		print(CONNECTIONS_LIST)
	
	elif 115 == data[0]:
		seq_num = int(data[1:2])
		print("Received packet:", seq_num)
		retransmit_stream(seq_num, conn, data)

	else:
		print("Unknown message")


def start_client():
	global CONNECTIONS_LIST
	global SERVER_SOCKET


	send_message((BOOTSTRAP_IP, PORT), "removeclient:" + MY_IP)
	time.sleep(0.5)
	send_message((BOOTSTRAP_IP, PORT), "client:" + str({"reset": False, "node": MY_IP, "client": MY_IP, "dist": 1}))

	while RUN:
		conn, addr = SERVER_SOCKET.accept()
		print("Connection accepted:", addr)
		data = conn.recv(20480)
		t = threading.Thread(target=client_worker, args=(conn, data))
		t.start()
	


def client_worker(conn, data):
	if 115 == data[0]:
		seq_num = int(data[1:6])
		print_frame(seq_num, data[7:], conn)
	elif "activate_client:".encode() in data:
		pass
	else:
		print("Unknown message", data)


def print_frame(seq_num, frame, conn):
	global FRAME_COUNTER
	global root
	global LABEL
	while frame:
		if seq_num > FRAME_COUNTER or seq_num == 1:
			try:
				FRAME_COUNTER = seq_num
				image = Image.open(io.BytesIO(frame))
				photo = ImageTk.PhotoImage(image)
				LABEL.configure(image = photo, height=288) 
				LABEL.image = photo
				LABEL.pack()
				root.update()
			except:
				print("Break")
				send_message((BOOTSTRAP_IP, PORT), "client:" + str({"reset": True, "node": MY_IP, "client": MY_IP, "dist": 1}))
				return
		else: print("[*] Received out of order frame")
		try:
			conn.sendall(b"tick")
		except:
			print("Break")
			send_message((BOOTSTRAP_IP, PORT), "client:" + str({"reset": True, "node": MY_IP, "client": MY_IP, "dist": 1}))
			return
		try:
			data = conn.recv(20480)
		except:
			print("Break")
			send_message((BOOTSTRAP_IP, PORT), "client:" + str({"reset": True, "node": MY_IP, "client": MY_IP, "dist": 1}))
			return
		try:
			seq_num = int(data[1:6])
		except: pass
		frame = data[7:]



def reset_clients():
	global CLIENTS_SOCKETS
	toremove = []
	print("Reseting clients")
	for ck in CLIENTS_SOCKETS:
		toremove.append(ck)
	for ck in toremove:
		CLIENTS_SOCKETS[ck].close()
		CLIENTS_SOCKETS.pop(ck)




def retransmit_stream(seq_num, conn, data):
	global FRAME_COUNTER
	global CLIENTS
	if seq_num < FRAME_COUNTER: return
	FRAME_COUNTER = seq_num
	print("[+] Starting stream to", list(CLIENTS_SOCKETS.keys()))
	while data:
		toremove = []
		i = 0
		while i < len(CLIENTS_SOCKETS):
			ck = list(CLIENTS_SOCKETS.keys())[i]
			try:
				cs = CLIENTS_SOCKETS[ck]
				cs.sendall(data)
			except:
				toremove.append(ck)
				i = i + 1
				continue
			try:
				conn.sendall(b"tick")
			except:
				reset_clients()
				return
			i = i + 1

		for tr in toremove:
			
			if tr in CLIENTS.keys():
				CLIENTS.pop(tr)
				for a in CONNECTIONS_LIST: send_message((a, PORT), "removeclient:" + tr)
			
			toremoveclients = []
			for c in CLIENTS:
				if CLIENTS[c]["node"] == tr:
					toremoveclients.append(c)
			for c in toremoveclients:
				CLIENTS.pop(c)

			if tr in CLIENTS_SOCKETS.keys():
				CLIENTS_SOCKETS[tr].close()
				CLIENTS_SOCKETS.pop(tr)

			print("[-] Stopped streaming to", tr)
		if len(CLIENTS_SOCKETS) == 0:
			conn.close()
			return
		try:
			data = conn.recv(20480)
		except:
			reset_clients()
			return



def start_stream():
	global CLIENTS_SOCKETS
	global CLIENTS
	global is_streaming

	stream = VideoStream(VIDEO_FILE_NAME)
	frame = stream.nextFrame()

	while frame:
		seq_num = stream.frameNbr()
		toremoveck = []
		i = 0
		while i < len(CLIENTS_SOCKETS):
			ck = list(CLIENTS_SOCKETS.keys())[i]
			try:
				s = CLIENTS_SOCKETS[ck]
				s.sendall(("s" + str(seq_num).zfill(5) + "s").encode() + frame)
			except:
				toremoveclients = []
				for c in CLIENTS:
					if CLIENTS[c]["node"] == ck:
						toremoveclients.append(c)
				for c in toremoveclients: CLIENTS.pop(c)
				toremoveck.append(ck)
			i = i + 1
		for ck in toremoveck:
			print("[-] Stopped streaming to", ck)
			CLIENTS_SOCKETS.pop(ck)
		if len(CLIENTS_SOCKETS) == 0:
			is_streaming = False
			return
		frame = stream.nextFrame()
		time.sleep(0.05)



def update_client(data):
	global CLIENTS
	
	data = data.replace("True", "'True'").replace("False", "'False'")
	data = json.loads(data.replace("'",'"'))
	addr = data["node"]
	
	reset = False
	if data["reset"] == "True": reset = True

	if data["dist"] > 10: return None
	
	if not data["client"] in CLIENTS or reset:
		CLIENTS[data["client"]] = {"dist":data["dist"], "node":addr, "backproped":False, "active":False, "reset": reset}
	
	#if CLIENTS[data["client"]]["dist"] > data["dist"]:
	#	CLIENTS[data["client"]] = {"dist":data["dist"], "node":addr, "backproped":False, "active":False, "reset": False}
	
	return data["client"]



def backprop_clients(addr):
	global CONNECTIONS_LIST
	global CLIENTS

	for ck in CLIENTS:
		c = CLIENTS[ck]
		if not c["backproped"]:
			for n in CONNECTIONS_LIST:
				if n != c["node"] and n != ck and n != addr:
					send_message((n,PORT), "client:" + str({"reset": c["reset"], "node":MY_IP, "client":ck, "dist":c["dist"] + 1}))
			c["backproped"] = True
			c["reset"] = False


def activate_client_route(client):
	global CLIENTS
	global CLIENTS_SOCKETS

	c = CLIENTS[client]
	if not c["active"]:
		c["active"] = True
		send_message_thread((c["node"],PORT), "activate_client:" + str(client))
		if not c["node"] in list(CLIENTS_SOCKETS.keys()):
			cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			cs.connect((c["node"],PORT))
			CLIENTS_SOCKETS[c["node"]] = cs


def send_message(addr, msg, wait=False):
	if not wait:
		t = threading.Thread(target=send_message_thread, args=(addr, msg))
		t.start()
	else: send_message_thread(addr, msg)


def send_message_thread(addr, msg):
	global CONNECTIONS_LIST
	global CLIENTS_SOCKETS
	try:
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect(addr)	
		s.sendall(msg.encode())
		s.close()
	except:
		try:
			CLIENTS_SOCKETS.pop(addr[0])
			CONNECTIONS_LIST.remove(addr[0])
			if addr[0] in CLIENTS.keys():
				CLIENTS.pop(addr[0])
				send_message(addr, "removeclient:" + addr[0])
			else: send_message(addr, "removenode:" + addr[0])
		except: pass


def init_server():
	global SERVER_SOCKET
	global PORT

	SERVER_SOCKET = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	SERVER_SOCKET.bind((MY_IP, PORT))
	SERVER_SOCKET.listen()



def close():
	print("Quiting")
	os._exit(0)
	if main_thread:
		while main_thread.is_alive():
			main_thread.join(1)



def main():
	global LABEL	
	global MY_IP
	global JSON_FILE_NAME
	global BOOTSTRAP_IP
	global root
	global main_thread


	if len(sys.argv) < 3:
		print("Invalid arguments")
		return 	
	
	MY_IP = sys.argv[1]

	JSON_FILE_NAME = sys.argv[-1]
	if JSON_FILE_NAME.endswith(".json"):
		init_server()
		bootstraper()
	elif not "-c" in  sys.argv:
		BOOTSTRAP_IP = sys.argv[2]
		init_server()
		start_node()
	else:
		BOOTSTRAP_IP = sys.argv[2]
		root = Tk()
		LABEL = Label(root, height=19)
		init_server()
		main_thread = threading.Thread(target=start_client).start()
		root.protocol("WM_DELETE_WINDOW", close)
		root.mainloop()


if __name__ == '__main__':
	main()