import socket
import json
import uuid
import threading


HOST = '127.0.0.1'
PORT = 5000

def listen(sock):
    buf = b''
    while True:
        data = sock.recv(4096)
        if not data:
            break
        buf += data
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            if not line.strip():
                continue
            try:
                obj = json.loads(line.decode('utf-8'))
                print('Respuesta:', obj)
            except Exception as e:
                print('Respuesta malformada:', line, e)

if __name__ == '__main__':
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))

    # hilo para recibir respuestas
    t = threading.Thread(target=listen, args=(s,), daemon=True)
    t.start()


    # enviar varias tareas
    for i in range(5):
        task = {
            'task_id': str(uuid.uuid4()),
            'action': 'sleep',
            'secs': 1 + i * 0.2
        }
        s.sendall((json.dumps(task) + '\n').encode('utf-8'))


    # pedir un factorial
    task = {'task_id': str(uuid.uuid4()), 'action': 'fact', 'n': 8}
    s.sendall((json.dumps(task) + '\n').encode('utf-8'))


    # mantener la conexión a la espera de respuestas
    try:
        while True:
            pass
    except KeyboardInterrupt:
        s.close()
        print('cliente cerrado')