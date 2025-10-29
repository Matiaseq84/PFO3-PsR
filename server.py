import socket
import threading
import json
from concurrent.futures import ThreadPoolExecutor
import time

HOST = '0.0.0.0'
PORT = 5000
MAX_WORKERS = 8


executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

def process_task(payload):
    """Función que ejecuta la tarea"""
    task_id = payload.get('task_id')
    action = payload.get('action')
    # ejemplo: sleep
    if action == 'sleep':
        secs = float(payload.get('secs', 1))
        time.sleep(secs)
        return {'task_id': task_id, 'status':'ok', 'result': f'slept {secs}s'}
    # ejemplo: compute factorial (pequeño)
    if action == 'fact':
        n = int(payload.get('n', 1))
        res = 1
        for i in range(2, n+1):
            res *= i
        return {'task_id': task_id, 'status':'ok', 'result': res}
    return {'task_id': task_id, 'status':'error', 'error': 'unknown action'}

def handle_connection(conn, addr):
    """Lee líneas JSON del socket y responde con JSON por cada petición."""
    print(f'Conexión desde {addr}')
    with conn:
        buf = b''
        while True:
            data = conn.recv(4096)
            if not data:
                break
            buf += data
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line.decode('utf-8'))
                except Exception as e:
                    err = {'status':'error','error': 'malformed json', 'detail': str(e)}
                    conn.sendall((json.dumps(err) + '\n').encode('utf-8'))
                    continue


                # enviamos a threadpool y cuando esté listo, respondemos por socket
                # aquí usamos un callback que envía la respuesta por la misma conexión
                future = executor.submit(process_task, payload)


                def _send_result(fut):
                    try:
                        res = fut.result()
                    except Exception as e:
                        res = {'task_id': payload.get('task_id'), 'status':'error', 'error': str(e)}
                    try:
                        conn.sendall((json.dumps(res) + '\n').encode('utf-8'))
                    except Exception as e:
                        print('Error enviando respuesta:', e)


                future.add_done_callback(_send_result)


    print(f'Conexión cerrada {addr}')

def serve_forever():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))
    s.listen(100)
    print(f'Servidor escuchando en {HOST}:{PORT} (pool={MAX_WORKERS})')
    try:
        while True:
            conn, addr = s.accept()
            t = threading.Thread(target=handle_connection, args=(conn, addr), daemon=True)
            t.start()
    finally:
        s.close()

if __name__ == '__main__':
    serve_forever()