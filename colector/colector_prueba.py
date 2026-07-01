import paho.mqtt.client as mqtt
import time

# Variables globales para métricas
contador_mensajes = 0
tiempo_inicio = None

def on_message(client, userdata, msg):
    global contador_mensajes, tiempo_inicio
    
    if tiempo_inicio is None:
        tiempo_inicio = time.time()
        
    contador_mensajes += 1
    
    # Cada 1000 mensajes recibidos, calculamos y mostramos el Throughput
    if contador_mensajes % 1000 == 0:
        tiempo_actual = time.time()
        duracion = tiempo_actual - tiempo_inicio
        throughput = contador_mensajes / duracion if duracion > 0 else 0
        print(f"[MÉTRICA] Mensajes procesados: {contador_mensajes} | Tiempo: {duracion:.2f}s | Throughput: {throughput:.2f} msgs/s")

client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
client.on_message = on_message

print("Conectando al broker Mosquitto para pruebas de rendimiento...")
client.connect("localhost", 1883, 60)
client.subscribe("ciudad/sensores/medicion")

print("Esperando ráfagas masivas... (Presiona Ctrl+C para detener)")
client.loop_forever()
