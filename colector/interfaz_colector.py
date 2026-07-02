import sys
import time
import json
import os
import paho.mqtt.client as mqtt
import psutil
from PyQt5.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QHBoxLayout, QLabel, QWidget, QPushButton
from PyQt5.QtCore import QThread, pyqtSignal, QTimer
import pyqtgraph as pg
import numpy as np
from PIL import Image

LON_MIN, LON_MAX = -77.16, -76.96
LAT_MIN, LAT_MAX = -12.15, -11.95

class MqttWorker(QThread):
    msg_signal = pyqtSignal(dict)

    def run(self):
        def on_message(client, userdata, msg):
            try:
                tiempo_llegada = time.time()
                data = json.loads(msg.payload.decode())
                data["tiempo_llegada"] = tiempo_llegada
                self.msg_signal.emit(data)
            except Exception:
                pass

        client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        client.on_message = on_message
        client.connect("localhost", 1883, 60)
        client.subscribe("ciudad/sensores/medicion")
        client.loop_forever()

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Monitoreo HPC - Control de Ciudad Inteligente con Mapa")
        self.resize(1100, 750)

        # Inicializar variables de estado (Centralizadas para poder resetearlas)
        self.limpiar_tablero()

        main_layout = QHBoxLayout()
        panel_izquierdo = QWidget()
        layout_metricas = QVBoxLayout()
        
        self.lbl_total_sensores = QLabel("Carga Esperada: Esperando ráfaga...")
        self.lbl_mensajes_enviados = QLabel("Mensajes Enviados (C): 0")
        self.lbl_mensajes_totales = QLabel("Mensajes Recibidos (Python): 0")
        self.lbl_mensajes_perdidos = QLabel("Mensajes Perdidos: 0")
        self.lbl_throughput = QLabel("Mensajes por Segundo: 0.00 msgs/s")
        self.lbl_latencia = QLabel("Latencia Promedio: 0.00 ms")
        self.lbl_activos = QLabel("Sensores Activos (Verdes): 0")
        self.lbl_cpu = QLabel("Uso de CPU: 0.0%")
        self.lbl_memoria = QLabel("Uso de Memoria: 0.0%")
        
        metricas_lista = [
            self.lbl_total_sensores, self.lbl_mensajes_enviados, self.lbl_mensajes_totales, 
            self.lbl_mensajes_perdidos, self.lbl_throughput, self.lbl_latencia, 
            self.lbl_activos, self.lbl_cpu, self.lbl_memoria
        ]
        for lbl in metricas_lista:
            lbl.setStyleSheet("font-size: 14px; font-weight: bold; padding: 4px; color: #333;")
            layout_metricas.addWidget(lbl)
            
        # --- BOTÓN DE LIMPIEZA MANUAL ---
        layout_metricas.addSpacing(20) # Espacio visual
        self.btn_limpiar = QPushButton("Limpiar datos")
        self.btn_limpiar.setStyleSheet("""
            QPushButton {
                background-color: #595959;
                color: white;
                font-size: 13px;
                font-weight: bold;
                border-radius: 5px;
                padding: 10px;
            }
            QPushButton:hover {
                background-color: #c9302c;
            }
            QPushButton:pressed {
                background-color: #ac2925;
            }
        """)
        # Conectar el clic del botón a la función de reseteo
        self.btn_limpiar.clicked.connect(self.limpiar_tablero)
        layout_metricas.addWidget(self.btn_limpiar)
        layout_metricas.addStretch() # Empuja todo hacia arriba
            
        panel_izquierdo.setLayout(layout_metricas)
        main_layout.addWidget(panel_izquierdo, stretch=1)

        # --- PANEL DERECHO (MAPA) ---
        self.plot_widget = pg.PlotWidget()
        self.plot_widget.setBackground('k')
        self.plot_widget.setTitle("Nube de Puntos sobre Mapa de la Ciudad", color="w", size="12pt")
        
        if os.path.exists("mapa_lima.png"):
            img = Image.open("mapa_lima.png").convert("RGBA")
            img_data = np.array(img)
            img_data = np.rot90(img_data, -1)
            img_item = pg.ImageItem(img_data)
            rect = pg.QtCore.QRectF(LON_MIN, LAT_MIN, LON_MAX - LON_MIN, LAT_MAX - LAT_MIN)
            img_item.setRect(rect)
            self.plot_widget.addItem(img_item)
            self.plot_widget.setXRange(LON_MIN, LON_MAX)
            self.plot_widget.setYRange(LAT_MIN, LAT_MAX)
        
        self.scatter = pg.ScatterPlotItem(size=8)
        self.plot_widget.addItem(self.scatter)
        main_layout.addWidget(self.plot_widget, stretch=3)

        container = QWidget()
        container.setLayout(main_layout)
        self.setCentralWidget(container)

        self.worker = MqttWorker()
        self.worker.msg_signal.connect(self.procesar_mensaje)
        self.worker.start()

        self.timer = QTimer()
        self.timer.timeout.connect(self.actualizar_interfaz)
        self.timer.start(100)

    def limpiar_tablero(self):
        """ Restablece todas las métricas y estructuras de datos a cero """
        self.total_mensajes = 0
        self.mensajes_enviados_simulador = 0
        self.tiempo_inicio = None
        self.latencias = []
        self.base_sensores = {}
        self.tiempo_vida_verde = 3.0
        self.tiempo_vida_total = 5.0
        print("[INTERFAZ] Tablero de control restablecido. Listo para nueva carga HPC.")

    def procesar_mensaje(self, data):
        # DETECCIÓN AUTOMÁTICA (Seguridad): Si llega una ráfaga nueva y el tablero tiene datos viejos completos, limpia solo
        total_envio_actual = data.get("total_envio", 50000)
        if self.mensajes_enviados_simulador > 0 and self.total_mensajes >= self.mensajes_enviados_simulador:
            # Si entran datos nuevos habiendo terminado la simulación anterior, auto-reiniciar
            self.limpiar_tablero()

        if self.tiempo_inicio is None:
            self.tiempo_inicio = time.time()
            
        self.total_mensajes += 1
        
        self.mensajes_enviados_simulador = total_envio_actual
        intervalo = data.get("intervalo", 5)
        
        self.tiempo_vida_verde = float(intervalo)
        self.tiempo_vida_total = self.tiempo_vida_verde + 2.0
        
        sensor_id = data.get("id")
        lat = data.get("lat")
        lon = data.get("lon")
        ts_simulador = data.get("timestamp")
        ts_llegada = data.get("tiempo_llegada")
        
        if sensor_id and lat and lon:
            self.base_sensores[sensor_id] = {
                "pos": (lon, lat),
                "last_seen": time.time()
            }
            
            if ts_simulador:
                latencia = (ts_llegada - float(ts_simulador)) * 1000
                if 0 <= latencia < 10000:
                    self.latencias.append(latencia)
                    if len(self.latencias) > 500:
                        self.latencias.pop(0)

    def actualizar_interfaz(self):
        ahora = time.time()
        
        if self.tiempo_inicio:
            duracion = ahora - self.tiempo_inicio
            throughput = self.total_mensajes / duracion if duracion > 0 else 0
        else:
            throughput = 0
            
        latencia_prom = np.mean(self.latencias) if self.latencias else 0.0
        
        sensores_activos_verdes = 0
        x_coords = []
        y_coords = []
        colores = []
        sensores_a_eliminar = []

        for s_id, info in list(self.base_sensores.items()):
            tiempo_transcurrido = ahora - info["last_seen"]
            if tiempo_transcurrido >= self.tiempo_vida_total:
                sensores_a_eliminar.append(s_id)
                continue
                
            lon, lat = info["pos"]
            x_coords.append(lon)
            y_coords.append(lat)
            
            if tiempo_transcurrido < self.tiempo_vida_verde:
                sensores_activos_verdes += 1
                colores.append(pg.mkBrush(0, 255, 0, 255))
            else:
                colores.append(pg.mkBrush(128, 128, 128, 140))

        for s_id in sensores_a_eliminar:
            del self.base_sensores[s_id]

        perdidos = max(0, self.mensajes_enviados_simulador - self.total_mensajes) if self.mensajes_enviados_simulador > 0 else 0

        # Si el tablero está vacío (recién limpiado), mostrar textos iniciales limpios
        if self.mensajes_enviados_simulador == 0:
            self.lbl_total_sensores.setText("Carga Esperada: Esperando ráfaga...")
            self.lbl_mensajes_enviados.setText("Mensajes Enviados (C): 0")
            self.lbl_mensajes_totales.setText("Mensajes Recibidos (Python): 0")
            self.lbl_mensajes_perdidos.setText("Mensajes Perdidos: 0")
            self.lbl_throughput.setText("Mensajes por Segundo: 0.00 msgs/s")
            self.lbl_latencia.setText("Latencia Promedio: 0.00 ms")
            self.lbl_activos.setText("Sensores Activos (Verdes): 0")
        else:
            self.lbl_total_sensores.setText(f"Carga Esperada: {self.mensajes_enviados_simulador} sensores")
            self.lbl_mensajes_enviados.setText(f"Mensajes Enviados (C): {self.mensajes_enviados_simulador}")
            self.lbl_mensajes_totales.setText(f"Mensajes Recibidos (Python): {self.total_mensajes}")
            self.lbl_mensajes_perdidos.setText(f"Mensajes Perdidos: {perdidos}")
            self.lbl_throughput.setText(f"Mensajes por Segundo: {throughput:.2f} msgs/s")
            self.lbl_latencia.setText(f"Latencia Promedio: {latencia_prom:.2f} ms")
            self.lbl_activos.setText(f"Sensores Activos (Verdes): {sensores_activos_verdes}")
        
        self.lbl_cpu.setText(f"Uso de CPU: {psutil.cpu_percent()}%")
        self.lbl_memoria.setText(f"Uso de Memoria: {psutil.virtual_memory().percent}%")

        if x_coords:
            self.scatter.setData(x=x_coords, y=y_coords, brush=colores)
        else:
            self.scatter.clear()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
