import sys
import time
import json
import os
import paho.mqtt.client as mqtt
import psutil
from PyQt5.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QHBoxLayout, QLabel, QWidget
from PyQt5.QtCore import QThread, pyqtSignal, QTimer
from PyQt5.QtGui import QImage
import pyqtgraph as pg
import numpy as np
from PIL import Image  # Librería para procesar el mapa de fondo

# --- CONFIGURACIÓN DE PARÁMETROS ---
TIEMPO_VIDA_VERDE = 3.0
TOTAL_SENSORES_CIUDAD = 50000

# Límites geográficos exactos para encajar el mapa de fondo
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
        self.resize(1100, 700)

        self.total_mensajes = 0
        self.tiempo_inicio = None
        self.latencias = []
        self.base_sensores = {}

        # --- DISEÑO DE LA INTERFAZ ---
        main_layout = QHBoxLayout()
        
        panel_izquierdo = QWidget()
        layout_metricas = QVBoxLayout()
        
        self.lbl_total_sensores = QLabel(f"Número Total Sensores: {TOTAL_SENSORES_CIUDAD}")
        self.lbl_mensajes_totales = QLabel("Mensajes Recibidos: 0")
        self.lbl_throughput = QLabel("Mensajes por Segundo: 0.00 msgs/s")
        self.lbl_latencia = QLabel("Latencia Promedio: 0.00 ms")
        self.lbl_activos = QLabel("Sensores Activos (Verdes): 0")
        self.lbl_conectados = QLabel("Sensores Conectados: 0")
        self.lbl_desconectados = QLabel(f"Sensores Desconectados: {TOTAL_SENSORES_CIUDAD}")
        self.lbl_cpu = QLabel("Uso de CPU: 0.0%")
        self.lbl_memoria = QLabel("Uso de Memoria: 0.0%")
        
        metricas_lista = [
            self.lbl_total_sensores, self.lbl_mensajes_totales, self.lbl_throughput, 
            self.lbl_latencia, self.lbl_activos, self.lbl_conectados, 
            self.lbl_desconectados, self.lbl_cpu, self.lbl_memoria
        ]
        for lbl in metricas_lista:
            lbl.setStyleSheet("font-size: 14px; font-weight: bold; padding: 5px; color: #333;")
            layout_metricas.addWidget(lbl)
            
        panel_izquierdo.setLayout(layout_metricas)
        main_layout.addWidget(panel_izquierdo, stretch=1)

        # Gráfico del mapa
        self.plot_widget = pg.PlotWidget()
        self.plot_widget.setBackground('k')
        self.plot_widget.setTitle("Nube de Puntos sobre Mapa de la Ciudad", color="w", size="12pt")
        self.plot_widget.setLabel('left', 'Latitud')
        self.plot_widget.setLabel('bottom', 'Longitud')
        
        # --- CARGAR MAPA DE FONDO EN PYQTGRAPH ---
        if os.path.exists("mapa_lima.png"):
            img = Image.open("mapa_lima.png").convert("RGBA")
            img_data = np.array(img)
            # Invertir los ejes para que coincida con la orientación del plano de PyQtGraph
            img_data = np.rot90(img_data, -1)
            img_item = pg.ImageItem(img_data)
            
            # Ajustar la imagen para que abarque exactamente nuestro rango geográfico
            rect = pg.QtCore.QRectF(LON_MIN, LAT_MIN, LON_MAX - LON_MIN, LAT_MAX - LAT_MIN)
            img_item.setRect(rect)
            self.plot_widget.addItem(img_item)
            
            # Fijar los límites iniciales de la vista
            self.plot_widget.setXRange(LON_MIN, LON_MAX)
            self.plot_widget.setYRange(LAT_MIN, LAT_MAX)
        
        # Capa superior: Nube de puntos de sensores
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

    def procesar_mensaje(self, data):
        if self.tiempo_inicio is None:
            self.tiempo_inicio = time.time()
            
        self.total_mensajes += 1
        
        sensor_id = data.get("id")
        lat = data.get("lat")
        lon = data.get("lon")
        ts_simulador = data.get("timestamp")
        ts_llegada = data.get("tiempo_llegada")
        
        if sensor_id and lat and lon:
            # Forzar el rango geográfico dentro de los límites del mapa para la simulación
            self.base_sensores[sensor_id] = {
                "pos": (lon, lat),
                "last_seen": time.time()
            }
            
            if ts_simulador:
                latencia = (ts_llegada - float(ts_simulador)) * 1000
                if latencia >= 0:
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
        
        for s_id, info in self.base_sensores.items():
            lon, lat = info["pos"]
            x_coords.append(lon)
            y_coords.append(lat)
            
            if ahora - info["last_seen"] < TIEMPO_VIDA_VERDE:
                sensores_activos_verdes += 1
                colores.append(pg.mkBrush(0, 255, 0, 255))
            else:
                colores.append(pg.mkBrush(128, 128, 128, 180)) # Gris intermitente con ligera opacidad

        sensores_conectados = len(self.base_sensores)
        sensores_desconectados = TOTAL_SENSORES_CIUDAD - sensores_conectados

        self.lbl_mensajes_totales.setText(f"Mensajes Recibidos: {self.total_mensajes}")
        self.lbl_throughput.setText(f"Mensajes por Segundo: {throughput:.2f} msgs/s")
        self.lbl_latencia.setText(f"Latencia Promedio: {latencia_prom:.2f} ms")
        self.lbl_activos.setText(f"Sensores Activos (Verdes): {sensores_activos_verdes}")
        self.lbl_conectados.setText(f"Sensores Conectados: {sensores_conectados}")
        self.lbl_desconectados.setText(f"Sensores Desconectados: {max(0, sensores_desconectados)}")
        
        self.lbl_cpu.setText(f"Uso de CPU: {psutil.cpu_percent()}%")
        self.lbl_memoria.setText(f"Uso de Memoria: {psutil.virtual_memory().percent}%")

        if x_coords:
            self.scatter.setData(x=x_coords, y=y_coords, brush=colores)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
