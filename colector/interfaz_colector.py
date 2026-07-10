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

# Coordenadas geográficas límites fijas para el mapa de Lima Metropolitana
LON_MIN, LON_MAX = -77.16, -76.96
LAT_MIN, LAT_MAX = -12.15, -11.95

class MqttWorker(QThread):
    
    batch_signal = pyqtSignal(list)

    def __init__(self):
        super().__init__()
        self.buffer = []
        self.last_flush = time.time()

    def run(self):
        def on_message(client, userdata, msg):
            try:
                tiempo_llegada = time.time()
                data = json.loads(msg.payload.decode())
                data["tiempo_llegada"] = tiempo_llegada
                self.buffer.append(data)
                
                
                if len(self.buffer) >= 20000 or (time.time() - self.last_flush) > 0.05:
                    self.batch_signal.emit(self.buffer)
                    self.buffer = []
                    self.last_flush = time.time()
            except Exception:
                pass

        client = mqtt.Client(client_id="Interfaz_Monitoreo_Unico",clean_session=True,callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        client.max_inflight_messages_set(1000000)
        client.max_queued_messages_set(1000000)
        
        client.on_message = on_message
        client.connect("localhost", 1883, 60)
        client.subscribe("ciudad/sensores/medicion")
        client.loop_forever()

    def purgar_cola_inmediata(self):
        self.buffer = []
        self.last_flush = time.time()

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Monitoreo:")
        self.resize(1150, 750)
        
        main_layout = QHBoxLayout()
        panel_izquierdo = QWidget()
        layout_metricas = QVBoxLayout()
        
        # Panel de 9 Métricas consolidadas con la nueva semántica de conexión
        self.lbl_mensajes_recibidos = QLabel("Mensajes Recibidos (UI): 0")
        self.lbl_throughput = QLabel("Mensajes por segundo: 0.00 msgs/s")
        self.lbl_latencia = QLabel("Latencia promedio: 0.00 ms")
        self.lbl_cpu = QLabel("Uso de CPU: 0.0%")
        self.lbl_memoria = QLabel("Uso de memoria: 0.0%")
        self.lbl_sensores_totales = QLabel("Número Total de Sensores: 0")
        self.lbl_activos = QLabel("Sensores Activos: 0")
        self.lbl_conectados = QLabel("Sensores conectados: 0")
        self.lbl_desconectados = QLabel("Sensores desconectados: 0")
        
        metricas_lista = [            
            self.lbl_mensajes_recibidos,
            self.lbl_throughput,
            self.lbl_latencia,
            self.lbl_cpu,
            self.lbl_memoria,
            self.lbl_sensores_totales,
            self.lbl_activos,
            self.lbl_conectados,
            self.lbl_desconectados
        ]
        
        for lbl in metricas_lista:
            lbl.setStyleSheet("font-size: 14px; font-weight: bold; padding: 6px; color: #333;")
            layout_metricas.addWidget(lbl)
            
        layout_metricas.addSpacing(25)
        self.btn_limpiar = QPushButton("Limpiar datos")
        self.btn_limpiar.setStyleSheet("background-color: #595959; color: white; font-size: 13px; font-weight: bold; border-radius: 5px; padding: 10px;")
        self.btn_limpiar.clicked.connect(self.limpiar_tablero)
        layout_metricas.addWidget(self.btn_limpiar)
        layout_metricas.addStretch()
        
        panel_izquierdo.setLayout(layout_metricas)
        main_layout.addWidget(panel_izquierdo, stretch=1)
        
        self.plot_widget = pg.PlotWidget()
        self.plot_widget.setBackground('k')
        self.plot_widget.setTitle("Distribución de Sensores", color="w", size="12pt")
        
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
        
        self.scatter = pg.ScatterPlotItem(size=4, symbol='s', pen=None)
        self.plot_widget.addItem(self.scatter)
        main_layout.addWidget(self.plot_widget, stretch=3)
        
        container = QWidget()
        container.setLayout(main_layout)
        self.setCentralWidget(container)
        
        # Umbrales temporales de la máquina de estados
        self.tiempo_vida_verde = 2.0  
        self.tiempo_vida_gris = 5.0   
        
        self.inicializar_variables_sistema()
        self.worker = MqttWorker()
        self.worker.batch_signal.connect(self.procesar_bloque_mensajes)
        self.worker.start()
        
        self.timer = QTimer()
        self.timer.timeout.connect(self.actualizar_interfaz)
        self.timer.start(50)

    def inicializar_variables_sistema(self):
        self.total_mensajes = 0
        self.tiempo_inicio = None
        self.ultimo_mensaje_ts = None
        self.latencias = []
        self.base_sensores = {}
        self.universo_total_configurado = 0

    def limpiar_tablero(self):
        if hasattr(self, 'worker') and self.worker:
            self.worker.purgar_cola_inmediata()
            
        self.inicializar_variables_sistema()
        self.scatter.clear()
        
        
        self.lbl_mensajes_recibidos.setText("Mensajes Recibidos: 0")
        self.lbl_throughput.setText("Mensajes por segundo: 0.00 msgs/s")
        self.lbl_latencia.setText("Latencia promedio: 0.00 ms")
        self.lbl_cpu.setText("Uso de CPU: 0.0%")
        self.lbl_memoria.setText("Uso de memoria: 0.0%")
        self.lbl_sensores_totales.setText("Número Total de Sensores: 0")
        self.lbl_activos.setText("Sensores Activos: 0")
        self.lbl_conectados.setText("Sensores conectados: 0")
        self.lbl_desconectados.setText("Sensores desconectados: 0")

    def procesar_bloque_mensajes(self, lista_datos):
        if not lista_datos:
            return
            
        if self.tiempo_inicio is None:
            self.tiempo_inicio = time.time()

        ahora = time.time()
        self.ultimo_mensaje_ts = ahora
        
        actualizaciones_locales = {}
        nuevas_latencias = []
        
        for item in lista_datos:
            self.total_mensajes += 1
            try:
                data = item
                sensor_id = data.get("sensor_id")
                if sensor_id:
                    actualizaciones_locales[sensor_id] = {
                        "pos": (data.get("x", 0), data.get("y", 0)),
                        "last_seen": ahora
                    }
                    
                ts_simulador = data.get("timestamp")
                ts_llegada = data.get("tiempo_llegada")
                if ts_simulador and ts_llegada:
                    latencia = (ts_llegada - float(ts_simulador)) * 1000
                    if 0 <= latencia < 5000:
                        nuevas_latencias.append(latencia)
            except Exception:
                pass

        if actualizaciones_locales:
            self.base_sensores.update(actualizaciones_locales)
        
        self.universo_total_configurado = max(self.universo_total_configurado, len(self.base_sensores))
        
        if nuevas_latencias:
            self.latencias.extend(nuevas_latencias)
            if len(self.latencias) > 10000:
                self.latencias = self.latencias[-10000:]

    def actualizar_interfaz(self):
        ahora = time.time()
        
        self.lbl_cpu.setText(f"Uso de CPU: {psutil.cpu_percent()}%")
        self.lbl_memoria.setText(f"Uso de memoria: {psutil.virtual_memory().percent}%")
        
        if self.tiempo_inicio is None:
            return
            
        if self.ultimo_mensaje_ts and (ahora - self.ultimo_mensaje_ts) > 2.0:
            throughput = 0.00
        else:
            duracion = ahora - self.tiempo_inicio
            throughput = self.total_mensajes / duracion if duracion > 0 else 0
        
        latencia_prom = np.mean(self.latencias) if self.latencias else 0.0
        
        sensores_activos = 0
        sensores_conectados = 0
        lons, lats, colores = [], [], []
        
        
        for s_id, info in list(self.base_sensores.items()):
            tiempo_transcurrido = ahora - info["last_seen"]
            
            if tiempo_transcurrido < self.tiempo_vida_verde:
                sensores_activos += 1
                sensores_conectados += 1
                lon_val, lat_val = info["pos"]
                lons.append(lon_val)
                lats.append(lat_val)
                colores.append((0, 255, 0, 255)) 
                
            
            elif tiempo_transcurrido < self.tiempo_vida_gris:
                sensores_conectados += 1 
                lon_val, lat_val = info["pos"]
                lons.append(lon_val)
                lats.append(lat_val)
                colores.append((168, 168, 168, 140)) 
                
            
        
        totales_universo = max(self.universo_total_configurado, len(self.base_sensores))
        
       
        sensores_desconectados = totales_universo - sensores_conectados

        
        self.lbl_mensajes_recibidos.setText(f"Mensajes Recibidos: {self.total_mensajes}")
        self.lbl_throughput.setText(f"Mensajes por segundo: {throughput:.2f} msgs/s")
        self.lbl_latencia.setText(f"Latencia promedio: {latencia_prom:.2f} ms")
        self.lbl_sensores_totales.setText(f"Número Total de Sensores: {totales_universo}")
        self.lbl_activos.setText(f"Sensores Activos: {sensores_activos}")
        self.lbl_conectados.setText(f"Sensores conectados: {sensores_conectados}")
        self.lbl_desconectados.setText(f"Sensores desconectados: {sensores_desconectados}")
        
        if lons and lats:
            np_x = np.array(lons, dtype=float)
            np_y = np.array(lats, dtype=float)
            brushes = [pg.mkBrush(*c) for c in colores]
            self.scatter.setData(x=np_x, y=np_y, brush=brushes)
        else:
            self.scatter.clear()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())