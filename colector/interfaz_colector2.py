import sys
import time
import json
import os
import threading
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

# Ventana deslizante para throughput/latencia: en vez de promediar desde el
# inicio del programa (lo que diluye cualquier cambio reciente), promediamos
# solo los últimos N segundos. Así un cambio en el simulador se refleja en
# pocos segundos, no en minutos.
VENTANA_METRICAS_SEG = 5.0


class MqttWorker(QThread):

    batch_signal = pyqtSignal(list)
    connection_signal = pyqtSignal(bool)  # True = conectado, False = perdido/caído

    def __init__(self):
        super().__init__()
        self.buffer = []
        self.last_flush = time.time()
        # CAMBIO: candado explícito para proteger self.buffer, que ahora se
        # toca desde dos hilos (el de MQTT vía on_message, y el de la GUI
        # vía purgar_cola_inmediata). Antes no había ninguna protección.
        self.lock = threading.Lock()

    def run(self):
        def on_connect(client, userdata, flags, reason_code, properties=None):
            self.connection_signal.emit(True)

        def on_disconnect(client, userdata, reason_code, properties=None):
            # CAMBIO: antes no había ningún aviso visible si el broker se
            # caía o se reiniciaba (p. ej. al aplicar un mosquitto.conf
            # nuevo). El dashboard seguía mostrando el último throughput
            # calculado, dando la falsa impresión de que "nada cambió".
            self.connection_signal.emit(False)

        def on_message(client, userdata, msg):
            try:
                tiempo_llegada = time.time()
                data = json.loads(msg.payload.decode())
                data["tiempo_llegada"] = tiempo_llegada
                with self.lock:
                    self.buffer.append(data)
                    listo_por_tamano = len(self.buffer) >= 20000
                    listo_por_tiempo = (time.time() - self.last_flush) > 0.05
                    if listo_por_tamano or listo_por_tiempo:
                        lote = self.buffer
                        self.buffer = []
                        self.last_flush = time.time()
                    else:
                        lote = None
                if lote:
                    self.batch_signal.emit(lote)
            except Exception:
                pass

        client = mqtt.Client(client_id="Interfaz_Monitoreo_Unico", clean_session=True,
                              callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        client.max_inflight_messages_set(1000000)
        client.max_queued_messages_set(1000000)

        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_message = on_message
        client.connect("localhost", 1883, 60)
        client.subscribe("ciudad/sensores/medicion")
        client.loop_forever()

    def purgar_cola_inmediata(self):
        with self.lock:
            self.buffer = []
            self.last_flush = time.time()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Monitoreo:")
        self.resize(1150, 800)

        main_layout = QHBoxLayout()
        panel_izquierdo = QWidget()
        layout_metricas = QVBoxLayout()

        self.lbl_conexion = QLabel("Broker: conectando...")
        self.lbl_mensajes_recibidos = QLabel("Mensajes Recibidos (UI): 0")
        self.lbl_throughput = QLabel(f"Mensajes por segundo (últimos {VENTANA_METRICAS_SEG:.0f}s): 0.00 msgs/s")
        self.lbl_latencia = QLabel("Latencia promedio: 0.00 ms")
        self.lbl_perdida = QLabel("Pérdida última ráfaga: N/D")
        self.lbl_cpu = QLabel("Uso de CPU: 0.0%")
        self.lbl_memoria = QLabel("Uso de memoria: 0.0%")
        self.lbl_sensores_totales = QLabel("Número Total de Sensores: 0")
        self.lbl_activos = QLabel("Sensores Activos: 0")
        self.lbl_conectados = QLabel("Sensores conectados: 0")
        self.lbl_desconectados = QLabel("Sensores desconectados: 0")

        metricas_lista = [
            self.lbl_conexion,
            self.lbl_mensajes_recibidos,
            self.lbl_throughput,
            self.lbl_latencia,
            self.lbl_perdida,
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

        self.tiempo_vida_verde = 2.0
        self.tiempo_vida_gris = 5.0

        self.inicializar_variables_sistema()
        self.worker = MqttWorker()
        self.worker.batch_signal.connect(self.procesar_bloque_mensajes)
        self.worker.connection_signal.connect(self.actualizar_estado_conexion)
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

        # CAMBIO: eventos con marca de tiempo para calcular throughput/latencia
        # sobre una ventana deslizante (últimos VENTANA_METRICAS_SEG segundos)
        # en vez de un promedio acumulado desde el arranque del dashboard.
        self.eventos_recientes = []  # lista de (timestamp_llegada, latencia_ms)

        # CAMBIO: conteo de mensajes recibidos por número de ráfaga, para
        # comparar contra "total_envio" (el total esperado que ya viaja en
        # el payload) y calcular una tasa de pérdida real, no solo un
        # contador de mensajes recibidos sin punto de referencia.
        self.mensajes_por_rafaga = {}
        self.ultima_rafaga_reportada = None

    def actualizar_estado_conexion(self, conectado):
        if conectado:
            self.lbl_conexion.setText("Broker: conectado")
            self.lbl_conexion.setStyleSheet("font-size: 14px; font-weight: bold; padding: 6px; color: green;")
        else:
            self.lbl_conexion.setText("Broker: DESCONECTADO")
            self.lbl_conexion.setStyleSheet("font-size: 14px; font-weight: bold; padding: 6px; color: red;")

    def limpiar_tablero(self):
        if hasattr(self, 'worker') and self.worker:
            self.worker.purgar_cola_inmediata()

        self.inicializar_variables_sistema()
        self.scatter.clear()

        self.lbl_mensajes_recibidos.setText("Mensajes Recibidos: 0")
        self.lbl_throughput.setText(f"Mensajes por segundo (últimos {VENTANA_METRICAS_SEG:.0f}s): 0.00 msgs/s")
        self.lbl_latencia.setText("Latencia promedio: 0.00 ms")
        self.lbl_perdida.setText("Pérdida última ráfaga: N/D")
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
                latencia = None
                if ts_simulador and ts_llegada:
                    latencia = (ts_llegada - float(ts_simulador)) * 1000
                    if not (0 <= latencia < 5000):
                        latencia = None

                self.eventos_recientes.append((ahora, latencia))

                # CAMBIO: conteo por ráfaga para calcular pérdida real.
                rafaga = data.get("rafaga")
                total_esperado = data.get("total_envio")
                if rafaga is not None:
                    entrada = self.mensajes_por_rafaga.setdefault(rafaga, {"recibidos": 0, "esperados": total_esperado})
                    entrada["recibidos"] += 1
                    if total_esperado is not None:
                        entrada["esperados"] = total_esperado
            except Exception:
                pass

        if actualizaciones_locales:
            self.base_sensores.update(actualizaciones_locales)

        self.universo_total_configurado = max(self.universo_total_configurado, len(self.base_sensores))

        # Poda de eventos y ráfagas viejas para no acumular memoria indefinidamente.
        limite = ahora - max(VENTANA_METRICAS_SEG * 4, 20.0)
        self.eventos_recientes = [e for e in self.eventos_recientes if e[0] >= limite]
        if len(self.mensajes_por_rafaga) > 50:
            claves_viejas = sorted(self.mensajes_por_rafaga.keys())[:-30]
            for k in claves_viejas:
                del self.mensajes_por_rafaga[k]

    def actualizar_interfaz(self):
        ahora = time.time()

        self.lbl_cpu.setText(f"Uso de CPU: {psutil.cpu_percent()}%")
        self.lbl_memoria.setText(f"Uso de memoria: {psutil.virtual_memory().percent}%")

        if self.tiempo_inicio is None:
            return

        # CAMBIO: throughput y latencia calculados solo sobre la ventana
        # deslizante reciente, no sobre todo el histórico. Un cambio real
        # en el simulador ahora se ve reflejado en segundos, no diluido
        # entre minutos de datos previos.
        limite_ventana = ahora - VENTANA_METRICAS_SEG
        eventos_ventana = [e for e in self.eventos_recientes if e[0] >= limite_ventana]

        if self.ultimo_mensaje_ts and (ahora - self.ultimo_mensaje_ts) > 2.0:
            throughput = 0.0
        else:
            throughput = len(eventos_ventana) / VENTANA_METRICAS_SEG

        latencias_ventana = [lat for (_, lat) in eventos_ventana if lat is not None]
        latencia_prom = np.mean(latencias_ventana) if latencias_ventana else 0.0

        # CAMBIO: pérdida real de la última ráfaga completa reportada,
        # comparando "recibidos" contra "esperados" (total_envio del payload).
        # Antes esta información simplemente no existía en el dashboard.
        if self.mensajes_por_rafaga:
            claves_ordenadas = sorted(self.mensajes_por_rafaga.keys())
            # Se ignora la última clave: puede seguir en curso, aún incompleta.
            candidatas = claves_ordenadas[:-1] if len(claves_ordenadas) > 1 else claves_ordenadas
            if candidatas:
                ultima_clave = candidatas[-1]
                info = self.mensajes_por_rafaga[ultima_clave]
                esperados = info["esperados"]
                recibidos = info["recibidos"]
                if esperados:
                    perdida_pct = max(0.0, (esperados - recibidos) / esperados * 100.0)
                    self.lbl_perdida.setText(
                        f"Pérdida ráfaga #{ultima_clave}: {perdida_pct:.2f}% ({recibidos}/{esperados})")

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
        self.lbl_throughput.setText(f"Mensajes por segundo (últimos {VENTANA_METRICAS_SEG:.0f}s): {throughput:.2f} msgs/s")
        self.lbl_latencia.setText(f"Latencia promedio (últimos {VENTANA_METRICAS_SEG:.0f}s): {latencia_prom:.2f} ms")
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
