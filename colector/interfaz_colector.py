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

# Ventana deslizante para throughput/latencia
VENTANA_METRICAS_SEG = 5.0


def fmt_int(n):
    return f"{n:,}".replace(",", " ")


def fmt_float(f, decimals):
    return f"{f:,.{decimals}f}".replace(",", " ")


class MqttWorker(QThread):
    batch_signal = pyqtSignal(list)
    connection_signal = pyqtSignal(bool)  # True = conectado, False = perdido/caído

    def __init__(self):
        super().__init__()
        self.buffer = []
        self.last_flush = time.time()
        self.lock = threading.Lock()

    def run(self):
        def on_connect(client, userdata, flags, reason_code, properties=None):
            self.connection_signal.emit(True)

        def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
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

        estilo_seccion = "font-size: 11px; font-weight: bold; color: #888; padding: 10px 6px 2px 6px; border-bottom: 1px solid #ddd;"
        estilo_label = "font-size: 14px; font-weight: bold; padding: 4px 6px; color: #333;"

        def seccion(texto):
            lbl = QLabel(texto)
            lbl.setStyleSheet(estilo_seccion)
            return lbl

        # ── ESTADO DEL SISTEMA ──
        layout_metricas.addWidget(seccion("━━━ ESTADO DEL SISTEMA ━━━"))
        self.lbl_conexion = QLabel("Broker MQTT: conectando...")
        self.lbl_cpu = QLabel("CPU Colector (Máx): 0.0 %")
        self.lbl_memoria = QLabel("Memoria Colector (Máx): 0.0 %")
        for lbl in (self.lbl_conexion, self.lbl_cpu, self.lbl_memoria):
            lbl.setStyleSheet(estilo_label)
            layout_metricas.addWidget(lbl)

        # ── RENDIMIENTO MQTT ──
        layout_metricas.addWidget(seccion("━━━ RENDIMIENTO MQTT ━━━"))
        self.lbl_mensajes_recibidos = QLabel("Mensajes recibidos: 0")
        self.lbl_throughput = QLabel("Throughput (Máx): 0.0 msgs/s")
        self.lbl_latencia = QLabel("Latencia promedio (Máx): 0.0 ms")
        for lbl in (self.lbl_mensajes_recibidos, self.lbl_throughput, self.lbl_latencia):
            lbl.setStyleSheet(estilo_label)
            layout_metricas.addWidget(lbl)

        # ── SENSORES ──
        layout_metricas.addWidget(seccion("━━━ SENSORES ━━━"))
        self.lbl_sensores_totales = QLabel("Sensores configurados: 0")
        self.lbl_activos = QLabel("Sensores activos: 0")
        self.lbl_inactivos = QLabel("Sensores inactivos: 0")
        self.lbl_apagados = QLabel("Sensores apagados: 0")
        for lbl in (self.lbl_sensores_totales, self.lbl_activos, self.lbl_inactivos, self.lbl_apagados):
            lbl.setStyleSheet(estilo_label)
            layout_metricas.addWidget(lbl)

        # ── RÁFAGAS ──
        layout_metricas.addWidget(seccion("━━━ RÁFAGAS ━━━"))
        self.lbl_rafaga = QLabel("Ráfaga #0 (—)")
        self.lbl_total_esperado = QLabel("Total esperado: 0")
        self.lbl_perdida = QLabel("Pérdida global: N/D")
        for lbl in (self.lbl_rafaga, self.lbl_total_esperado, self.lbl_perdida):
            lbl.setStyleSheet(estilo_label)
            layout_metricas.addWidget(lbl)

        layout_metricas.addSpacing(20)
        self.btn_limpiar = QPushButton("Limpiar datos")
        self.btn_limpiar.setStyleSheet("background-color: #595959; color: white; font-size: 13px; font-weight: bold; border-radius: 5px; padding: 10px;")
        self.btn_limpiar.clicked.connect(self.limpiar_tablero)
        layout_metricas.addWidget(self.btn_limpiar)
        layout_metricas.addStretch()

        panel_izquierdo.setLayout(layout_metricas)
        main_layout.addWidget(panel_izquierdo, stretch=1)

        self.plot_widget = pg.PlotWidget()
        self.plot_widget.setBackground('k')
        #self.plot_widget.setTitle("Distribución de Sensores", color="w", size="12pt")

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

        self.scatter = pg.ScatterPlotItem(size=3, symbol='s', pen=None)
        self.plot_widget.addItem(self.scatter)
        main_layout.addWidget(self.plot_widget, stretch=3)

        container = QWidget()
        container.setLayout(main_layout)
        self.setCentralWidget(container)

        self.tiempo_verde = 2.0  
        self.tiempo_plomo = 5.0  

        self.inicializar_variables_sistema()
        self.worker = MqttWorker()
        self.worker.batch_signal.connect(self.procesar_bloque_mensajes)
        self.worker.connection_signal.connect(self.actualizar_estado_conexion)
        self.worker.start()

        self.timer = QTimer()
        self.timer.timeout.connect(self.actualizar_interfaz)
        self.timer.start(100)

    def inicializar_variables_sistema(self):
        self.total_mensajes = 0
        self.tiempo_inicio = None
        self.ultimo_mensaje_ts = None
        self.base_sensores = {}
        self.total_sensores_config = 0

        self.max_throughput = 0.0
        self.max_latencia = 0.0
        self.max_cpu = 0.0
        self.max_memoria = 0.0

        self.eventos_recientes = []
        self.mensajes_por_rafaga = {}
        self.ultima_rafaga_reportada = None
        self.rafaga_actual = 0
        self.intervalo_config = 0
        self.timestamps_por_rafaga = {}

    def actualizar_estado_conexion(self, conectado):
        if conectado:
            self.lbl_conexion.setText("Broker MQTT: Conectado")
            self.lbl_conexion.setStyleSheet("font-size: 14px; font-weight: bold; padding: 6px; color: green;")
        else:
            self.lbl_conexion.setText("Broker MQTT: DESCONECTADO")
            self.lbl_conexion.setStyleSheet("font-size: 14px; font-weight: bold; padding: 6px; color: red;")

    def limpiar_tablero(self):
        if hasattr(self, 'worker') and self.worker:
            self.worker.purgar_cola_inmediata()

        self.inicializar_variables_sistema()
        self.scatter.clear()

        self.lbl_mensajes_recibidos.setText("Mensajes recibidos: 0")
        self.lbl_throughput.setText("Throughput (Máx): 0.0 msgs/s")
        self.lbl_latencia.setText("Latencia promedio (Máx): 0.0 ms")
        self.lbl_cpu.setText("CPU Colector (Máx): 0.0 %")
        self.lbl_memoria.setText("Memoria Colector (Máx): 0.0 %")
        self.lbl_sensores_totales.setText("Sensores configurados: 0")
        self.lbl_activos.setText("Sensores activos: 0")
        self.lbl_inactivos.setText("Sensores inactivos: 0")
        self.lbl_apagados.setText("Sensores apagados: 0")
        self.lbl_rafaga.setText("Ráfaga #0 (—)")
        self.lbl_total_esperado.setText("Total esperado: 0")
        self.lbl_perdida.setText("Pérdida global: N/D")

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

                rafaga = data.get("rafaga")
                if rafaga is not None and sensor_id is not None:
                    self.mensajes_por_rafaga.setdefault(rafaga, set()).add(sensor_id)

                total_envio = data.get("total_envio")
                if total_envio is not None:
                    self.total_sensores_config = total_envio

                intervalo = data.get("intervalo")
                if intervalo is not None:
                    self.intervalo_config = intervalo

                timestamp = data.get("timestamp")
                if rafaga is not None and timestamp is not None:
                    ts_float = float(timestamp)
                    self.rafaga_actual = max(self.rafaga_actual, rafaga)
                    entry = self.timestamps_por_rafaga.setdefault(rafaga, {"min": ts_float, "max": ts_float})
                    if ts_float < entry["min"]:
                        entry["min"] = ts_float
                    if ts_float > entry["max"]:
                        entry["max"] = ts_float

            except Exception:
                pass

        if actualizaciones_locales:
            self.base_sensores.update(actualizaciones_locales)

        limite = ahora - max(VENTANA_METRICAS_SEG * 4, 20.0)
        self.eventos_recientes = [e for e in self.eventos_recientes if e[0] >= limite]
        if len(self.mensajes_por_rafaga) > 50:
            claves_viejas = sorted(self.mensajes_por_rafaga.keys())[:-30]
            for k in claves_viejas:
                del self.mensajes_por_rafaga[k]
        if len(self.timestamps_por_rafaga) > 50:
            viejos = sorted(self.timestamps_por_rafaga.keys())[:-30]
            for k in viejos:
                del self.timestamps_por_rafaga[k]

    def actualizar_interfaz(self):
        ahora = time.time()

        cpu_actual = psutil.cpu_percent()
        memoria_actual = psutil.virtual_memory().percent
        self.max_cpu = max(self.max_cpu, cpu_actual)
        self.max_memoria = max(self.max_memoria, memoria_actual)

        self.lbl_cpu.setText(f"CPU Colector (Máx): {self.max_cpu} %")
        self.lbl_memoria.setText(f"Memoria Colector (Máx): {self.max_memoria} %")

        if self.tiempo_inicio is None:
            return

        limite_ventana = ahora - VENTANA_METRICAS_SEG
        eventos_ventana = [e for e in self.eventos_recientes if e[0] >= limite_ventana]

        if not eventos_ventana:
            throughput_actual = 0.0
        else:
            throughput_actual = len(eventos_ventana) / VENTANA_METRICAS_SEG

        latencias_ventana = [lat for (_, lat) in eventos_ventana if lat is not None]
        latencia_actual = np.mean(latencias_ventana) if latencias_ventana else 0.0

        self.max_throughput = max(self.max_throughput, throughput_actual)
        self.max_latencia = max(self.max_latencia, latencia_actual)

        claves = sorted(self.mensajes_por_rafaga.keys())
        if claves:
            actual = claves[-1]
            recibidos_actual = len(self.mensajes_por_rafaga[actual])
            estado = "completado" if recibidos_actual >= self.total_sensores_config else "recibiendo..."
            self.lbl_rafaga.setText(f"Ráfaga #{actual} ({estado})")

        if self.rafaga_actual > 0 and self.total_sensores_config > 0:
            total_esperado = self.rafaga_actual * self.total_sensores_config
            self.lbl_total_esperado.setText(f"Total esperado: {fmt_int(total_esperado)}")
            perdidos = total_esperado - self.total_mensajes
            if perdidos > 0:
                pct = perdidos / total_esperado * 100.0
                self.lbl_perdida.setText(f"Pérdida global: {fmt_float(pct, 1)} %")
            else:
                self.lbl_perdida.setText("Pérdida global: 0.0 %")

        sensores_activos = 0
        sensores_inactivos = 0
        
        lons, lats, colores = [], [], []

        for s_id, info in list(self.base_sensores.items()):
            tiempo_transcurrido = ahora - info["last_seen"]

            if tiempo_transcurrido <= self.tiempo_verde:
                sensores_activos += 1
                lon_val, lat_val = info["pos"]
                lons.append(lon_val)
                lats.append(lat_val)
                colores.append((0, 255, 0, 255))

            elif tiempo_transcurrido <= self.tiempo_plomo:
                sensores_inactivos += 1
                lon_val, lat_val = info["pos"]
                lons.append(lon_val)
                lats.append(lat_val)
                colores.append((128, 128, 128, 255))

        total_referencia = max(self.total_sensores_config, len(self.base_sensores))
        sensores_apagados = total_referencia - (sensores_activos + sensores_inactivos)

        self.lbl_mensajes_recibidos.setText(f"Mensajes recibidos: {fmt_int(self.total_mensajes)}")
        self.lbl_throughput.setText(f"Throughput (Máx): {fmt_float(self.max_throughput, 1)} msgs/s")
        self.lbl_latencia.setText(f"Latencia promedio (Máx): {fmt_float(self.max_latencia, 1)} ms")
        self.lbl_sensores_totales.setText(f"Sensores configurados: {fmt_int(self.total_sensores_config)}")
        self.lbl_activos.setText(f"Sensores activos (Verde): {fmt_int(sensores_activos)}")
        self.lbl_inactivos.setText(f"Sensores inactivos (Plomo): {fmt_int(sensores_inactivos)}")
        self.lbl_apagados.setText(f"Sensores apagados: {fmt_int(max(0, sensores_apagados))}")

        # ── CAMBIO CLAVE AQUÍ: Corrección de redibujado en PyQtGraph ──
        if lons and lats:
            np_x = np.array(lons, dtype=float)
            np_y = np.array(lats, dtype=float)
            brushes = [pg.mkBrush(*c) for c in colores]
            self.scatter.setData(x=np_x, y=np_y, brush=brushes)
        else:
            # Forzar limpieza total si la lista queda vacía
            self.scatter.clear()
        
        # Forzar explícitamente a Qt a repintar el área gráfica de inmediato
        self.plot_widget.viewport().update()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())