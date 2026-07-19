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
        # CAMBIO: candado explícito para proteger self.buffer, que ahora se
        # toca desde dos hilos (el de MQTT vía on_message, y el de la GUI
        # vía purgar_cola_inmediata). Antes no había ninguna protección.
        self.lock = threading.Lock()

    def run(self):
        def on_connect(client, userdata, flags, reason_code, properties=None):
            self.connection_signal.emit(True)

        def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
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

        estilo_seccion = "font-size: 11px; font-weight: bold; color: #888; padding: 10px 6px 2px 6px; border-bottom: 1px solid #ddd;"
        estilo_label = "font-size: 14px; font-weight: bold; padding: 4px 6px; color: #333;"

        def seccion(texto):
            lbl = QLabel(texto)
            lbl.setStyleSheet(estilo_seccion)
            return lbl

        # ── ESTADO DEL SISTEMA ──
        layout_metricas.addWidget(seccion("━━━ ESTADO DEL SISTEMA ━━━"))
        self.lbl_conexion = QLabel("Broker MQTT: conectando...")
        self.lbl_cpu = QLabel("CPU Colector: 0.0 %")
        self.lbl_memoria = QLabel("Memoria Colector: 0.0 %")
        for lbl in (self.lbl_conexion, self.lbl_cpu, self.lbl_memoria):
            lbl.setStyleSheet(estilo_label)
            layout_metricas.addWidget(lbl)

        # ── RENDIMIENTO MQTT ──
        layout_metricas.addWidget(seccion("━━━ RENDIMIENTO MQTT ━━━"))
        self.lbl_mensajes_recibidos = QLabel("Mensajes recibidos: 0")
        self.lbl_throughput = QLabel("Throughput: 0.0 msgs/s")
        self.lbl_latencia = QLabel("Latencia promedio: 0.0 ms")
        for lbl in (self.lbl_mensajes_recibidos, self.lbl_throughput, self.lbl_latencia):
            lbl.setStyleSheet(estilo_label)
            layout_metricas.addWidget(lbl)

        # ── SENSORES ──
        layout_metricas.addWidget(seccion("━━━ SENSORES ━━━"))
        self.lbl_sensores_totales = QLabel("Sensores configurados: 0")
        self.lbl_activos = QLabel("Sensores activos: 0")
        self.lbl_inactivos = QLabel("Sensores inactivos: 0")
        for lbl in (self.lbl_sensores_totales, self.lbl_activos, self.lbl_inactivos):
            lbl.setStyleSheet(estilo_label)
            layout_metricas.addWidget(lbl)

        # ── RÁFAGAS ──
        layout_metricas.addWidget(seccion("━━━ RÁFAGAS ━━━"))
        self.lbl_rafaga = QLabel("Ráfaga #0: 0/0 (recibiendo...)")
        self.lbl_ultima_rafaga = QLabel("Última ráfaga: —")
        self.lbl_esperados = QLabel("Mensajes esperados: 0")
        self.lbl_recibidos_rafaga = QLabel("Mensajes recibidos: 0")
        self.lbl_duracion = QLabel("Duración: 0.0000 s")
        self.lbl_perdida = QLabel("Pérdida: N/D")
        for lbl in (self.lbl_rafaga, self.lbl_ultima_rafaga, self.lbl_esperados, self.lbl_recibidos_rafaga, self.lbl_duracion, self.lbl_perdida):
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
        self.timer.start(1000)

    def inicializar_variables_sistema(self):
        self.total_mensajes = 0
        self.tiempo_inicio = None
        self.ultimo_mensaje_ts = None
        self.base_sensores = {}
        self.total_sensores_config = 0

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
        self.lbl_throughput.setText("Throughput: 0.0 msgs/s")
        self.lbl_latencia.setText("Latencia promedio: 0.0 ms")
        self.lbl_perdida.setText("Pérdida: N/D")
        self.lbl_cpu.setText("CPU Colector: 0.0 %")
        self.lbl_memoria.setText("Memoria Colector: 0.0 %")
        self.lbl_sensores_totales.setText("Sensores configurados: 0")
        self.lbl_activos.setText("Sensores activos: 0")
        self.lbl_inactivos.setText("Sensores inactivos: 0")
        self.lbl_rafaga.setText("Ráfaga #0: 0/0 (recibiendo...)")
        self.lbl_ultima_rafaga.setText("Última ráfaga: —")
        self.lbl_esperados.setText("Mensajes esperados: 0")
        self.lbl_recibidos_rafaga.setText("Mensajes recibidos: 0")
        self.lbl_duracion.setText("Duración: 0.0000 s")

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
                sensor_id = data.get("sensor_id")
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

        # Poda de eventos y ráfagas viejas para no acumular memoria indefinidamente.
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

        self.lbl_cpu.setText(f"CPU Colector: {psutil.cpu_percent()} %")
        self.lbl_memoria.setText(f"Memoria Colector: {psutil.virtual_memory().percent} %")

        if self.tiempo_inicio is None:
            return

        # CAMBIO: throughput y latencia calculados solo sobre la ventana
        # deslizante reciente, no sobre todo el histórico. Un cambio real
        # en el simulador ahora se ve reflejado en segundos, no diluido
        # entre minutos de datos previos.
        limite_ventana = ahora - VENTANA_METRICAS_SEG
        eventos_ventana = [e for e in self.eventos_recientes if e[0] >= limite_ventana]

        if not eventos_ventana:
            throughput = 0.0
        else:
            throughput = len(eventos_ventana) / VENTANA_METRICAS_SEG

        latencias_ventana = [lat for (_, lat) in eventos_ventana if lat is not None]
        latencia_prom = np.mean(latencias_ventana) if latencias_ventana else 0.0

        claves = sorted(self.mensajes_por_rafaga.keys())

        # Rafaga en progreso (última en claves)
        if len(self.mensajes_por_rafaga) >= 1:
            actual = claves[-1]
            recibidos_actual = len(self.mensajes_por_rafaga[actual])
            estado = "completado" if recibidos_actual >= self.total_sensores_config else "recibiendo..."
            self.lbl_rafaga.setText(
                f"Ráfaga #{actual}: {fmt_int(recibidos_actual)}/{fmt_int(self.total_sensores_config)} ({estado})")

        # Última ráfaga completa
        if len(self.mensajes_por_rafaga) >= 2:
            ultima = claves[-2]
            self.lbl_ultima_rafaga.setText(f"Última ráfaga: #{ultima}")
            timings = self.timestamps_por_rafaga.get(ultima)
            if timings:
                diff = timings["max"] - timings["min"]
                self.lbl_duracion.setText(f"Duración: {diff:.4f} s")
            recibidos = len(self.mensajes_por_rafaga[ultima])
            self.lbl_esperados.setText(f"Mensajes esperados: {fmt_int(self.total_sensores_config)}")
            self.lbl_recibidos_rafaga.setText(f"Mensajes recibidos: {fmt_int(recibidos)}")

        # Pérdida: compara las dos últimas ráfagas completas
        if len(self.mensajes_por_rafaga) >= 3:
            rafaga_ref = claves[-3]
            rafaga_actual = claves[-2]
            sensores_ref = self.mensajes_por_rafaga[rafaga_ref]
            sensores_actual = self.mensajes_por_rafaga[rafaga_actual]
            perdidos = sensores_ref - sensores_actual
            if sensores_ref:
                perdida_pct = len(perdidos) / len(sensores_ref) * 100.0
                self.lbl_perdida.setText(f"Pérdida: {fmt_float(perdida_pct, 1)} %")

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

        total_referencia = max(self.total_sensores_config, len(self.base_sensores))
        sensores_desconectados = total_referencia - sensores_conectados

        self.lbl_mensajes_recibidos.setText(f"Mensajes recibidos: {fmt_int(self.total_mensajes)}")
        self.lbl_throughput.setText(f"Throughput: {fmt_float(throughput, 1)} msgs/s")
        self.lbl_latencia.setText(f"Latencia promedio: {fmt_float(latencia_prom, 1)} ms")
        self.lbl_sensores_totales.setText(f"Sensores configurados: {fmt_int(self.total_sensores_config)}")
        self.lbl_activos.setText(f"Sensores activos: {fmt_int(sensores_activos)}")
        self.lbl_inactivos.setText(f"Sensores inactivos: {fmt_int(sensores_desconectados)}")

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
