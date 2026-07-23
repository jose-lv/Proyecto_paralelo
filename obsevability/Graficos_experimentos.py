from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

matplotlib.rcParams.update({
    "figure.dpi": 160,
    "savefig.dpi": 220,
    "font.size": 10,
    "axes.titlesize": 11,
    "axes.labelsize": 10,
    "legend.fontsize": 9,
    "axes.edgecolor": "#222222",
    "grid.color": "#f0f0f0",
    "figure.facecolor": "white",
    "axes.facecolor": "white",
})


COLOR_P = { 1: "#18468B", 2: "#2B78B2", 4: "#41ABC9", 8: "#459F91"}

COLOR_SENSORES = {10000: "#18468B", 50000: "#2B78B2", 100000: "#459F91"}

SENSORES = [10000, 50000, 100000]
PROCESOS_MPI = [1, 2, 4, 8]
ANOMALOS_10K = ["EXP-058", "EXP-061"]

OUT = Path(__file__).parent
CSV = OUT / "Matriz_exp.csv"


def style(ax, title, xlabel, ylabel):
    ax.set_title(title, pad=10)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.grid(True, linestyle="-", linewidth=0.5, alpha=0.7)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


# ===============================================================================================================
# FIGURAS GENERADAS A PARTIR DEL CSV AL UTILIZAR EL THROUGHPUT PARA LOS GRAFICOS DE METRICAS DEL PROGRAMA
# ================================================================================================================

def fig_throughput_vs_sensores(df):
    """Figura 7.1 — throughput medio por volumen de sensores, curvas por P."""
    fig, ax = plt.subplots(figsize=(7.2, 4.3), constrained_layout=True)
    for p in PROCESOS_MPI:
        sub = df[df["P"] == p].groupby("sensores")["throughput"].mean()
        ax.plot(list(sub.index), list(sub.values), marker="o", ms=7.5, lw=2.8,
                color=COLOR_P[p], label=f"MPI P={p}")
    style(ax, "Throughput frente al volumen de sensores", "Sensores", "Throughput (msgs/s)")
    ax.legend(frameon=False, loc="upper left")
    ax.set_xticks(SENSORES)
    ax.set_xticklabels(["10k", "50k", "100k"])
    fig.savefig(OUT / "fig07_1_throughput_vs_sensores.png")
    plt.close(fig)


def fig_throughput_vs_openmp_100k(df):
    """Figura 7.2 — throughput vs hilos OpenMP T, solo 100 000 sensores."""
    sub100 = df[df["sensores"] == 100000]
    fig, ax = plt.subplots(figsize=(7.2, 4.3), constrained_layout=True)
    for p in PROCESOS_MPI:
        s = sub100[sub100["P"] == p].sort_values("T")
        ax.plot(s["T"].tolist(), s["throughput"].tolist(), marker="o", ms=7.5, lw=2.8,
                color=COLOR_P[p], label=f"MPI P={p}")
    style(ax, "Throughput frente a hilos OpenMP (100 000 sensores)", "Hilos OpenMP (T)", "Throughput (msgs/s)")
    ax.legend(frameon=False, loc="upper left")
    ax.set_xscale("log", base=2)
    ax.set_xticks([1, 2, 4, 8, 16, 32])
    ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    fig.savefig(OUT / "fig07_throughput_vs_openmp_100k.png")
    plt.close(fig)


def fig_latencia_vs_sensores(df):
    """Figura 7.3 — latencia media por volumen de sensores, curvas por P."""
    fig, ax = plt.subplots(figsize=(7.2, 4.3), constrained_layout=True)
    for p in PROCESOS_MPI:
        sub = df[df["P"] == p].groupby("sensores")["latencia"].mean()
        ax.plot(list(sub.index), list(sub.values), marker="o", ms=7.5, lw=2.8,
                color=COLOR_P[p], label=f"MPI P={p}")
    style(ax, "Latencia frente al volumen de sensores", "Sensores", "Latencia (ms)")
    ax.legend(frameon=False, loc="upper left")
    ax.set_xticks(SENSORES)
    ax.set_xticklabels(["10k", "50k", "100k"])
    fig.savefig(OUT / "fig07_2_latencia_vs_sensores.png")
    plt.close(fig)


def fig_perdida_vs_sensores(df_completo):
    """Figura 7.4 — perdida media por volumen de sensores (matriz completa, sin excluir anomalos)."""
    fig, ax = plt.subplots(figsize=(7.2, 4.3), constrained_layout=True)
    for p in PROCESOS_MPI:
        sub = df_completo[df_completo["P"] == p].groupby("sensores")["perdida"].mean()
        ax.plot(list(sub.index), list(sub.values), marker="o", ms=7.5, lw=2.8,
                color=COLOR_P[p], label=f"MPI P={p}")
    style(ax, "Perdida media frente al volumen de sensores", "Sensores", "Perdida (%)")
    ax.legend(frameon=False, loc="upper left")
    ax.set_xticks(SENSORES)
    ax.set_xticklabels(["10k", "50k", "100k"])
    fig.savefig(OUT / "fig07_perdida_vs_sensores.png")
    plt.close(fig)


def fig_cpu_100k(df_completo):
    """Figura 7.5 — CPU maxima del colector, solo 100 000 sensores."""
    sub100 = df_completo[df_completo["sensores"] == 100000].sort_values("hilos")
    fig, ax = plt.subplots(figsize=(7.2, 4.3), constrained_layout=True)
    for p in PROCESOS_MPI:
        sp = sub100[sub100["P"] == p]
        ax.scatter(sp["hilos"].tolist(), sp["cpu"].tolist(), s=55, color=COLOR_P[p],
                   label=f"P={p}", edgecolors="white", linewidths=0.5)
    style(ax, "Uso de CPU maximo del colector (100k)", "Hilos totales (P x T)", "CPU (%)")
    ax.legend(frameon=False, fontsize=8, loc="upper left")
    fig.savefig(OUT / "fig07_4_cpu_100k.png")
    plt.close(fig)


def fig_memoria_100k(df_completo):
    """Figura 7.6 — memoria maxima del colector, solo 100 000 sensores."""
    sub100 = df_completo[df_completo["sensores"] == 100000].sort_values("hilos")
    fig, ax = plt.subplots(figsize=(7.2, 4.3), constrained_layout=True)
    for p in PROCESOS_MPI:
        sp = sub100[sub100["P"] == p]
        ax.scatter(sp["hilos"].tolist(), sp["memoria"].tolist(), s=55, color=COLOR_P[p],
                   label=f"P={p}", edgecolors="white", linewidths=0.5)
    style(ax, "Uso de memoria maximo del colector (100k)", "Hilos totales (P x T)", "Memoria (%)")
    ax.legend(frameon=False, fontsize=8, loc="upper left")
    fig.savefig(OUT / "fig07_5_memoria_100k.png")
    plt.close(fig)


def fig_speedup_throughput(df):
    """Figura 7.7 — speedup relativo de throughput, base P=1,T=1 por volumen."""
    fig, ax = plt.subplots(figsize=(7.2, 4.3), constrained_layout=True)
    for sv in SENSORES:
        base = float(df[(df["P"] == 1) & (df["T"] == 1) & (df["sensores"] == sv)]["throughput"].iloc[0])
        sub = df[df["sensores"] == sv].copy()
        sub["speedup"] = sub["throughput"] / base
        g = sub.groupby("hilos")["speedup"].mean().sort_index()
        ax.plot(list(g.index), list(g.values), marker="o", ms=7.5, lw=2.8,
                color=COLOR_SENSORES[sv], label=f"{sv // 1000}k sensores")
    style(ax, "Speedup relativo de throughput (base P=1, T=1)", "Hilos totales (P x T)", "Speedup")
    ax.axhline(1.0, color="#888888", ls="--", lw=1)
    ax.legend(frameon=False, loc="upper left")
    fig.savefig(OUT / "fig07_speedup_throughput.png")
    plt.close(fig)


# =====================================================================================
# FIGURAS GENERADAS AL UTILIZAR EL TIEMPO DE GENERACION DE SENSORES DEL SIMULADOR PURO
# =====================================================================================

def fig_speedup_simulador():
    """NUEVA FIGURA — Speedup del simulador según volumen de sensores."""
    fig, ax = plt.subplots(figsize=(7.2, 4.3), constrained_layout=True)
    hilos = [1, 2, 4, 8]
    speedup_10k = [1.00, 1.38, 1.28, 0.59]
    speedup_50k = [1.00, 1.33, 2.49, 1.02]
    speedup_100k = [1.00, 0.74, 4.42, 3.30]

    # Colores por defecto de Matplotlib (Azul, Naranja, Verde)
    ax.plot(hilos, speedup_10k, marker='o', linewidth=2, label='10 000 sensores')
    ax.plot(hilos, speedup_50k, marker='s', linewidth=2, label='50 000 sensores')
    ax.plot(hilos, speedup_100k, marker='^', linewidth=2, label='100 000 sensores')
    ax.axhline(y=1, linestyle='--', color='gray', label='Speedup ideal = 1')

    ax.set_title('Speedup del simulador según volumen de sensores')
    ax.set_xlabel('Procesos MPI')
    ax.set_ylabel('Speedup')
    ax.set_xticks(hilos)
    ax.grid(True, alpha=0.3)
    ax.legend(loc='upper right')

    fig.savefig(OUT / "fig08_speedup_simulador.png")
    plt.close(fig)


def fig_eficiencia_simulador():
    """NUEVA FIGURA — Eficiencia del simulador."""
    fig, ax = plt.subplots(figsize=(7.2, 4.3), constrained_layout=True)
    procesos = [1, 2, 4, 8]
    ef_10k = [100, 69, 32, 7]
    ef_50k = [100, 67, 62, 13]
    ef_100k = [100, 37, 110, 41]

    # Colores por defecto de Matplotlib (Azul, Naranja, Verde)
    ax.plot(procesos, ef_10k, marker='o', linewidth=2, label='10 000 sensores')
    ax.plot(procesos, ef_50k, marker='s', linewidth=2, label='50 000 sensores')
    ax.plot(procesos, ef_100k, marker='^', linewidth=2, label='100 000 sensores')
    ax.axhline(y=100, linestyle='--', color='gray', label='Eficiencia ideal (100%)')

    ax.set_title('Eficiencia del simulador')
    ax.set_xlabel('Procesos MPI')
    ax.set_ylabel('Eficiencia (%)')
    ax.set_xticks(procesos)
    ax.grid(True, alpha=0.3)
    ax.legend(loc='lower left')

    fig.savefig(OUT / "fig09_eficiencia_simulador.png")
    plt.close(fig)


def fig_escalabilidad_fuerte():
    """NUEVA FIGURA — Escalabilidad fuerte."""
    fig, ax = plt.subplots(figsize=(7.2, 4.3), constrained_layout=True)
    procesos = [1, 2, 4, 8]
    ideal = [1, 2, 4, 8]
    speedup_10k = [1.00, 1.38, 1.28, 0.59]
    speedup_50k = [1.00, 1.33, 2.49, 1.02]
    speedup_100k = [1.00, 0.74, 4.42, 3.30]

    # Colores por defecto de Matplotlib (Azul, Naranja, Verde, Rojo)
    ax.plot(procesos, ideal, '--', linewidth=3, label='Escalabilidad ideal')
    ax.plot(procesos, speedup_10k, marker='o', linewidth=2, label='10k sensores')
    ax.plot(procesos, speedup_50k, marker='s', linewidth=2, label='50k sensores')
    ax.plot(procesos, speedup_100k, marker='^', linewidth=2, label='100k sensores')

    ax.set_title('Escalabilidad fuerte')
    ax.set_xlabel('Procesos MPI')
    ax.set_ylabel('Speedup')
    ax.set_xticks(procesos)
    ax.grid(True, alpha=0.3)
    ax.legend(loc='upper left')

    fig.savefig(OUT / "fig10_escalabilidad_fuerte.png")
    plt.close(fig)


def main():
    df = pd.read_csv(CSV)

    column_mapping = {
        "ID Experimento": "id",
        "Procesos MPI (P)": "P",
        "Hilos OpenMP (T)": "T",
        "Hilos totales (Px T)": "hilos",
        "Volumen de sensores": "sensores",
        "Throughput msgs/s": "throughput",
        "Latencia ms": "latencia",
        "Pérdida de mensajes %": "perdida",
        "Uso de CPU %": "cpu",
        "Uso de memoria %": "memoria"
    }
    df = df.rename(columns=column_mapping)

    df_sin_anomalos = df[~df["id"].isin(ANOMALOS_10K)].copy()

    tareas = [
        ("throughput_vs_sensores", lambda: fig_throughput_vs_sensores(df_sin_anomalos)),
        ("throughput_vs_openmp_100k", lambda: fig_throughput_vs_openmp_100k(df_sin_anomalos)),
        ("latencia_vs_sensores", lambda: fig_latencia_vs_sensores(df_sin_anomalos)),
        ("perdida_vs_sensores", lambda: fig_perdida_vs_sensores(df)),
        ("cpu_100k", lambda: fig_cpu_100k(df)),
        ("memoria_100k", lambda: fig_memoria_100k(df)),
        ("speedup_throughput", lambda: fig_speedup_throughput(df_sin_anomalos)),
        ("speedup_simulador", fig_speedup_simulador),
        ("eficiencia_simulador", fig_eficiencia_simulador),
        ("escalabilidad_fuerte", fig_escalabilidad_fuerte),
    ]

    for nombre, func in tareas:
        try:
            func()
            print(f"OK    -> {nombre}")
        except Exception as e:
            print(f"FALLO -> {nombre}: {e}")


if __name__ == "__main__":
    main()