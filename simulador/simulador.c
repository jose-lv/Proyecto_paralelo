#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <omp.h>
#include <mpi.h>        // <--- ¡Librería del protocolo MPI!
#include "MQTTClient.h"

#define ADDRESS     "tcp://localhost:1883"
#define TOPIC       "ciudad/sensores/medicion"
#define QOS         0
#define TIMEOUT     10000L

// Subimos la valla: 10,000 sensores en total (Requerimiento Nivel 1 del PDF)
#define TOTAL_SENSORES 50000 

int main(int argc, char* argv[]) {
    // 1. Inicializar el entorno de comunicación distribuida MPI
    int mpi_rank, mpi_size;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank); // ID del proceso actual
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size); // Cantidad total de procesos

    // Calcular cuántos sensores le toca simular a este proceso específico
    int sensores_por_proceso = TOTAL_SENSORES / mpi_size;
    int inicio_sensor = mpi_rank * sensores_por_proceso;
    int fin_sensor = inicio_sensor + sensores_por_proceso;

    // Crear un ID de cliente MQTT único por cada proceso MPI para evitar colisiones
    char client_id[30];
    sprintf(client_id, "Simulador_MPI_Proceso_%d", mpi_rank);

    MQTTClient client;
    MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
    int rc;

    // Crear el cliente MQTT de este proceso
    if ((rc = MQTTClient_create(&client, ADDRESS, client_id,
        MQTTCLIENT_PERSISTENCE_NONE, NULL)) != MQTTCLIENT_SUCCESS) {
        printf("[Proceso MPI %d] Error al crear cliente MQTT: %d\n", mpi_rank, rc);
        MPI_Finalize();
        return EXIT_FAILURE;
    }

    conn_opts.keepAliveInterval = 20;
    conn_opts.cleansession = 1;

    if ((rc = MQTTClient_connect(client, &conn_opts)) != MQTTCLIENT_SUCCESS) {
        printf("[Proceso MPI %d] Fallo de conexión al broker: %d\n", mpi_rank, rc);
        MQTTClient_destroy(&client);
        MPI_Finalize();
        return EXIT_FAILURE;
    }

    // Sincronizar todos los procesos antes de desatar la tormenta de datos
    MPI_Barrier(MPI_COMM_WORLD);
    
    double t_inicio = MPI_Wtime(); // Medir tiempo de ejecución para tus métricas

    // 2. Región Paralela Híbrida: OpenMP paraleliza la porción asignada por MPI
    #pragma omp parallel
    {
        int thread_id = omp_get_thread_num();
        
        #pragma omp single
        printf("[Proceso MPI %d] Simulando rango de sensores [%d al %d] usando %d hilos OpenMP.\n", 
               mpi_rank, inicio_sensor, fin_sensor - 1, omp_get_num_threads());

        #pragma omp for
        for (int i = inicio_sensor; i < fin_sensor; i++) {
            char payload[256];
            MQTTClient_deliveryToken token;

            // Variación de geografía simulada según el ID único del sensor
	    double offset_lat = ((rand() % 10000) - 5000) / 50000.0; // Desplazamiento norte-sur
	    double offset_lon = ((rand() % 10000) - 5000) / 50000.0; // Desplazamiento este-oeste

	    double lat = -12.0500 + offset_lat;
	    double lon = -77.0600 + offset_lon;

            double temp = 18.0 + (rand() % 120) / 10.0;
            double hum = 60.0 + (rand() % 300) / 10.0;

            sprintf(payload, 
                    "{\n  \"id\": \"sensor_%06d\",\n  \"lat\": %.5f,\n  \"lon\": %.5f,\n  \"temperatura\": %.1f,\n  \"humedad\": %.1f,\n  \"timestamp\": %ld\n}", 
                    i, lat, lon, temp, hum, (long)time(NULL));

            MQTTClient_message pubmsg = MQTTClient_message_initializer;
            pubmsg.payload = payload;
            pubmsg.payloadlen = (int)strlen(payload);
            pubmsg.qos = QOS;
            pubmsg.retained = 0;

            MQTTClient_publishMessage(client, TOPIC, &pubmsg, &token);
        }
    }

    // Esperar que terminen las publicaciones de este proceso
    MQTTClient_disconnect(client, 1000);
    MQTTClient_destroy(&client);

    double t_fin = MPI_Wtime();
    printf("[Proceso MPI %d] Finalizó su carga en %.4f segundos.\n", mpi_rank, (t_fin - t_inicio));

    // 3. Cerrar el entorno global de MPI
    MPI_Finalize();
    return EXIT_SUCCESS;
}
