#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <omp.h>
#include <mpi.h>
#include <MQTTClient.h>

#define ADDRESS     "tcp://localhost:1883"
#define CLIENTID    "SimuladorHPC_Dinámico"
#define TOPIC       "ciudad/sensores/medicion"
#define QOS         0
#define TIMEOUT     5000L

int main(int argc, char* argv[]) {
    int num_procs, rank_id;

    // Valores por defecto en caso falte algún argumento
    int hilos_openmp = 4;
    int total_sensores = 50000;
    int intervalo_configurado = 5;

    // Leer parámetros pasados por consola: ./simulador [hilos] [sensores] [intervalo]
    if (argc > 1) hilos_openmp = atoi(argv[1]);
    if (argc > 2) total_sensores = atoi(argv[2]);
    if (argc > 3) intervalo_configurado = atoi(argv[3]);

    // Configurar dinámicamente la cantidad de hilos OpenMP antes de entrar a la región paralela
    omp_set_num_threads(hilos_openmp);

    int provisto;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provisto);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank_id);

    // Reparto de carga dinámico basado en el argumento de sensores recibido
    int sensores_por_proceso = total_sensores / num_procs;
    int inicio_sensor = rank_id * sensores_por_proceso;
    int fin_sensor = (rank_id == num_procs - 1) ? total_sensores - 1 : (inicio_sensor + sensores_por_proceso - 1);

    srand(time(NULL) + rank_id);

    if (rank_id == 0) {
        printf("\n============================================================\n");
        printf("[HPC MASTER] CONFIGURACIÓN EXPERIMENTAL EN EJECUCIÓN:\n");
        printf(" -> Procesos MPI totales: %d\n", num_procs);
        printf(" -> Hilos OpenMP por proceso: %d (Total hilos en clúster: %d)\n", hilos_openmp, num_procs * hilos_openmp);
        printf(" -> Universo de Sensores: %d\n", total_sensores);
        printf(" -> Intervalo de Transmisión: %ds\n", intervalo_configurado);
        printf("============================================================\n\n");
    }

    double t_inicio = MPI_Wtime();

    #pragma omp parallel
    {
        int hilo_id = omp_get_thread_num();
        MQTTClient client;
        MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
        char clientId_hilo[60];
        
        sprintf(clientId_hilo, "%s_%d_H%d", CLIENTID, rank_id, hilo_id);
        MQTTClient_create(&client, ADDRESS, clientId_hilo, MQTTCLIENT_PERSISTENCE_NONE, NULL);
        conn_opts.keepAliveInterval = 60;
        conn_opts.cleansession = 1;

        if (MQTTClient_connect(client, &conn_opts) == MQTTCLIENT_SUCCESS) {
            
            #pragma omp for nowait
            for (int i = inicio_sensor; i <= fin_sensor; i++) {
                double offset_lat = ((rand() % 10000) - 5000) / 50000.0;
                double offset_lon = ((rand() % 10000) - 5000) / 50000.0;
                double lat = -12.0500 + offset_lat;
                double lon = -77.0600 + offset_lon;

                double ts = (double)time(NULL);

                char json_payload[220];
                // Enviamos "total_sensores" dinámico dentro del JSON para que Python sepa cuánto procesar
                sprintf(json_payload, "{\"id\": \"S-%06d\", \"lat\": %.5f, \"lon\": %.5f, \"timestamp\": %.4f, \"intervalo\": %d, \"total_envio\": %d}", 
                        i, lat, lon, ts, intervalo_configurado, total_sensores);

                MQTTClient_message pubmsg = MQTTClient_message_initializer;
                pubmsg.payload = json_payload;
                pubmsg.payloadlen = (int)strlen(json_payload);
                pubmsg.qos = QOS;
                MQTTClient_deliveryToken token;

                MQTTClient_publishMessage(client, TOPIC, &pubmsg, &token);
                MQTTClient_waitForCompletion(client, token, TIMEOUT);
            }
            MQTTClient_disconnect(client, 10);
            MQTTClient_destroy(&client);
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);
    double t_fin = MPI_Wtime();

    printf("[Proceso MPI %d] Rango completado en %.4f segundos.\n", rank_id, (t_fin - t_inicio));

    MPI_Finalize();
    return 0;
}
