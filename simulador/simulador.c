#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <omp.h>        // Biblioteca estándar para OpenMP (paralelismo basado en hilos)
#include <mpi.h>        // Biblioteca estándar para MPI (paralelismo basado en procesos)
#include <MQTTClient.h> // Biblioteca Paho MQTT para el envío de mensajes

// Definición de constantes para la conexión MQTT
#define ADDRESS     "tcp://localhost:1883"
#define CLIENTID    "SimuladorHPC_Continuo"
#define TOPIC       "ciudad/sensores/medicion"
#define QOS         0

int main(int argc, char* argv[]) {
    // num_procs: Total de procesos MPI lanzados. rank_id: ID único de cada proceso (0 a N-1).
    int num_procs, rank_id;
    
    int hilos_openmp = 4;
    int total_sensores = 1000;
    int intervalo_configurado = 1;

    if (argc > 1) hilos_openmp = atoi(argv[1]);
    if (argc > 2) total_sensores = atoi(argv[2]);
    if (argc > 3) intervalo_configurado = atoi(argv[3]);

    // OMP: Define cuántos hilos se crearán por cada proceso MPI al abrir regiones paralelas
    omp_set_num_threads(hilos_openmp);
    
    int provisto;
    // MPI: Inicializa el entorno. MPI_THREAD_MULTIPLE permite llamadas concurrentes desde distintos hilos
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provisto);
    
    // Obtiene el tamaño del comunicador global y el identificador del proceso actual
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank_id);

    // Reparto de carga: Cada proceso MPI calcula el rango de sensores que le corresponde procesar
    int sensores_por_proceso = total_sensores / num_procs;
    int inicio_sensor = rank_id * sensores_por_proceso;
    int fin_sensor = (rank_id == num_procs - 1) ? total_sensores - 1 : (inicio_sensor + sensores_por_proceso - 1);


    if (rank_id == 0) {
        printf("\n============================================================\n");
        printf("CONFIGURACIÓN EXPERIMENTAL:\n");
        printf(" -> Procesos MPI totales: %d\n", num_procs);
        printf(" -> Hilos OpenMP por proceso: %d (Total hilos en clúster: %d)\n", hilos_openmp, num_procs * hilos_openmp);
        printf(" -> Universo Total de Sensores: %d\n", total_sensores);
        printf(" -> Transmisión Continua cada: %ds\n", intervalo_configurado);
        printf("============================================================\n\n");
    }

    // OMP: Inicia la región paralela. Cada proceso MPI bifurca su ejecución en 'hilos_openmp' hilos
    #pragma omp parallel
    {
        // Cada hilo obtiene su ID dentro del proceso actual (0 a hilos_openmp-1)
        int hilo_id = omp_get_thread_num();
        
        // Semilla local por hilo: rand_r es thread-safe y asegura secuencias aleatorias independientes por hilo
        unsigned int semilla_hilo = (unsigned int)(time(NULL) + rank_id + hilo_id);
        
        // Configuración de cliente MQTT: cada hilo actúa como un sensor independiente
        MQTTClient client;
        MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
        char clientId_hilo[60];
        
        sprintf(clientId_hilo, "%s_%d_H%d", CLIENTID, rank_id, hilo_id);
        MQTTClient_create(&client, ADDRESS, clientId_hilo, MQTTCLIENT_PERSISTENCE_NONE, NULL);
        
        conn_opts.keepAliveInterval = 60;
        conn_opts.cleansession = 1;

        if (MQTTClient_connect(client, &conn_opts) == MQTTCLIENT_SUCCESS) {
            int nro_rafaga = 1;

            while (1) {
                double t_inicio_envio = MPI_Wtime();
                double ts_simulador = (double)time(NULL);

                // OMP: Distribuye las iteraciones del bucle for entre los hilos del proceso MPI actual, 'nowait' permite que un hilo comience a limpiar sin esperar a otros
                #pragma omp for nowait
                for (int i = inicio_sensor; i <= fin_sensor; i++) {
                    // Generación de datos con rand_r
                    double offset_lat = ((rand_r(&semilla_hilo) % 10000) - 5000) / 50000.0;
                    double offset_lon = ((rand_r(&semilla_hilo) % 10000) - 5000) / 50000.0;
                    double lat = -12.0500 + offset_lat;
                    double lon = -77.0600 + offset_lon;

                    double temperature = 18.0 + (rand_r(&semilla_hilo) % 120) / 10.0;
                    double humidity = 60.0 + (rand_r(&semilla_hilo) % 300) / 10.0;

                    char json_payload[320];
                    sprintf(json_payload, "{\"sensor_id\": \"S-%06d\", \"x\": %.5f, \"y\": %.5f, \"temperature\": %.1f, \"humidity\": %.1f, \"timestamp\": %.4f, \"intervalo\": %d, \"total_envio\": %d}",
                            i, lon, lat, temperature, humidity, ts_simulador, intervalo_configurado, total_sensores);

                    MQTTClient_message pubmsg = MQTTClient_message_initializer;
                    pubmsg.payload = json_payload;
                    pubmsg.payloadlen = (int)strlen(json_payload);
                    pubmsg.qos = QOS;

                    MQTTClient_deliveryToken token;
                    MQTTClient_publishMessage(client, TOPIC, &pubmsg, &token);
                } 

                double t_fin_envio_local = MPI_Wtime();
                double tiempo_procesamiento_local = t_fin_envio_local - t_inicio_envio;
                double tiempo_total_maximo;

                // MPI: Reduce los tiempos locales de todos los procesos al valor máximo (tiempo de cuello de botella)
                MPI_Reduce(&tiempo_procesamiento_local, &tiempo_total_maximo, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

                // Solo el hilo maestro del proceso 0 informa el progreso global
                if (rank_id == 0 && hilo_id == 0) {
                    printf("Ráfaga #%d finalizada en %.4f segundos.\n", nro_rafaga, tiempo_total_maximo);
                }
                nro_rafaga++;

                // Sincronización temporal entre ráfagas para que estas inicien despues del intervalo_configurado
                double tiempo_restante = (double)intervalo_configurado - tiempo_total_maximo;
                if (tiempo_restante > 0) {
                    usleep((useconds_t)(tiempo_restante * 1000000.0));
                }
            }
            MQTTClient_disconnect(client, 10);
            MQTTClient_destroy(&client);
        }
    }

    // MPI: Cierra y libera los recursos del entorno de comunicación distribuida
    MPI_Finalize();
    return 0;
}