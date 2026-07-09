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
    // Variables para almacenar el número total de procesos MPI y el identificador del proceso actual
    int num_procs, rank_id;
    
    // Valores por defecto para la simulación
    int hilos_openmp = 4;
    int total_sensores = 1000;
    int intervalo_configurado = 1;

    // Lectura de parámetros desde la línea de comandos (si se proporcionan)
    if (argc > 1) hilos_openmp = atoi(argv[1]);
    if (argc > 2) total_sensores = atoi(argv[2]);
    if (argc > 3) intervalo_configurado = atoi(argv[3]);

    /* * OMP: Configura el número de hilos que utilizará OpenMP en las 
     * regiones paralelas que se definan más adelante.
     */
    omp_set_num_threads(hilos_openmp);
    
    int provisto;
    /*
     * MPI_Init_thread: Inicializa el entorno de ejecución de MPI soportando hilos.
     * Se usa 'MPI_THREAD_MULTIPLE' porque múltiples hilos de OpenMP van a realizar 
     * llamadas a funciones de MPI o interactuar de forma paralela de manera simultánea.
     * El parámetro 'provisto' devuelve el nivel de soporte real que otorgó la implementación de MPI.
     */
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provisto);
    
    /*
     * MPI_Comm_size: Obtiene el número total de procesos que forman parte 
     * del comunicador global (MPI_COMM_WORLD) y lo guarda en 'num_procs'.
     */
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
    
    /*
     * MPI_Comm_rank: Obtiene el identificador (ID o Rank) del proceso actual 
     * dentro del comunicador global y lo guarda en 'rank_id' (va desde 0 hasta num_procs-1).
     */
    MPI_Comm_rank(MPI_COMM_WORLD, &rank_id);

    // --- Reparto de carga entre procesos MPI ---
    // Determina cuántos sensores le toca procesar a este nodo/proceso en particular
    int sensores_por_proceso = total_sensores / num_procs;
    int inicio_sensor = rank_id * sensores_por_proceso;
    // Si es el último proceso, se lleva el residuo para no dejar sensores sin procesar
    int fin_sensor = (rank_id == num_procs - 1) ? total_sensores - 1 : (inicio_sensor + sensores_por_proceso - 1);

    // El proceso Máster (Rank 0) se encarga de imprimir la configuración inicial en la consola
    if (rank_id == 0) {
        printf("\n============================================================\n");
        printf("CONFIGURACIÓN EXPERIMENTAL:\n");
        printf(" -> Procesos MPI totales: %d\n", num_procs);
        printf(" -> Hilos OpenMP por proceso: %d (Total hilos en clúster: %d)\n", hilos_openmp, num_procs * hilos_openmp);
        printf(" -> Universo Total de Sensores: %d\n", total_sensores);
        printf(" -> Transmisión Continua cada: %ds\n", intervalo_configurado);
        printf("============================================================\n\n");
    }

    /*
     * #pragma omp parallel: Declara una REGIÓN PARALELA.
     * A partir de aquí, el hilo principal se bifurca (fork) en la cantidad de hilos configurada.
     * Cada hilo ejecutará de forma independiente y duplicada el bloque de código encerrado entre llaves.
     */
    #pragma omp parallel
    {
        /*
         * OMP: Obtiene el identificador único del hilo actual dentro de este proceso 
         * (va desde 0 hasta hilos_openmp-1).
         */
        int hilo_id = omp_get_thread_num();
        
        // Crear una semilla única y privada para ESTE hilo específico
        unsigned int semilla_hilo = (unsigned int)(time(NULL) + rank_id + hilo_id);
        
        // Cada hilo maneja su propio cliente y opciones de conexión MQTT de forma privada
        MQTTClient client;
        MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
        char clientId_hilo[60];
        
        // Se genera un ClientID único combinando el ID del proceso MPI y el ID del hilo de OpenMP
        sprintf(clientId_hilo, "%s_%d_H%d", CLIENTID, rank_id, hilo_id);
        MQTTClient_create(&client, ADDRESS, clientId_hilo, MQTTCLIENT_PERSISTENCE_NONE, NULL);
        
        conn_opts.keepAliveInterval = 60;
        conn_opts.cleansession = 1;

        // Intento de conexión al Broker MQTT
        if (MQTTClient_connect(client, &conn_opts) == MQTTCLIENT_SUCCESS) {
            int nro_rafaga = 1;

            // Bucle infinito de simulación continua
            while (1) {
                /*
                 * MPI_Wtime: Devuelve el tiempo actual en segundos (alta precisión) desde un punto fijo en el pasado.
                 * Se usa aquí para medir el rendimiento y calcular cuánto tardó el proceso de envío.
                 */
                double t_inicio_envio = MPI_Wtime();
                double ts_simulador = (double)time(NULL);

                /*
                 * #pragma omp for nowait: Distribuye las iteraciones del ciclo 'for' entre los hilos del equipo actual.
                 * En lugar de que cada hilo haga todo el for, OpenMP divide el rango (inicio_sensor a fin_sensor) equitativamente.
                 * 'nowait' elimina la barrera implícita al final del for, permitiendo que los hilos que terminen primero 
                 * continúen con las siguientes líneas sin esperar a que los demás hilos terminen de enviar.
                 */
                #pragma omp for nowait
                for (int i = inicio_sensor; i <= fin_sensor; i++) {
                    // Generación de coordenadas geográficas aleatorias con pequeños offsets
                    double offset_lat = ((rand_r(&semilla_hilo) % 10000) - 5000) / 50000.0;
                    double offset_lon = ((rand_r(&semilla_hilo) % 10000) - 5000) / 50000.0;
                    double lat = -12.0500 + offset_lat;
                    double lon = -77.0600 + offset_lon;

                    // Simulación de variables ambientales dinámicas
                    double temperature = 18.0 + (rand_r(&semilla_hilo) % 120) / 10.0; // Rango: 18.0°C a 30.0°C
                    double humidity = 60.0 + (rand_r(&semilla_hilo) % 300) / 10.0;    // Rango: 60.0% a 90.0%

                    // Construcción de la cadena de texto en formato JSON
                    char json_payload[320];
                    sprintf(json_payload, "{\"sensor_id\": \"S-%06d\", \"x\": %.5f, \"y\": %.5f, \"temperature\": %.1f, \"humidity\": %.1f, \"timestamp\": %.4f, \"intervalo\": %d, \"total_envio\": %d}",
                            i, lon, lat, temperature, humidity, ts_simulador, intervalo_configurado, total_sensores);

                    // Inicialización y configuración del mensaje MQTT
                    MQTTClient_message pubmsg = MQTTClient_message_initializer;
                    pubmsg.payload = json_payload;
                    pubmsg.payloadlen = (int)strlen(json_payload);
                    pubmsg.qos = QOS;

                    // Publicación síncrona/bloqueante del mensaje hacia el Broker MQTT
                    MQTTClient_deliveryToken token;
                    MQTTClient_publishMessage(client, TOPIC, &pubmsg, &token);
                } // Fin del for mapeado por OpenMP

                // Captura del tiempo final de la ráfaga usando la función de alta precisión de MPI
                double t_fin_envio = MPI_Wtime();
                double tiempo_procesamiento = t_fin_envio - t_inicio_envio;

                // Solo el Proceso Máster (Rank 0) y su Hilo principal (Hilo 0) informan el estado en consola
                if (rank_id == 0 && hilo_id == 0) {
                    printf("[HPC MASTER] Ráfaga del Ciclo #%d inyectada con éxito en %.4f segundos.\n", nro_rafaga, tiempo_procesamiento);
                }
                nro_rafaga++;

                // Control del temporizador: calcula cuánto tiempo queda del segundo configurado para dormir el hilo
                double tiempo_restante = (double)intervalo_configurado - tiempo_procesamiento;
                if (tiempo_restante > 0) {
                    // usleep requiere microsegundos, por lo tanto se multiplica por 1,000,000
                    usleep((useconds_t)(tiempo_restante * 1000000.0));
                }
            } // Fin del while(1)

            // Desconexión y limpieza de los recursos del cliente MQTT (no se alcanzará debido al while(1))
            MQTTClient_disconnect(client, 10);
            MQTTClient_destroy(&client);
        }
    } // Fin de la región paralela. Aquí los hilos se unen (join) volviendo a quedar solo el hilo principal.

    /*
     * MPI_Finalize: Termina formalmente el entorno de ejecución de MPI. 
     * Limpia los recursos y estados de la red de procesos antes de salir de la aplicación.
     */
    MPI_Finalize();
    return 0;
}