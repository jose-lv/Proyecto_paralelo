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

    // Contador de mensajes fallidos, compartido por todos los hilos del proceso.
    // Se actualiza con #pragma omp atomic, así que no necesita mutex explícito.
    long mensajes_fallidos_proceso = 0;
    int nro_rafaga = 1; // Compartido: solo lo toca el hilo master, protegido por la barrera.

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

        int conectado = (MQTTClient_connect(client, &conn_opts) == MQTTCLIENT_SUCCESS);
        if (!conectado) {
            // Antes: si fallaba la conexión, el hilo quedaba mudo sin avisar a nadie.
            fprintf(stderr, "[Proceso %d - Hilo %d] ERROR: no se pudo conectar al broker MQTT.\n", rank_id, hilo_id);
        }

        double t_inicio_envio = 0.0;

        while (conectado) {

            // El hilo master mide el inicio de la ráfaga y resetea el contador de esta ráfaga.
            #pragma omp master
            {
                t_inicio_envio = MPI_Wtime();
            }
            // Barrera: nadie empieza a publicar hasta que t_inicio_envio esté fijado.
            #pragma omp barrier

            // CAMBIO CLAVE: se quitó "nowait". #pragma omp for trae una barrera implícita
            // al final, así que TODOS los hilos del proceso terminan su rango de sensores
            // antes de que cualquiera pueda seguir de largo hacia la siguiente ráfaga.
            // Con "nowait" un hilo rápido podía re-entrar al `for` de la ráfaga N+1 mientras
            // otros hilos del mismo equipo seguían en la ráfaga N: eso rompe la construcción
            // de reparto de trabajo de OpenMP y produce publicaciones perdidas/mezcladas.
            #pragma omp for
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
                        i, lon, lat, temperature, humidity, (double)time(NULL), intervalo_configurado, total_sensores);

                MQTTClient_message pubmsg = MQTTClient_message_initializer;
                pubmsg.payload = json_payload;
                pubmsg.payloadlen = (int)strlen(json_payload);
                pubmsg.qos = QOS;

                MQTTClient_deliveryToken token;
                int rc = MQTTClient_publishMessage(client, TOPIC, &pubmsg, &token);

                // CAMBIO: antes no se revisaba el retorno; con QoS 0 no hay ACK, pero
                // MQTTClient_publishMessage sí puede fallar localmente (p. ej. buffer lleno,
                // socket caído). Ahora lo contamos de forma thread-safe.
                if (rc != MQTTCLIENT_SUCCESS) {
                    #pragma omp atomic
                    mensajes_fallidos_proceso++;
                }
            }
            // <- Barrera implícita del "#pragma omp for" de arriba: todos llegan aquí
            //    antes de que el master siga.

            // CAMBIO CLAVE: antes, TODOS los hilos de TODOS los procesos llamaban a
            // MPI_Reduce, lo cual es un uso frágil e ineficiente de una operación colectiva.
            // Ahora solo el hilo master de cada proceso participa en el Reduce.
            #pragma omp master
            {
                double t_fin_envio_local = MPI_Wtime();
                double tiempo_procesamiento_local = t_fin_envio_local - t_inicio_envio;
                double tiempo_total_maximo;
                long fallidos_totales;

                // Reduce de tiempos (máximo = cuello de botella) y de mensajes fallidos (suma),
                // uno por proceso MPI, tal como espera la operación colectiva.
                MPI_Reduce(&tiempo_procesamiento_local, &tiempo_total_maximo, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
                MPI_Reduce(&mensajes_fallidos_proceso, &fallidos_totales, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

                if (rank_id == 0) {
                    printf("Ráfaga #%d finalizada en %.4f segundos. Mensajes fallidos (publish) en el clúster: %ld\n",
                           nro_rafaga, tiempo_total_maximo, fallidos_totales);
                }
                nro_rafaga++;
                mensajes_fallidos_proceso = 0; // Reinicia el contador para la siguiente ráfaga.

                // Sincronización temporal entre ráfagas para que estas inicien después del intervalo_configurado
                double tiempo_restante = (double)intervalo_configurado - tiempo_total_maximo;
                if (tiempo_restante > 0) {
                    usleep((useconds_t)(tiempo_restante * 1000000.0));
                }
            }
            // CAMBIO: "omp master" NO trae barrera implícita al final. Sin este barrier,
            // los hilos que no son master podrían adelantarse a la siguiente ráfaga mientras
            // el master todavía está calculando el reduce o durmiendo el intervalo restante.
            #pragma omp barrier
        }

        if (conectado) {
            MQTTClient_disconnect(client, 10);
        }
        MQTTClient_destroy(&client);
    }

    // MPI: Cierra y libera los recursos del entorno de comunicación distribuida
    MPI_Finalize();
    return 0;
}
