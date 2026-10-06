#include <iostream>
#include <cstdlib>
#include <ctime>
#include <chrono>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <omp.h>
#include <mpi.h>
#include <windows.h>

using namespace std;

const string NOMBRE_EQUIPO = "EQUIPO 7";
const string INTEGRANTE = "ANGULO DIAZ JULIO ABRAHAM";


struct TiemposEjecucion {
    double t_ordenamiento_sec = 0.0;
    double t_ordenamiento_omp = 0.0;
    double t_busqueda_sec_local = 0.0;
    double t_busqueda_sec_dist = 0.0;
    double t_busqueda_bin_local = 0.0;
    double t_busqueda_bin_dist = 0.0;
} g_tiempos;


string obtenerNombreHost() {
    char hostname[256];
    DWORD size = sizeof(hostname);
    if (GetComputerNameA(hostname, &size)) {
        return string(hostname);
    }
    return "HOST_DESCONOCIDO";
}


void inicializarLogNodo(ofstream& log_file, int rank, int total_procs, int hilos_omp, string host_name) {
    stringstream ss;
    ss << "log_equipo_" << host_name << "_nodo_" << rank << ".txt";
    log_file.open(ss.str().c_str(), ios::out | ios::trunc);

    log_file << "==========================================================================" << endl;
    log_file << "REGISTRO DE AUDITORIA - PRACTICA 1.5: BUSQUEDA PARALELA DISTRIBUIDA" << endl;
    log_file << "EQUIPO: " << NOMBRE_EQUIPO << " | INTEGRANTE: " << INTEGRANTE << endl;
    log_file << "==========================================================================" << endl;
    log_file << "Host Name         : " << host_name << endl;
    log_file << "Nodo MPI (Rank)   : " << rank << " de " << total_procs << endl;
    log_file << "Hilos OpenMP/Nodo : " << hilos_omp << endl;
    log_file << "==========================================================================" << endl << endl;
    log_file.flush();
}


void merge(int* arr, int l, int m, int r) {
    int n1 = m - l + 1;
    int n2 = r - m;
    int* L = new int[n1];
    int* R = new int[n2];

    for (int i = 0; i < n1; i++) L[i] = arr[l + i];
    for (int j = 0; j < n2; j++) R[j] = arr[m + 1 + j];

    int i = 0, j = 0, k = l;
    while (i < n1 && j < n2) {
        if (L[i] <= R[j]) arr[k++] = L[i++];
        else arr[k++] = R[j++];
    }
    while (i < n1) arr[k++] = L[i++];
    while (j < n2) arr[k++] = R[j++];

    delete[] L;
    delete[] R;
}

void parallel_merge_sort(int* arr, int l, int r, int depth = 0) {
    if (l < r) {
        int m = l + (r - l) / 2;
        if (depth < 4 && (r - l) > 2000) {
            #pragma omp parallel sections
            {
                #pragma omp section
                parallel_merge_sort(arr, l, m, depth + 1);
                #pragma omp section
                parallel_merge_sort(arr, m + 1, r, depth + 1);
            }
        } else {
            sort(arr + l, arr + r + 1);
            return;
        }
        merge(arr, l, m, r);
    }
}

// Búsqueda Secuencial Local (OpenMP)
int busquedaSecuencialLocal(int* arr, int n, int target, ofstream& log_file, bool modo_detallado, string host_name) {
    int pos_encontrada = -1;

    #pragma omp parallel
    {
        int thread_id = omp_get_thread_num();
        #pragma omp for nowait
        for (int i = 0; i < n; i++) {
            if (arr[i] == target) {
                #pragma omp critical
                {
                    pos_encontrada = i;
                }
                if (modo_detallado) {
                    #pragma omp critical
                    {
                        log_file << "[Equipo: " << host_name << "] [Nodo MPI: 0] [Hilo OpenMP: " << thread_id
                                 << "] [Busqueda Secuencial Local] -> Hallado en indice: " << i << endl;
                    }
                }
            }
        }
    }
    return pos_encontrada;
}

// Búsqueda Binaria Local en Arreglo Ordenado (OpenMP)
int busquedaBinariaLocal(int* arr, int n, int target, ofstream& log_file, bool modo_detallado, string host_name) {
    int pos_encontrada = -1;

    #pragma omp parallel
    {
        int thread_id = omp_get_thread_num();
        int num_threads = omp_get_num_threads();
        int chunk = n / num_threads;
        int inicio = thread_id * chunk;
        int fin = (thread_id == num_threads - 1) ? n - 1 : (inicio + chunk - 1);

        if (target >= arr[inicio] && target <= arr[fin]) {
            int low = inicio, high = fin;
            while (low <= high) {
                int mid = low + (high - low) / 2;
                if (arr[mid] == target) {
                    #pragma omp critical
                    {
                        pos_encontrada = mid;
                    }
                    if (modo_detallado) {
                        #pragma omp critical
                        {
                            log_file << "[Equipo: " << host_name << "] [Nodo MPI: 0] [Hilo OpenMP: " << thread_id
                                     << "] [Bloque: " << inicio << "-" << fin
                                     << "] [Busqueda Binaria Local] -> Hallado en Indice Global: " << mid << endl;
                        }
                    }
                    break;
                }
                if (arr[mid] < target) low = mid + 1;
                else high = mid - 1;
            }
        }
    }
    return pos_encontrada;
}

int main(int argc, char* argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &provided);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int num_hilos = 4;
    omp_set_num_threads(num_hilos);

    string host_name = obtenerNombreHost();
    ofstream log_file;
    inicializarLogNodo(log_file, rank, size, num_hilos, host_name);

    int N = 0;
    int target = 0;
    bool ordenado = false;
    bool modo_detallado = false;

    int* arr_global = NULL;
    int* local_arr = NULL;

    if (rank == 0) {
        cout << "==========================================================================" << endl;
        cout << "  PRACTICA 1.5: BUSQUEDA PARALELA DISTRIBUIDA (MPI + OpenMP)" << endl;
        cout << "  " << INTEGRANTE << " - " << NOMBRE_EQUIPO << endl;
        cout << "==========================================================================" << endl;
    }

    int opcion = -1;
    do {
        if (rank == 0) {
            cout << "\n---------------- MENU PRINCIPAL ----------------" << endl;
            cout << "1. Generar Arreglo Dinamico (Aleatorio)" << endl;
            cout << "2. Ordenar Arreglo (Secuencial vs OpenMP Parallel Merge Sort)" << endl;
            cout << "3. Ejecutar Busqueda Secuencial (Local vs Distribuida)" << endl;
            cout << "4. Ejecutar Busqueda Binaria (Local vs Distribuida)" << endl;
            cout << "5. Mostrar Tiempos y Comparativa de Rendimiento" << endl;
            cout << "0. Salir" << endl;
            cout << "Seleccione una opcion: ";
            cin >> opcion;
        }

        MPI_Bcast(&opcion, 1, MPI_INT, 0, MPI_COMM_WORLD);

        switch (opcion) {
            case 1: { // Generar Arreglo Dinámico
                if (rank == 0) {
                    cout << "\n--- GENERACION DE ARREGLO DINAMICO ---" << endl;
                    cout << "1. Prueba de Control / Reducida (100 elementos - Logs detallados)" << endl;
                    cout << "2. Prueba Masiva de Rendimiento (10,000,000 elementos)" << endl;
                    cout << "Seleccione el tipo de prueba: ";
                    int sub_opc;
                    cin >> sub_opc;

                    if (sub_opc == 1) {
                        N = 100;
                        modo_detallado = true;
                    } else {
                        N = 10000000;
                        modo_detallado = false;
                    }

                    if (arr_global != NULL) delete[] arr_global;
                    arr_global = new int[N];

                    srand(12345);
                    for (int i = 0; i < N; i++) {
                        arr_global[i] = rand() % (N * 3);
                    }

                    target = arr_global[N / 2];
                    ordenado = false;

                    cout << "-> Arreglo asignado dinamicamente con " << N << " elementos." << endl;
                    cout << "-> Elemento Objetivo (Target) configurado en: " << target << endl;
                }

                MPI_Bcast(&N, 1, MPI_INT, 0, MPI_COMM_WORLD);
                MPI_Bcast(&target, 1, MPI_INT, 0, MPI_COMM_WORLD);
                MPI_Bcast(&modo_detallado, 1, MPI_C_BOOL, 0, MPI_COMM_WORLD);

                log_file << "[ACCION] Generacion de arreglo dinamico completada. N = " << N
                         << " | Target = " << target << endl;
                break;
            }

            case 2: { // Ordenar Arreglo (Secuencial vs OpenMP)
                if (N == 0) {
                    if (rank == 0) cout << "[!] Debe generar el arreglo primero (Opcion 1)." << endl;
                    break;
                }

                if (rank == 0) {
                    cout << "\n--- COMPARATIVA DE ORDENAMIENTO (SECUENCIAL VS OPENMP) ---" << endl;

                    // 1. Copia para medir Ordenamiento Secuencial
                    int* arr_copia = new int[N];
                    for (int i = 0; i < N; i++) arr_copia[i] = arr_global[i];

                    double t_sec_start = MPI_Wtime();
                    sort(arr_copia, arr_copia + N);
                    double t_sec_end = MPI_Wtime();
                    g_tiempos.t_ordenamiento_sec = t_sec_end - t_sec_start;
                    delete[] arr_copia;

                    // 2. Ordenamiento Paralelo OpenMP sobre el arreglo principal
                    double t_omp_start = MPI_Wtime();
                    parallel_merge_sort(arr_global, 0, N - 1);
                    double t_omp_end = MPI_Wtime();
                    g_tiempos.t_ordenamiento_omp = t_omp_end - t_omp_start;

                    ordenado = true;

                    cout << "-> Ordenamiento Secuencial (std::sort)       : " << fixed << setprecision(6) << g_tiempos.t_ordenamiento_sec << " s" << endl;
                    cout << "-> Ordenamiento Paralelo (Merge Sort OpenMP)  : " << fixed << setprecision(6) << g_tiempos.t_ordenamiento_omp << " s" << endl;
                    if (g_tiempos.t_ordenamiento_omp > 0) {
                        cout << "-> Speedup de Ordenamiento OpenMP             : " << (g_tiempos.t_ordenamiento_sec / g_tiempos.t_ordenamiento_omp) << "x" << endl;
                    }
                }

                MPI_Bcast(&ordenado, 1, MPI_C_BOOL, 0, MPI_COMM_WORLD);
                log_file << "[ACCION] Ordenamiento completado. Estado Ordenado: " << ordenado << endl;
                break;
            }

            case 3: { // Búsqueda Secuencial (Local vs Distribuida)
                if (N == 0) {
                    if (rank == 0) cout << "[!] Debe generar el arreglo primero (Opcion 1)." << endl;
                    break;
                }

                if (rank == 0) {
                    double t0 = MPI_Wtime();
                    int res_local = busquedaSecuencialLocal(arr_global, N, target, log_file, modo_detallado, host_name);
                    double t1 = MPI_Wtime();
                    g_tiempos.t_busqueda_sec_local = t1 - t0;

                    cout << "\n[LOCAL OPENMP] Busqueda Secuencial -> Indice: " << res_local
                         << " | Tiempo: " << g_tiempos.t_busqueda_sec_local << " s" << endl;
                }

                int local_n = N / size;
                if (local_arr != NULL) delete[] local_arr;
                local_arr = new int[local_n];

                MPI_Barrier(MPI_COMM_WORLD);
                double t_dist_start = MPI_Wtime();

                MPI_Scatter(arr_global, local_n, MPI_INT, local_arr, local_n, MPI_INT, 0, MPI_COMM_WORLD);

                int pos_dist_local = -1;
                int offset_global = rank * local_n;

                #pragma omp parallel
                {
                    int thread_id = omp_get_thread_num();
                    #pragma omp for nowait
                    for (int i = 0; i < local_n; i++) {
                        if (local_arr[i] == target) {
                            int idx_g = offset_global + i;
                            #pragma omp critical
                            {
                                pos_dist_local = idx_g;
                            }
                            if (modo_detallado) {
                                #pragma omp critical
                                {
                                    log_file << "[Equipo: " << host_name << "] [Nodo MPI: " << rank
                                             << "] [Hilo OpenMP: " << thread_id
                                             << "] [Bloque: " << offset_global << "-" << (offset_global + local_n - 1)
                                             << "] [Busqueda Secuencial Distribuida] -> Hallado en Indice Global: " << idx_g << endl;
                                }
                            }
                        }
                    }
                }

                int global_found_idx = -1;
                MPI_Reduce(&pos_dist_local, &global_found_idx, 1, MPI_INT, MPI_MAX, 0, MPI_COMM_WORLD);

                MPI_Barrier(MPI_COMM_WORLD);
                double t_dist_end = MPI_Wtime();

                if (rank == 0) {
                    g_tiempos.t_busqueda_sec_dist = t_dist_end - t_dist_start;
                    cout << "[DISTRIBUIDA MPI+OpenMP] Busqueda Secuencial -> Indice Global: " << global_found_idx
                         << " | Tiempo: " << g_tiempos.t_busqueda_sec_dist << " s" << endl;
                }
                break;
            }

            case 4: { // Búsqueda Binaria (Local vs Distribuida)
                if (N == 0 || !ordenado) {
                    if (rank == 0) cout << "[!] El arreglo debe estar generado (Opcion 1) y ORDENADO (Opcion 2) previamente." << endl;
                    break;
                }

                if (rank == 0) {
                    double t0 = MPI_Wtime();
                    int res_bin_local = busquedaBinariaLocal(arr_global, N, target, log_file, modo_detallado, host_name);
                    double t1 = MPI_Wtime();
                    g_tiempos.t_busqueda_bin_local = t1 - t0;

                    cout << "\n[LOCAL OPENMP] Busqueda Binaria -> Indice Global: " << res_bin_local
                         << " | Tiempo: " << g_tiempos.t_busqueda_bin_local << " s" << endl;
                }

                int local_n = N / size;
                if (local_arr != NULL) delete[] local_arr;
                local_arr = new int[local_n];

                MPI_Barrier(MPI_COMM_WORLD);
                double t_dist_start = MPI_Wtime();

                MPI_Scatter(arr_global, local_n, MPI_INT, local_arr, local_n, MPI_INT, 0, MPI_COMM_WORLD);

                int pos_dist_bin_local = -1;
                int offset_global = rank * local_n;

                if (target >= local_arr[0] && target <= local_arr[local_n - 1]) {
                    #pragma omp parallel
                    {
                        int thread_id = omp_get_thread_num();
                        int num_threads = omp_get_num_threads();
                        int chunk = local_n / num_threads;
                        int inicio = thread_id * chunk;
                        int fin = (thread_id == num_threads - 1) ? local_n - 1 : (inicio + chunk - 1);

                        if (target >= local_arr[inicio] && target <= local_arr[fin]) {
                            int low = inicio, high = fin;
                            while (low <= high) {
                                int mid = low + (high - low) / 2;
                                if (local_arr[mid] == target) {
                                    int idx_g = offset_global + mid;
                                    #pragma omp critical
                                    {
                                        pos_dist_bin_local = idx_g;
                                    }
                                    if (modo_detallado) {
                                        #pragma omp critical
                                        {
                                            log_file << "[Equipo: " << host_name << "] [Nodo MPI: " << rank
                                                     << "] [Hilo OpenMP: " << thread_id
                                                     << "] [Bloque: " << (offset_global + inicio) << "-" << (offset_global + fin)
                                                     << "] [Busqueda Binaria Distribuida] -> Hallado en Indice Global: " << idx_g << endl;
                                        }
                                    }
                                    break;
                                }
                                if (local_arr[mid] < target) low = mid + 1;
                                else high = mid - 1;
                            }
                        }
                    }
                }

                int global_bin_found_idx = -1;
                MPI_Reduce(&pos_dist_bin_local, &global_bin_found_idx, 1, MPI_INT, MPI_MAX, 0, MPI_COMM_WORLD);

                MPI_Barrier(MPI_COMM_WORLD);
                double t_dist_end = MPI_Wtime();

                if (rank == 0) {
                    g_tiempos.t_busqueda_bin_dist = t_dist_end - t_dist_start;
                    cout << "[DISTRIBUIDA MPI+OpenMP] Busqueda Binaria -> Indice Global: " << global_bin_found_idx
                         << " | Tiempo: " << g_tiempos.t_busqueda_bin_dist << " s" << endl;
                }
                break;
            }

            case 5: { // Mostrar Tiempos y Comparativa de Rendimiento
                if (rank == 0) {
                    cout << "\n==========================================================================" << endl;
                    cout << "              RESUMEN Y COMPARATIVA DE RENDIMIENTO (MPI + OpenMP)" << endl;
                    cout << "==========================================================================" << endl;
                    cout << " Tamano del Arreglo (N) : " << N << " elementos" << endl;
                    cout << " Elemento Objetivo (Target): " << target << endl;
                    cout << "--------------------------------------------------------------------------" << endl;
                    cout << " Ordenamiento Secuencial (std::sort)             : " << g_tiempos.t_ordenamiento_sec << " s" << endl;
                    cout << " Ordenamiento Paralelo (Merge Sort OpenMP)        : " << g_tiempos.t_ordenamiento_omp << " s" << endl;
                    if (g_tiempos.t_ordenamiento_omp > 0)
                        cout << " -> Speedup Ordenamiento OpenMP                  : " << (g_tiempos.t_ordenamiento_sec / g_tiempos.t_ordenamiento_omp) << "x" << endl;
                    cout << "--------------------------------------------------------------------------" << endl;
                    cout << " Busqueda Secuencial Local (OpenMP 4 hilos)       : " << g_tiempos.t_busqueda_sec_local << " s" << endl;
                    cout << " Busqueda Secuencial Distribuida (MPI + OpenMP)   : " << g_tiempos.t_busqueda_sec_dist << " s" << endl;
                    if (g_tiempos.t_busqueda_sec_dist > 0)
                        cout << " -> Speedup Secuencial Distribuido: " << (g_tiempos.t_busqueda_sec_local / g_tiempos.t_busqueda_sec_dist) << "x" << endl;
                    cout << "--------------------------------------------------------------------------" << endl;
                    cout << " Busqueda Binaria Local (OpenMP 4 hilos)          : " << g_tiempos.t_busqueda_bin_local << " s" << endl;
                    cout << " Busqueda Binaria Distribuida (MPI + OpenMP)      : " << g_tiempos.t_busqueda_bin_dist << " s" << endl;
                    if (g_tiempos.t_busqueda_bin_dist > 0)
                        cout << " -> Speedup Binario Distribuido: " << (g_tiempos.t_busqueda_bin_local / g_tiempos.t_busqueda_bin_dist) << "x" << endl;
                    cout << "==========================================================================" << endl;
                }
                break;
            }

            case 0: { // Salir
                if (rank == 0) {
                    cout << "\nFinalizando programa..." << endl;
                    cout << "INTEGRANTE: " << INTEGRANTE << " - " << NOMBRE_EQUIPO << endl;
                }
                break;
            }

            default:
                if (rank == 0) cout << "Opcion invalida." << endl;
                break;
        }

    } while (opcion != 0);

    if (arr_global != NULL) delete[] arr_global;
    if (local_arr != NULL) delete[] local_arr;

    log_file << "Cierre del proceso de ejecucion en el Nodo " << rank << "." << endl;
    log_file.close();

    MPI_Finalize();
    return 0;
}
