# Actividad1.5-MPI-OpenMP
Práctica 1.5 - Búsqueda Paralela Distribuida con MPI y OpenMP - Equipo 7.
**Alumno:** Julio Abraham Angulo Díaz  
**Equipo:** 7  
**Materia:** Computación Paralela / Distribuida  

---

Descripción del Proyecto
Este repositorio contiene la implementación y evidencias de la **Actividad 1.5**, la cual consiste en un sistema híbrido de búsqueda (Secuencial y Binaria) y ordenamiento paralelo utilizando **MPI** (para la distribución por bloques entre nodos en red) y **OpenMP** (para la aceleración multihilo en memoria compartida) en un clúster de 3 nodos.

---

Archivos en el Repositorio
* `main.cpp`: Código fuente completo en C++ (MPI + OpenMP).
* `log_equipo_DESCKTOP-86KB588_nodo_0.txt`: Log de auditoría del Nodo Maestro (Nodo 0).
* `log_equipo_WIN-AVI5NBOUPA5_nodo_1.txt`: Log de auditoría del Nodo Esclavo 1 (Nodo 1).
* `log_equipo_ESCLAVO2_nodo_2.txt`: Log de auditoría del Nodo Esclavo 2 (Nodo 2).
* `Reporte_Actividad_1.5.pdf`: Reporte técnico formal y análisis de rendimiento.

 comando para ejecutar
 mpiexec -hosts 3 Localhost 1 192.168.1.57 1 192.168.1.56 1 practica1.5.exe
