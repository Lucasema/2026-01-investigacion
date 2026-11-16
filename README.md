# Módulo M2 (Clientes) - Calificación de Conductores con Transactional Outbox y Colas Asíncronas

**Desarrollo de Software · UTN FRRe**  
Trabajo Práctico Integrador (TPI) · Plataforma de Movilidad Urbana.  
**Tema:** Colas de mensajes y procesamiento asíncrono (Tema 2) y Patrón Transactional Outbox (Tema 27).

---

## 1. Investigación Conceptual

### 1.1. Colas de Mensajes y Procesamiento Asíncrono

**Definición y Problema que Resuelve**  
En una arquitectura web tradicional basada en comunicación síncrona, cuando un cliente envía una solicitud HTTP a una API, la conexión de red permanece abierta y bloqueada hasta que el servidor ejecuta toda la lógica de negocio y devuelve la respuesta. Si la operación incluye tareas pesadas o que involucran a otros módulos (como recálculo de reputación, auditoría o notificaciones), la demora se traslada directamente al usuario. Este acoplamiento síncrono provoca agotamiento de hilos, saturación de memoria y errores de tiempo de espera agotado (Gateway Timeout).

El uso de colas de mensajes y procesamiento asíncrono resuelve este cuello de botella intercalando un gestor de mensajería (**Message Broker**, como RabbitMQ) entre el servidor web (**Productor**) y el programa encargado de ejecutar la tarea pesada (**Consumidor** o **Worker**). Una cola es una estructura temporal de tipo FIFO (First In, First Out) donde cada tarea viaja encapsulada como un paquete de texto estructurado en formato JSON.

**Desacoplamiento HTTP y Comportamiento ante Fallas del Consumidor**  
El flujo técnico opera en dos fases independientes:
* **Fase 1: Respuesta HTTP inmediata (Código 202 Accepted):** Cuando la API recibe la petición, valida la estructura de los datos de entrada y registra la tarea sin ejecutar el procesamiento pesado. El mensaje ingresa a la cola del broker y la API responde en milisegundos con el código HTTP **202 Accepted**.
* **Fase 2: Retención y recuperación ante caída del Worker:** El broker almacena cada mensaje en disco duro y gestiona su ciclo de vida mediante confirmaciones manuales (`ACK` / `NACK`). Si el contenedor del worker se apaga o falla a mitad de una tarea antes de emitir el `ACK`, el broker detecta la desconexión TCP, saca el mensaje del estado `Unacked` y lo devuelve automáticamente al estado `Ready` al principio de la fila para que no se pierda.

**Características Principales**
* **Desacoplamiento temporal:** El productor y el consumidor no necesitan estar encendidos en el mismo instante.
* **Durabilidad y persistencia:** Tanto la cola (`durable=True`) como los mensajes (`delivery_mode=2`) se graban en disco duro.
* **Control de flujo (Prefetch):** Mediante `prefetch_count=1`, el broker entrega un único mensaje a la vez a cada worker, protegiendo su memoria RAM.

---

### 1.2. Patrón Transactional Outbox

**Definición y Problema que Resuelve**  
En los sistemas distribuidos, un servicio suele necesitar realizar dos acciones conjuntas: modificar el estado en su **Base de Datos** e informar ese suceso publicando un evento en el **Broker de Mensajes**. Dado que la base de datos (PostgreSQL) y el broker (RabbitMQ) son servidores independientes, no comparten una transacción atómica única.

Intentar ejecutar ambas escrituras de forma secuencial dentro del mismo endpoint genera el **Problema de la Doble Escritura (Dual-Write Problem)**:
1. Si se hace `COMMIT` en la base de datos y falla la publicación en RabbitMQ, el registro queda guardado pero el evento se pierde permanentemente.
2. Si se publica primero en el broker y falla la base de datos (`ROLLBACK`), el worker procesará un evento fantasma sobre un dato que no existe.

**Garantía de Consistencia mediante Transacción Local y Proceso Posterior**  
El patrón **Transactional Outbox** soluciona esto dividiendo la operación en dos mecanismos:
* **Mecanismo 1: Escritura atómica en tabla Outbox local:** En el mismo motor de base de datos se crea una tabla adicional llamada `outbox_eventos`. La API abre una única transacción local (`BEGIN`), inserta el registro principal y el evento JSON con estado `PENDIENTE` en la tabla outbox, ejecutando un único `COMMIT` atómico.
* **Mecanismo 2: Retransmisión asíncrona (Message Relay):** Un proceso independiente en segundo plano consulta periódicamente la tabla `outbox_eventos` (`SELECT ... WHERE estado = 'PENDIENTE'`) y publica los mensajes en RabbitMQ. Solo cuando el broker confirma la recepción, el proceso actualiza la fila a estado `PUBLICADO`.

**Idempotencia en el Consumidor**  
El patrón garantiza semántica de entrega **al menos una vez (At-least-once)**: ante una falla del relay justo antes de marcar como `PUBLICADO`, el evento puede enviarse duplicado. Por ello, es obligatorio que el worker consumidor sea **idempotente**, verificando en la base de datos si el identificador ya fue procesado antes de volver a ejecutar la lógica de negocio.

---

## 2. Relación con el Trabajo Práctico Integrador (Plataforma de Movilidad Urbana)

En el marco del Trabajo Práctico Integrador, este repositorio implementa el **Módulo M2: Clientes**, abarcando específicamente el requerimiento **RF-2.5 (Calificación del conductor)**:
* *"El cliente deberá poder registrar una valoración posterior a un viaje completado, evitando duplicados para el mismo viaje"*.

### 2.1. Escenario en la Plataforma
Cuando un cliente califica a un conductor desde su aplicación móvil tras finalizar un viaje, M2 registra la valoración y debe notificar asíncronamente al **Módulo M3 (Conductores y Vehículos)** para actualizar su reputación histórica y evaluar su habilitación (`RF-3.1`, `RF-3.4`), y a **M8 (Notificaciones y Soporte)** para auditar la reseña (`RF-8.8`).

Aplicar **Transactional Outbox** y **Colas Asíncronas** garantiza que:
1. La aplicación del cliente responda de inmediato (`202 Accepted`) sin demoras.
2. Si RabbitMQ sufre una caída en hora pico, la calificación del cliente no se pierda y quede resguardada de forma segura en la tabla outbox de PostgreSQL.
3. Si un mensaje se reenvía por duplicado, el consumidor (Worker de M3) lo detecte mediante control de idempotencia y evite sumar dos veces la misma puntuación al conductor.

---

## 3. Arquitectura de la Solución (Contenedores Docker)

El sistema se compone de 4 servicios integrados mediante Docker Compose:
1. **`db` (PostgreSQL 16):** Almacena la tabla de negocio `calificaciones` y la bandeja de salida `outbox_eventos`.
2. **`broker` (RabbitMQ 3):** Gestiona la cola durable `cola_calificaciones_m2`.
3. **`api` (FastAPI / M2):** Expone los endpoints HTTP en el puerto `8000`.
4. **`relay` y `worker`:** Procesos en segundo plano encargados de la retransmisión outbox y del consumo idempotente con confirmación manual (`ACK`).

---

## 4. Guía de Ejecución y Demostración Práctica

Asegúrate de tener **Docker Desktop** abierto y ejecutándose en tu PC. Abre tu terminal (PowerShell o CMD) en la carpeta raíz del proyecto y ejecuta:

```powershell
docker compose up -d --build
```

- **Panel interactivo Swagger UI (API):** [http://localhost:8000/docs](http://localhost:8000/docs)
- **Consola web de RabbitMQ:** [http://localhost:15672](http://localhost:15672) *(Usuario: `guest`, Clave: `guest`)*
- **Ver logs en tiempo real (con correlación de IDs y pasos):**
  ```powershell
  docker compose logs -f api relay worker
  ```

---

## 5. Guion de Demostración para el Coloquio (5 Actos)

### Acto 1: Flujo Normal y Desacoplamiento HTTP
1. En Swagger ([http://localhost:8000/docs](http://localhost:8000/docs)), despliega el **Botón 1 (`POST /calificaciones`)** y haz clic en *Execute*.
2. Observa que responde de forma instantánea con código **`202 Accepted`**.
3. En la terminal de logs verás la trazabilidad detallada paso a paso (`[CALIFICACIÓN #ID | PASO X]`), mostrando la transacción atómica en la BD de M2, la publicación del Relay y el consumo del Worker.

### Acto 2: Caída del Consumidor (Worker) y Fila en Espera
1. Detén el contenedor del worker:
   ```powershell
   docker compose stop worker
   ```
2. Ejecuta varias veces el **Botón 1** en Swagger. La API seguirá respondiendo `202 Accepted`.
3. Entra a la consola web de RabbitMQ y verás los mensajes acumulados en estado **`Ready`**.
4. Vuelve a encender el worker:
   ```powershell
   docker compose start worker
   ```
   Los mensajes pasarán a `Unacked` y se procesarán secuencialmente de a uno gracias al control de flujo (`prefetch_count=1`).

### Acto 3: Mecanismo ACK y Recuperación ante Caída del Worker
1. Con el sistema procesando un mensaje, mata de golpe al worker a mitad de la operación:
   ```powershell
   docker compose kill worker
   ```
2. Observa en la consola de RabbitMQ que el mensaje que estaba en `Unacked` regresa automáticamente al estado **`Ready`** al cortarse la conexión TCP sin recibir el `ACK`.
3. Vuelve a iniciar el worker (`docker compose start worker`) y verás en los logs el aviso de que el mensaje fue `[REENTREGADO]`.

### Acto 4: Caída del Broker y Patrón Transactional Outbox
1. Apaga RabbitMQ:
   ```powershell
   docker compose stop broker
   ```
2. Prueba el **Botón 2 (`POST /calificaciones/sin-outbox`)**: devolverá un **Error 500** porque se guardó en la BD pero el evento se perdió al estar caído el broker (*problema de doble escritura / dual-write*).
3. Prueba el **Botón 1 (`POST /calificaciones` CON Outbox)**: devolverá **`202 Accepted`** con total normalidad, ya que el evento queda resguardado de forma segura en la tabla `outbox_eventos` con estado `PENDIENTE`.
4. Enciende nuevamente RabbitMQ (`docker compose start broker`): el proceso Relay detectará la reconexión en el siguiente ciclo y publicará los eventos pendientes automáticamente.

### Acto 5: Idempotencia del Consumidor (RF-2.5 y RNF-09)
1. Toma el ID de una calificación completada (por ejemplo, `1`) y ejecuta el **Botón 3 (`POST /calificaciones/{id}/forzar-duplicado`)**.
2. Esto obliga al Relay a reenviar el evento a RabbitMQ.
3. En los logs verás cómo el Worker detecta mediante la base de datos que la calificación ya estaba procesada y muestra el aviso de **`IDEMPOTENCIA`**, descartando el duplicado para no sumar dos veces la puntuación al conductor.

---

## 6. Limpieza del Entorno
Para apagar los contenedores y borrar por completo los volúmenes de la base de datos y la cola:
```powershell
docker compose down -v
```

---

## 7. Referencias Bibliográficas y Documentación Técnica
* **Microservices.io (Chris Richardson):** Pattern *Transactional Outbox* y *Polling Publisher* (`https://microservices.io/patterns/data/transactional-outbox.html`).
* **AWS Prescriptive Guidance:** *Transactional Outbox Pattern* en arquitecturas distribuidas de la nube.
* **Microsoft Learn (Azure Architecture Center):** Patrones *Asynchronous Request-Reply* y *Queue-Based Load Leveling*.
* **RabbitMQ Official Documentation:** *Work Queues*, persistencia durable, confirmaciones manuales y control de flujo (`basic_qos`).