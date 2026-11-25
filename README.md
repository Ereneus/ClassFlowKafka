# ClassFlow — Plataforma Kafka

Plataforma de streaming de eventos de ClassFlow (DSY1107 — EP3). Levanta con Docker Compose un cluster de **3 nodos Zookeeper**, **3 brokers Kafka**, un contenedor que crea los tópicos y **Kafka UI**.

Los productores y consumidores están en el repositorio del backend ([ClassFlowBack](https://github.com/Ereneus/ClassFlowBack)). Kafka complementa al cluster RabbitMQ ([ClassFlowRabbitMQ](https://github.com/Ereneus/ClassFlowRabbitMQ)) y no lo reemplaza.

## Arquitectura

```
                 ┌──────────── classflow-back-net (red Docker compartida) ────────────┐
 zookeeper-1 ─┐  │                                                                     │
 zookeeper-2 ─┼─ ensemble (quorum 2 de 3) ── metadatos, elección del controller         │
 zookeeper-3 ─┘  │                                                                     │
                 │  kafka-1 ─┐                                                          │
                 │  kafka-2 ─┼─ 3 brokers: cada partición tiene 1 líder y 2 réplicas    │
                 │  kafka-3 ─┘                                                          │
                 │  kafka-init → crea/ajusta los tópicos (kafka/create-topics.sh)       │
                 │  kafka-ui   → http://<host>:8090                                     │
                 └─────────────────────────────────────────────────────────────────────┘
        ms-academic, ms-assistance (productores) · ms-notification, ms-analytics (consumidores)
```

| Componente | Imagen | Rol |
|---|---|---|
| `zookeeper-1..3` | `confluentinc/cp-zookeeper:7.7.1` | Guardan los metadatos del cluster (brokers vivos, tópicos, configuración) y eligen al broker *controller*. Con 3 nodos el quorum es 2: el cluster sigue operando si cae uno. |
| `kafka-1..3` | `confluentinc/cp-kafka:7.7.1` | Almacenan el log particionado y replicado. Cada partición tiene un líder que atiende lecturas y escrituras, y 2 réplicas seguidoras. |
| `kafka-init` | `confluentinc/cp-kafka:7.7.1` | Contenedor de un solo uso que crea los tópicos y les aplica su configuración. Es idempotente. |
| `kafka-ui` | `provectuslabs/kafka-ui:v0.7.2` | Consola web: brokers, tópicos, particiones, mensajes, consumer groups, offsets y lag. |

**Durabilidad** (en `docker-compose.yml` y en el script de tópicos):
- Factor de replicación 3 en todos los tópicos, incluidos los internos (`__consumer_offsets`, `__transaction_state`).
- `min.insync.replicas=2`. Junto con `acks=all` en los productores, una escritura se confirma solo cuando la tienen 2 réplicas. Se tolera la caída de 1 broker sin perder datos ni dejar de escribir.
- La creación automática de tópicos está desactivada (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`), así que un nombre mal escrito no crea un tópico por accidente.

**Listeners de cada broker:**

| Listener | Dirección | Uso |
|---|---|---|
| `INTERNAL` | `kafka-N:29092` | Tráfico entre brokers y desde los microservicios (red Docker) |
| `EXTERNAL` | `${KAFKA_EXTERNAL_HOST}:9092 / 9093 / 9094` | Acceso desde el host (IDE, pruebas locales) |

## Tópicos

Están definidos en [kafka/create-topics.sh](kafka/create-topics.sh). Todos tienen 3 particiones, 3 réplicas y `min.insync.replicas=2`.

| Tópico | Key | Política | Caso de uso |
|---|---|---|---|
| `classflow.academic.grades` | `gradeId` | `compact,delete`, 180 días | **Estado de las notas.** La compactación conserva la última versión de cada nota y permite reconstruir el estado completo releyendo el tópico (lo hace `ms-analytics` al arrancar). Los 180 días cubren un semestre. |
| `classflow.assistance.attendance` | `studentId` | `delete`, 30 días o 1 GiB por partición | **Bitácora de asistencia.** Flujo de alto volumen (un evento por estudiante y día). Se borra lo antiguo por tiempo o por tamaño, lo que ocurra primero. Con la key `studentId` los eventos de un estudiante quedan en orden. |
| `classflow.academic.grades.DLT` y `classflow.assistance.attendance.DLT` | la del mensaje original | `delete`, 14 días | Mensajes que los consumidores no pudieron procesar. Se guardan 2 semanas para analizarlos o reprocesarlos. |

**Por qué 3 particiones:** cada consumer group procesa hasta 3 eventos en paralelo (un consumidor por partición), y es igual al número de brokers, así que el liderazgo se reparte uno por broker. **Por qué 3 réplicas:** es el máximo con 3 brokers y, con `min.insync.replicas=2`, soporta perder un broker sin afectar a productores ni consumidores.

**Consumer groups** (definidos en el backend):

| Grupo | Servicio | Propósito |
|---|---|---|
| `classflow-notification` | `ms-notification` | Avisa al estudiante de notas nuevas o corregidas e inasistencias. Confirma el offset por registro. |
| `classflow-analytics` | `ms-analytics` | Calcula indicadores (promedios, asistencia por curso, estudiantes en riesgo). Confirma por lote. |

Cada grupo recibe todos los eventos con su propio offset.

## Puesta en marcha

Requisitos: Docker con Docker Compose v2.

```bash
cp .env.example .env                              # y completar KAFKA_UI_PASSWORD
docker network create classflow-back-net          # una vez por host (compartida con backend y RabbitMQ)
docker compose up -d
docker compose ps                                  # 3 zookeeper, 3 kafka, kafka-ui (kafka-init termina con exit 0)
```

Variables de `.env`:

| Variable | Descripción |
|---|---|
| `KAFKA_EXTERNAL_HOST` | Host con el que los brokers se anuncian fuera de Docker. Local: `localhost`; en EC2: IP pública o DNS de la instancia. |
| `KAFKA_UI_USER` / `KAFKA_UI_PASSWORD` | Login de Kafka UI. Generar la clave con `openssl rand -hex 16`. |

Orden recomendado: 1) esta plataforma, 2) [ClassFlowRabbitMQ](https://github.com/Ereneus/ClassFlowRabbitMQ), 3) el backend. Los microservicios reintentan la conexión, así que el orden no es obligatorio.

**En AWS EC2:** abrir en el Security Group el puerto `8090` (Kafka UI) solo para la IP del administrador. Los puertos `9092-9094` solo hacen falta para conectarse desde fuera de la instancia; los microservicios usan la red interna.

## Supervisión y administración

```bash
# Rol de cada Zookeeper (leader / follower)
for n in 1 2 3; do docker compose exec zookeeper-$n bash -c "echo srvr | nc localhost 2181 | grep Mode"; done

# Particiones, líder, réplicas e ISR de cada tópico
docker compose exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --describe --exclude-internal

# Configuración de un tópico (retención, cleanup policy)
docker compose exec kafka-1 kafka-configs --bootstrap-server kafka-1:29092 --describe --entity-type topics --entity-name classflow.assistance.attendance

# Offsets, lag y distribución del consumo por partición
docker compose exec kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:29092 --describe --group classflow-analytics

# Volver a aplicar la configuración de los tópicos
docker compose run --rm kafka-init
```

Lo mismo se ve en Kafka UI (`http://<host>:8090`): **Brokers**, **Topics → Partitions / Settings / Messages** y **Consumers**. El backend también expone una API de administración (`ms-kafka-admin`, `/api/kafka/**`, solo administradores).

## Pruebas de tolerancia a fallos

- **Caída de un broker:** `docker compose stop kafka-2`. Kafka elige nuevos líderes para sus particiones y el ISR baja a 2. Productores y consumidores siguen operando. Con `docker compose start kafka-2` la réplica se pone al día y vuelve al ISR.
- **Caída de un Zookeeper:** `docker compose stop zookeeper-3`. Los otros 2 mantienen el quorum. Si el detenido era el líder, se elige otro.
- **Consumidor detenido:** detener `ms-analytics` en el backend. En **Consumers** el lag del grupo `classflow-analytics` crece. Al reiniciarlo se pone al día sin perder eventos.

## Estructura

```
.
├── docker-compose.yml       # Zookeeper x3, Kafka x3, kafka-init y kafka-ui
├── kafka/create-topics.sh   # Definición idempotente de los tópicos
├── .env.example             # Variables a copiar en .env
└── README.md
```
