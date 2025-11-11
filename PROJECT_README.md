# Data Lakehouse de E-commerce: Pipeline con Arquitectura Medallion

## Contexto de Negocio: Los Dolores de Crecimiento de FastCart Inc.

FastCart Inc., una empresa de comercio electrónico en rápido crecimiento, comenzó hace cinco años con una visión simple: hacer que las compras en línea sean sin esfuerzo. Lo que empezó como un pequeño equipo enviando 100 pedidos por día ha explotado en un negocio próspero que procesa más de 50,000 pedidos diarios en 15 categorías de productos.

Pero el éxito trajo desafíos imprevistos.

### El Problema: Caos en los Datos

A medida que FastCart escalaba, su infraestructura de datos no pudo seguir el ritmo. Múltiples equipos construyeron sus propios pipelines de datos, creando un ecosistema fragmentado donde:

**Los equipos de Inteligencia de Negocios** luchan con reportes inconsistentes. Los números de ventas del dashboard de marketing no coinciden con los reportes de finanzas. Los gerentes de producto no pueden confiar en las métricas que usan para tomar decisiones millonarias de inventario.

**Los Ingenieros de Datos** pasan 60% de su tiempo apagando incendios. Problemas de calidad de datos se cuelan—pedidos duplicados, timestamps faltantes, totales incorrectos. Cada departamento mantiene bases de datos separadas, y reconciliarlas es una pesadilla.

**La Gerencia Ejecutiva** toma decisiones a ciegas. Para cuando reciben reportes semanales, los datos ya están obsoletos. No pueden rastrear el ciclo de vida de pedidos en tiempo real, perdiendo señales críticas sobre fallas de pago, picos de cancelación o cuellos de botella en envíos.

**La Experiencia del Cliente sufre**. Los pedidos se pierden en el limbo de procesamiento. El servicio al cliente no puede dar actualizaciones precisas de estado porque los datos están dispersos en múltiples sistemas con retrasos de sincronización.

### El Costo Real

- **$2M de pérdida anual** por procesamiento duplicado de pedidos y desajustes de inventario
- **45% de proyectos de analítica retrasados** debido a problemas de calidad de datos
- **Retraso de 3-5 días** antes de que ejecutivos vean métricas clave de negocio
- **30% del tiempo de ingeniería** gastado en apagar incendios de datos en lugar de innovación
- **Pérdida de ventaja competitiva** mientras competidores basados en datos se mueven más rápido

### La Solución: Un Data Lakehouse Moderno

FastCart necesita una base de datos escalable y confiable—una única fuente de verdad construida sobre mejores prácticas de la industria. Entra la **Arquitectura Medallion**: un enfoque de múltiples capas que transforma datos crudos en insights confiables y listos para el negocio.

Este proyecto implementa esa visión usando tecnologías modernas de big data:
- **Apache Kafka** para streaming de eventos en tiempo real
- **Apache Spark** para procesamiento distribuido de datos
- **Delta Lake** para almacenamiento de datos compatible con ACID
- **Hadoop** como capa de almacenamiento escalable

¿El resultado? Datos limpios, analítica rápida y toma de decisiones con confianza en todos los niveles.

---

## Descripción General del Proyecto

### ¿Qué es la Arquitectura Medallion?

La Arquitectura Medallion es un patrón de diseño de datos que mejora progresivamente la calidad de los datos a medida que fluyen a través de tres capas distintas:

```
Kafka Stream → Capa Bronze → Capa Silver → Capa Gold → Analítica/BI
   (Crudo)    (Ingesta Cruda) (Limpiado)   (Agregado)   (Insights)
```

**Capa Bronze (Zona Cruda)**
- Propósito: Aterrizar todos los datos crudos exactamente como se reciben de los sistemas fuente
- Características: Sin transformaciones, preservar estructura original, solo agregar
- Almacenamiento: Tablas Delta Lake particionadas por fecha de ingesta
- Beneficio: Registro histórico completo, permite replay de datos y auditoría

**Capa Silver (Zona Limpia)**
- Propósito: Crear datos limpios, validados y conformados
- Características: Conversiones de tipos de datos, deduplicación, manejo de nulos, estructuras aplanadas
- Almacenamiento: Tablas Delta Lake particionadas por dimensiones de negocio
- Beneficio: Datos de alta calidad listos para analítica de propósito general

**Capa Gold (Zona de Negocio)**
- Propósito: Entregar datasets agregados y específicos del negocio
- Características: Métricas pre-calculadas, tablas de dimensiones, optimizado para consultas
- Almacenamiento: Tablas Delta Lake optimizadas para casos de uso específicos
- Beneficio: Analítica rápida, lógica de negocio consistente, listo para autoservicio

### Stack Tecnológico

| Componente | Tecnología | Propósito |
|-----------|-----------|---------|
| Fuente de Datos | Generador de Ventas Python | Simula transacciones de e-commerce |
| Message Broker | Apache Kafka | Streaming de eventos en tiempo real |
| Motor de Procesamiento | Apache Spark (Batch) | Transformaciones distribuidas de datos |
| Formato de Almacenamiento | Delta Lake | Transacciones ACID, time travel, evolución de esquemas |
| Capa de Almacenamiento | Hadoop HDFS | Almacenamiento escalable y tolerante a fallos |
| Orquestación | Manual/Programado | Ejecuciones de procesamiento batch |

### Arquitectura del Pipeline

```
┌─────────────────────┐
│ Generador de Ventas │ → Produce eventos de pedidos (pending → confirmed → processing → shipped)
└────────┬────────────┘
         ↓
┌────────────────┐
│  Tópico Kafka  │ → ecommerce-sales (particionado, replicado)
└────────┬───────┘
         ↓
┌────────────────────────────────────────────────────────────┐
│                    PIPELINE SPARK                          │
│                                                            │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────┐  │
│  │ Capa Bronze  │ →   │ Capa Silver  │ →   │   Gold   │  │
│  │ (Datos Crudos)│     │  (Limpiado)  │     │(Métricas)│  │
│  └──────────────┘     └──────────────┘     └──────────┘  │
│         ↓                     ↓                   ↓       │
│  Tablas Delta          Tablas Delta       Tablas Delta   │
└────────────────────────────────────────────────────────────┘
         ↓                     ↓                   ↓
    ┌──────────────────────────────────────────────┐
    │      Almacenamiento HDFS / Delta Lake        │
    └──────────────────────────────────────────────┘
```

---

## Objetivos del Proyecto

Al completar este proyecto, usted:

1. **Implementará la Capa Bronze**: Ingerir datos de streaming crudos desde Kafka en Delta Lake sin modificaciones
2. **Construirá la Capa Silver**: Aplicar transformaciones de calidad de datos y crear datasets limpios
3. **Creará la Capa Gold**: Desarrollar agregaciones específicas del negocio para dos casos de uso
4. **Dominará Delta Lake**: Usar transacciones ACID, particionamiento y evolución de esquemas
5. **Trabajará con Spark**: Procesar datos a gran escala usando patrones de procesamiento batch
6. **Aplicará Mejores Prácticas de Ingeniería de Datos**: Implementar manejo apropiado de errores, logging y validación de datos

---

## Requisitos de Implementación del Pipeline

### Capa Bronze: Ingesta de Datos Crudos

**Objetivo**: Capturar todos los eventos de pedidos entrantes desde Kafka y persistirlos en su forma original.

**Input**: Tópico de Kafka `ecommerce-sales`

**Requisitos**:

1. Leer mensajes del tópico de Kafka usando el conector Kafka de Spark
2. Ingerir TODOS los campos del payload JSON sin ninguna transformación
3. Agregar metadata de ingesta:
   - `ingestion_timestamp`: Cuándo el registro fue escrito en Bronze
   - `ingestion_date`: Partición de fecha (formato YYYY-MM-DD)
4. Guardar como tabla Delta Lake en la ubicación: `/user/hadoop/bronze/ecommerce_sales/`
5. Particionar por `ingestion_date`
6. Habilitar características de Delta Lake:
   - Evolución de esquema (mergeSchema)
   - Escrituras solo de agregar (append-only)
7. Manejar escrituras duplicadas de forma idempotente

**Validación de Datos**:
- Asegurar que no se pierdan registros durante la ingesta
- Verificar que todos los campos JSON se preserven
- Validar el rastreo de offsets de Kafka

**Esquema Esperado** (preservar todos los campos originales más):

```
order_id: string
timestamp: string
product_id: string
product_name: string
quantity: integer
price: double
total: double
customer_id: string
customer_email: string
payment_method: string
shipping_address: struct (street, city, state, zip_code, country)
status: string
cancellation_reason: string (nullable)
ingestion_timestamp: timestamp
ingestion_date: date
```

---

### Capa Silver: Limpieza y Transformación de Datos

**Objetivo**: Transformar datos de Bronze en un formato limpio, validado y listo para analítica.

**Input**: Tabla Delta Lake de la capa Bronze

**Requisitos**:

**1. Conversiones de Tipos de Datos**

- Convertir el campo `timestamp` de string a tipo timestamp apropiado
- Asegurar que `price` y `total` sean tipos decimal/double
- Convertir `quantity` a tipo integer
- Convertir `ingestion_date` a tipo date

**2. Deduplicación**

- Remover registros duplicados basados en la combinación única de `order_id` + `timestamp`
- Para duplicados, mantener el registro con el `ingestion_timestamp` más reciente

**3. Manejo de Nulos**

- Identificar y manejar valores nulos en campos críticos:
  - `order_id`: No debe ser null (rechazar registro)
  - `customer_id`: No debe ser null (rechazar registro)
  - `total`: No debe ser null o cero (rechazar registro)
  - `cancellation_reason`: Mantener null (solo poblado para pedidos cancelados)
- Crear una tabla de registros rechazados para datos inválidos

**4. Aplanamiento de Datos**

- Aplanar el struct anidado `shipping_address` en columnas individuales:
  - `shipping_street`
  - `shipping_city`
  - `shipping_state`
  - `shipping_zip_code`
  - `shipping_country`

**5. Agregar Columnas de Metadata**

- `silver_ingestion_timestamp`: Cuándo el registro fue escrito en Silver
- `silver_ingestion_date`: Partición de fecha para la capa Silver
- `data_quality_score`: Flag booleano (pasa/falla basado en reglas de validación)

**6. Reglas de Validación de Datos**

- `total` debe ser igual a `price` * `quantity` (con tolerancia de 0.01)
- `status` debe ser uno de: pending, confirmed, processing, shipped, cancelled
- `timestamp` debe ser una fecha válida (no fecha futura)

**Output**:

- Tabla principal en `/user/hadoop/silver/ecommerce_sales/`
- Tabla de registros rechazados en `/user/hadoop/silver/ecommerce_sales_rejected/`
- Particionar tabla principal por `silver_ingestion_date` y `status`

---

### Capa Gold - Caso de Uso 1: Métricas de Desempeño de Ventas

**Objetivo**: Crear tablas agregadas para monitoreo y reportes de desempeño de ventas.

**Input**: Tabla Delta Lake de la capa Silver

**Requisitos**: Construir las siguientes tablas de métricas:

**Tabla 1: Ingresos Diarios por Producto**

- Agregación: Nivel diario, por producto
- Métricas a calcular:
  - `total_revenue`: Suma de todos los totales de pedidos para el producto
  - `total_orders`: Conteo de pedidos distintos
  - `total_quantity_sold`: Suma de cantidades
  - `avg_order_value`: Valor promedio de pedido
  - `min_price`: Precio mínimo observado
  - `max_price`: Precio máximo observado
- Dimensiones: `product_id`, `product_name`, `date`
- Filtro: Solo incluir pedidos enviados exitosamente
- Ordenar: Por date DESC, revenue DESC

**Tabla 2: Tendencias de Pedidos por Hora**

- Agregación: Nivel horario a través de todos los productos
- Métricas a calcular:
  - `orders_created_count`: Conteo de nuevos pedidos (estado pending)
  - `orders_completed_count`: Conteo de pedidos enviados
  - `revenue_per_hour`: Suma de total para pedidos enviados
  - `avg_processing_time_minutes`: Tiempo promedio de pending a shipped (si aplica)
- Dimensiones: `date`, `hour`
- Ordenar: Por date DESC, hour DESC

**Tabla 3: Distribución de Estados de Pedidos**

- Agregación: Nivel diario, por estado
- Métricas a calcular:
  - `status_count`: Conteo de pedidos en cada estado
  - `status_percentage`: Porcentaje de pedidos totales
  - `total_revenue_by_status`: Suma de totales de pedidos
- Dimensiones: `date`, `status`
- Incluir: Todos los estados (pending, confirmed, processing, shipped, cancelled)

**Tabla 4: Métricas de Conversión**

- Agregación: Nivel diario
- Métricas a calcular:
  - `total_orders_created`: Conteo de todos los pedidos creados
  - `total_orders_shipped`: Conteo de pedidos con estado shipped
  - `total_orders_cancelled`: Conteo de pedidos cancelados
  - `conversion_rate`: (shipped / created) * 100
  - `cancellation_rate`: (cancelled / created) * 100
- Dimensión: `date`

**Tabla 5: Ingresos por Método de Pago**

- Agregación: Nivel diario, por método de pago
- Métricas a calcular:
  - `total_revenue`: Suma de totales
  - `order_count`: Conteo de pedidos
  - `avg_order_value`: Valor promedio de pedido
  - `payment_method_percentage`: Porcentaje de pedidos diarios totales
- Dimensiones: `date`, `payment_method`

**Tabla 6: Productos Top**

- Agregación: General o diaria
- Métricas a calcular:
  - `top_10_by_revenue`: Productos rankeados por ingreso total
  - `top_10_by_quantity`: Productos rankeados por cantidad vendida
- Incluir: `product_id`, `product_name`, `total_revenue`, `quantity_sold`, `rank`

**Output**:

- Todas las tablas guardadas en `/user/hadoop/gold/sales_performance/`
- Cada tabla particionada por `date` donde aplique
- Modo de actualización: Sobrescribir partición para actualizaciones incrementales

---

### Capa Gold - Caso de Uso 2: Analítica de Comportamiento de Clientes

**Objetivo**: Crear tablas agregadas para segmentación de clientes y análisis de comportamiento.

**Input**: Tabla Delta Lake de la capa Silver

**Requisitos**: Construir las siguientes tablas de métricas:

**Tabla 1: Clientes Top**

- Agregación: General o ventana rodante de 30 días
- Métricas a calcular:
  - `total_spent`: Suma de todos los totales de pedidos por cliente
  - `order_count`: Conteo de pedidos por cliente
  - `avg_order_value`: Valor promedio de pedido por cliente
  - `first_order_date`: Fecha del primer pedido
  - `last_order_date`: Fecha del pedido más reciente
  - `customer_tenure_days`: Días desde el primer pedido
- Filtro: Top 20 clientes por `total_spent`
- Incluir: `customer_id`, `customer_email`, métricas anteriores
- Ordenar: Por `total_spent` DESC

**Tabla 2: Segmentación de Clientes**

- Agregación: Nivel de cliente
- Lógica de segmentación:
  - `high_value`: total_spent > $500
  - `medium_value`: total_spent entre $100 y $500
  - `low_value`: total_spent < $100
- Métricas a calcular para cada segmento:
  - `customer_count`: Conteo de clientes en el segmento
  - `total_revenue`: Suma de ingresos del segmento
  - `avg_customer_value`: Promedio gastado por cliente
  - `segment_percentage`: Porcentaje de clientes totales
- Dimensiones: `segment`, `date` (snapshot diario opcional)

**Tabla 3: Preferencias de Método de Pago**

- Agregación: Por método de pago
- Métricas a calcular:
  - `payment_method_usage_count`: Conteo de pedidos por método
  - `payment_method_percentage`: Porcentaje de pedidos totales
  - `avg_order_value_by_method`: Valor promedio de pedido por método de pago
  - `total_revenue_by_method`: Ingreso total por método de pago
- Dimensiones: `payment_method`
- Ordenar: Por conteo de uso DESC

**Tabla 4: Análisis Geográfico**

- Agregación: Por estado (de dirección de envío)
- Métricas a calcular:
  - `order_count_by_state`: Conteo de pedidos por estado
  - `revenue_by_state`: Ingreso total por estado
  - `avg_order_value_by_state`: Valor promedio de pedido por estado
  - `unique_customers_by_state`: Conteo de clientes distintos
- Filtro: Top 10 estados por conteo de pedidos
- Dimensiones: `shipping_state`, `date` (opcional)
- Ordenar: Por conteo de pedidos DESC

**Tabla 5: Retención de Clientes**

- Agregación: Nivel de cliente
- Lógica de clasificación:
  - `repeat_customer`: cliente con order_count > 1
  - `one_time_customer`: cliente con order_count = 1
- Métricas a calcular:
  - `repeat_customer_count`: Conteo de clientes recurrentes
  - `one_time_customer_count`: Conteo de clientes de una vez
  - `repeat_customer_percentage`: (recurrentes / total) * 100
  - `repeat_customer_revenue`: Ingresos de clientes recurrentes
  - `repeat_customer_revenue_percentage`: % de ingresos totales de recurrentes
- Dimensiones: `date` (snapshot diario opcional)

**Tabla 6: Tiempo de Procesamiento de Pedido por Segmento**

- Agregación: Por segmento de cliente
- Métricas a calcular:
  - `avg_processing_time_minutes`: Tiempo promedio desde creación de pedido (pending) a shipped
  - `median_processing_time_minutes`: Mediana de tiempo de procesamiento
  - `min_processing_time_minutes`: Tiempo de procesamiento más rápido
  - `max_processing_time_minutes`: Tiempo de procesamiento más lento
- Dimensiones: `customer_segment` (alto/medio/bajo valor)
- Lógica: Calcular diferencia de tiempo entre primer estado (pending) y estado shipped para cada pedido
- Nota: Solo incluir pedidos enviados exitosamente

**Output**:

- Todas las tablas guardadas en `/user/hadoop/gold/customer_analytics/`
- Cada tabla particionada apropiadamente
- Modo de actualización: Sobrescribir o merge basado en tipo de tabla

---

## Especificaciones Técnicas

### Formato de Almacenamiento: Delta Lake

**¿Por qué Delta Lake?**

- Transacciones ACID aseguran consistencia de datos
- Time travel habilita auditoría y rollback
- Evolución de esquema maneja estructuras de datos cambiantes
- Desempeño optimizado para batch y streaming
- Versionamiento de datos y logging de auditoría integrados

**Configuración de Delta Lake**:

```
Formato de almacenamiento: Delta
Compresión: Snappy (por defecto)
Tamaño objetivo de archivo: 128MB - 256MB por archivo
Optimización: Ejecutar comando OPTIMIZE periódicamente
Vacuum: Retener 7 días de historia (configurable)
```

### Modo de Procesamiento: Batch

**Patrón de Ejecución**: Jobs batch programados

**Calendario Recomendado**:

- Ingesta Bronze: Cada 5-15 minutos (micro-batch)
- Transformación Silver: Cada 15-30 minutos
- Agregación Gold: Cada 1-4 horas o diariamente

**Lógica de Procesamiento**:

- Leer datos incrementales usando change data feed integrado de Delta Lake
- Procesar solo nuevas particiones (evitar escaneos completos de tabla)
- Usar procesamiento distribuido de Spark para ejecución paralela
- Implementar checkpointing para tolerancia a fallos

### Estrategia de Particionamiento

**Capa Bronze**:

- Particionar por: `ingestion_date` (particiones diarias)
- Justificación: Habilita consultas eficientes basadas en fechas y gestión fácil de ciclo de vida de datos

**Capa Silver**:

- Particionar por: `silver_ingestion_date` Y `status`
- Justificación: Patrones comunes de consulta filtran por fecha y estado; particionamiento multi-nivel mejora desempeño de consultas

**Capa Gold**:

- Particionar por: `date` (varía por tabla)
- Justificación: La mayoría de consultas analíticas son basadas en rango de fechas; particiones diarias habilitan actualizaciones incrementales

**Mejores Prácticas de Particionamiento**:

- Evitar sobre-particionamiento (apuntar a 1GB+ por partición)
- Alinear claves de partición con patrones de consulta
- Usar poda de particiones en consultas

### Políticas de Retención de Datos

**Capa Bronze**: Retener 90 días

- Justificación: Trail de auditoría completo para cumplimiento y replay de datos

**Capa Silver**: Retener 365 días

- Justificación: Analítica de largo plazo y análisis de tendencias

**Capa Gold**: Retener indefinidamente (archivar después de 2 años)

- Justificación: Métricas de negocio históricas, costo mínimo de almacenamiento para datos agregados

### Volúmenes de Datos Esperados

Basado en la escala actual de FastCart:

**Volúmenes Diarios**:

- Pedidos creados: ~50,000 por día
- Eventos totales (con actualizaciones de estado): ~200,000 por día (4 estados por pedido en promedio)
- Tamaño de datos estimado (Bronze): ~200 MB por día (JSON sin comprimir)
- Tamaño de datos estimado (Silver): ~150 MB por día (Parquet con compresión)
- Tamaño de datos estimado (Gold): ~5 MB por día (agregado)

**Volúmenes Mensuales**:

- Bronze: ~6 GB
- Silver: ~4.5 GB
- Gold: ~150 MB

**Recursos del Cluster**:

- Mínimo: 3 nodos, 8GB RAM por nodo, 4 cores
- Recomendado: 5 nodos, 16GB RAM por nodo, 8 cores

---

## Requisitos del Sistema

### Requisitos de Sistema

- **Python**: 3.7 o superior
- **Apache Spark**: 3.x con soporte Hadoop
- **Apache Kafka**: 2.x o superior
- **Hadoop**: 3.x
- **Delta Lake**: Versión compatible con su versión de Spark

### Dependencias Python

Instalar los paquetes Python requeridos:

```bash
pip install -r requirements.txt
```

Paquetes requeridos:

- `faker`: Para generar datos de prueba realistas
- `kafka-python`: Cliente Python para Apache Kafka
- `pyspark`: API Python de Apache Spark
- `delta-spark`: Paquete Python de Delta Lake

### Configuración de Hadoop y Spark

Asegúrese que su ambiente tenga:

- Cluster Hadoop configurado con HDFS
- Cluster Spark o modo standalone
- Librerías Delta Lake incluidas en classpath de Spark
- Permisos apropiados de HDFS para su usuario

---

## Instalación y Configuración

### Instalación de Dependencias Python

Instalar dependencias:

```bash
pip install -r requirements.txt
```

O instalar individualmente:

```bash
pip install faker kafka-python pyspark delta-spark
```

### Instalación de Kafka (Configuración Nativa - Sin Docker)

Instalar e iniciar Kafka nativamente:

```bash
# Instalar Kafka (descarga Apache Kafka 3.6.1)
python kafka_native_manager.py install

# Iniciar Kafka y Zookeeper
python kafka_native_manager.py start

# Verificar estado
python kafka_native_manager.py status

# Crear el tópico para este proyecto
python kafka_native_manager.py create-topic ecommerce-sales --partitions 3

# Listar tópicos
python kafka_native_manager.py list-topics

# Consumir del tópico (para verificar flujo de datos)
python kafka_native_manager.py consume ecommerce-sales

# Detener Kafka cuando termine
python kafka_native_manager.py stop
```

El script descarga Kafka a `~/.kafka-local/` y ejecuta:

- Zookeeper en puerto 2181
- Kafka en puerto 9092

---

## Uso del Generador de Ventas

### Uso Básico

```bash
# Salida a consola (por defecto)
python sales_generator.py

# Enviar a Kafka
python sales_generator.py --kafka-bootstrap-servers localhost:9092 --kafka-topic ecommerce-sales

# Kafka + salida a consola
python sales_generator.py --kafka-bootstrap-servers localhost:9092 --kafka-topic ecommerce-sales --output-console

# Temporización personalizada
python sales_generator.py --duration 10 --min-delay 2 --max-delay 30
```

### Opciones de Comando

- `--duration MINUTOS`: Cuánto tiempo generar datos (por defecto: 5)
- `--min-delay SEGUNDOS`: Retraso mínimo entre pedidos (por defecto: 1)
- `--max-delay SEGUNDOS`: Retraso máximo entre pedidos (por defecto: 60)
- `--kafka-bootstrap-servers HOST:PUERTO`: Broker(s) de Kafka
- `--kafka-topic TOPIC`: Nombre del tópico de Kafka
- `--output-console`: Imprimir a consola cuando se usa Kafka
- `--no-console`: Deshabilitar salida a consola

---

## Inicio Rápido

Flujo de trabajo completo desde instalación hasta configuración del pipeline:

```bash
# 1. Instalar dependencias Python
pip install -r requirements.txt

# 2. Instalar Kafka
python kafka_native_manager.py install

# 3. Iniciar Kafka
python kafka_native_manager.py start

# 4. Crear tópico
python kafka_native_manager.py create-topic ecommerce-sales --partitions 3

# 5. Iniciar generación de datos de ventas (ejecutar en background o terminal separada)
python sales_generator.py --kafka-bootstrap-servers localhost:9092 --kafka-topic ecommerce-sales --duration 30

# 6. Verificar que los datos están fluyendo (en otra terminal)
python kafka_native_manager.py consume ecommerce-sales

# 7. ¡Ahora implemente su pipeline de Spark!
# - Crear job de ingesta de capa Bronze
# - Crear job de transformación de capa Silver
# - Crear jobs de agregación de capa Gold

# 8. Detener Kafka cuando termine
python kafka_native_manager.py stop
```

### Ejemplo de Salida

Cada pedido genera múltiples eventos JSON a medida que progresa a través de estados:

```json
{
  "order_id": "ORD-93239605",
  "timestamp": "2025-11-09T15:55:40.669190Z",
  "product_id": "PROD-004",
  "product_name": "USB-C Cable",
  "quantity": 3,
  "price": 11.16,
  "total": 33.48,
  "customer_id": "CUST-485923",
  "customer_email": "campbellmichael@example.org",
  "payment_method": "bank_transfer",
  "shipping_address": {
    "street": "123 Main St",
    "city": "Springfield",
    "state": "IL",
    "zip_code": "62701",
    "country": "USA"
  },
  "status": "pending"
}
```

El mismo pedido generará eventos subsecuentes con actualizaciones de estado: `confirmed`, `processing` y `shipped`. Algunos pedidos pueden transicionar a `cancelled` con un campo `cancellation_reason`.

---

## Criterios de Evaluación

Su implementación del pipeline será evaluada en los siguientes criterios:

### 1. Implementación Correcta de las Tres Capas (40%)

- Capa Bronze ingiere correctamente todos los mensajes de Kafka sin pérdida de datos
- Capa Silver aplica todas las transformaciones requeridas con precisión
- Capa Gold produce métricas correctas que coinciden con las especificaciones
- Todas las tablas Delta Lake están apropiadamente creadas y son consultables

### 2. Uso Apropiado de Características de Delta Lake (20%)

- Transacciones ACID usadas apropiadamente
- Evolución de esquema configurada correctamente
- Estrategia de particionamiento implementada según lo especificado
- Propiedades de tabla y optimizaciones aplicadas

### 3. Precisión de Métricas Calculadas (20%)

- Todas las métricas en capa Gold coinciden con las fórmulas especificadas
- Agregaciones producen resultados correctos
- Casos edge manejados (nulos, duplicados, cancelaciones)
- Consultas de validación prueban precisión de métricas

### 4. Organización de Código y Documentación (10%)

- Código limpio y legible con estructura apropiada
- Nombres de variables y funciones significativos
- Comentarios explicando lógica de negocio
- README o documentación explicando cómo ejecutar el pipeline

### 5. Consideraciones de Desempeño (10%)

- Uso eficiente de transformaciones de Spark
- Particionamiento y filtrado apropiado para evitar escaneos completos de tabla
- Caching usado apropiadamente para DataFrames reutilizados
- Sin shuffles innecesarios u operaciones redundantes

### Puntos Bonus:

- Manejo de errores y logging
- Chequeos de calidad de datos y alertas
- Monitoreo del pipeline y métricas
- Implementación de procesamiento incremental
- Pruebas unitarias para lógica de negocio

---

## Entregables

Entregar los siguientes componentes:

### 1. Aplicaciones Spark (Requerido)

Entregar su código de aplicación Spark:

- **Opción A**: Tres scripts separados (bronze.py, silver.py, gold.py)
- **Opción B**: Un script de pipeline unificado con funciones modulares
- El código debe ser ejecutable y bien documentado
- Incluir cualquier archivo de configuración necesario

### 2. Esquemas de Tablas Delta Lake (Requerido)

Documentar sus estructuras de tabla:

- Capa Bronze: Esquema final con datos de muestra
- Capa Silver: Esquema transformado con datos de muestra
- Capa Gold: Esquema para cada tabla de métricas
- Incluir sentencias DDL o descripciones de esquemas

### 3. Consultas de Muestra (Requerido)

Proveer consultas SQL o DataFrame mostrando:

- Datos en capa Bronze (primeras 10 filas)
- Datos en capa Silver (primeras 10 filas)
- Resultados de cada tabla de métricas de capa Gold
- Las consultas deben ser ejecutables contra sus tablas Delta Lake

### 4. Consultas de Validación (Requerido)

Probar que sus métricas son correctas:

- Verificación de conteo: Conteo Bronze = Conteo Silver (después de deduplicación)
- Validación de ingresos: Suma de totales coincide a través de capas
- Verificaciones puntuales de métricas: Mostrar cálculo para 2-3 métricas específicas
- Reporte de calidad de datos: Conteo de registros rechazados y razones

### 5. Documentación de Arquitectura (Requerido)

Documentar sus decisiones de diseño:

- Diagrama de arquitectura del pipeline
- Explicación de flujo de datos (cómo se mueven los datos a través de capas)
- Justificación de estrategia de particionamiento
- Recomendación de calendario de procesamiento
- Cualquier suposición o trade-off realizado

### 6. Instrucciones de Ejecución (Requerido)

Proveer pasos claros para ejecutar su pipeline:

- Prerequisitos y pasos de configuración
- Cómo ejecutar cada capa
- Tiempo de ejecución y requisitos de recursos esperados
- Cómo consultar los resultados
- Solución de problemas comunes

### 7. Opcional: Características Avanzadas

Mostrar capacidades adicionales:

- Pruebas unitarias para lógica de transformación
- Dashboard de monitoreo de calidad de datos
- Orquestación automatizada del pipeline (DAG de Airflow, scripts cron)
- Resultados de benchmarking de desempeño
- Configuración de pipeline CI/CD

---

## Consejos para el Éxito

1. **Comience Simple**: Haga funcionar primero la capa Bronze, luego itere a Silver y Gold
2. **Pruebe Incrementalmente**: Valide cada capa antes de pasar a la siguiente
3. **Use Datos Pequeños**: Pruebe con 5-10 minutos de datos generados antes de ejecuciones completas
4. **Monitoree Recursos**: Observe su UI de Spark y uso de HDFS
5. **Control de Versiones**: Haga commit de su código frecuentemente
6. **Haga Preguntas**: Si los requisitos no son claros, documente sus suposiciones
7. **Optimice Después**: Obtenga resultados correctos primero, luego optimice desempeño

---

## Soporte

Si encuentra problemas:

1. Revise el [TECHNICAL.md](TECHNICAL.md) para especificaciones detalladas
2. Verifique que Kafka esté corriendo: `python kafka_native_manager.py status`
3. Revise logs de Kafka: `python kafka_native_manager.py logs`
4. Verifique que los datos estén fluyendo: `python kafka_native_manager.py consume ecommerce-sales`
5. Revise logs de Spark para mensajes de error
6. Asegúrese que los permisos de HDFS sean correctos

---

## Documentación Adicional

Para información técnica detallada sobre el generador de datos, vea [README.md](README.md) y [TECHNICAL.md](TECHNICAL.md):

- Comportamiento y temporización del ciclo de vida del pedido
- Estructura de datos completa y esquema
- Catálogo de productos (15 productos)
- Detalles de configuración de Kafka
- Ejemplos de integración
- Estadísticas y monitoreo
- Guía de solución de problemas

---

## Licencia

Este proyecto es para propósitos educativos.

