import argparse
from pathlib import Path
from typing import Any

import yaml

SPACE_ID = "01f00000000000000000000000000000"
CONVERSATION_ID = "01f10000000000000000000000000000"
CONVERSATION_ID_2 = "01f10000000000000000000000000001"
BENCHMARK_RUN_ID = "01f20000000000000000000000000000"
BENCHMARK_RESULT_ID = "01f30000000000000000000000000000"
STATEMENT_ID = "11111111-2222-4333-8444-555555555555"
SERIALIZATION_RUN_ID = 123456789012341
RESTORE_POINTS_RUN_ID = 123456789012342
RESTORE_RUN_ID = 123456789012343
USER_ID = "1234567890123456"
USER_NAME = "eval-reader@example.invalid"
TABLE_ID = "eval_catalog.eval_schema.eval_orders"
SNAPSHOT_DATE = "2026-01-15"
DEFAULT_MODEL = "databricks-gpt-5-4-mini"
CONFIRMATION_GATED_TOOLS = {
    "start_genie_serialization_job",
    "start_genie_space_restore_job",
}

EVALUATION_TOOL_GROUPS = {
    "atlan": (
        "find_atlan_assets_by_databricks_table",
        "get_atlan_context_for_databricks_table",
    ),
    "conversations_and_messages": (
        "list_genie_space_conversations",
        "list_genie_conversation_messages",
        "list_genie_messages_for_conversations",
    ),
    "usage_metrics": (
        "get_genie_usage_metrics",
        "start_genie_usage_metrics_query",
        "get_genie_usage_metrics_query_result",
    ),
    "benchmarks": (
        "list_genie_benchmark_runs",
        "get_genie_benchmark_run",
        "list_genie_benchmark_run_results",
        "get_genie_benchmark_result_details",
    ),
    "snapshots": (
        "start_genie_serialization_job",
        "get_genie_serialization_job_run",
        "list_genie_space_restore_points",
        "get_genie_restore_points_job_run",
        "start_genie_space_restore_job",
        "get_genie_space_restore_job_run",
    ),
}


ALL_TOOL_PROFILES: dict[str, dict[str, Any]] = {
    "health": {
        "purpose": "comprobar que el servidor MCP local responde",
        "closest": "get_current_user",
        "arguments": {},
        "positive": "Comprueba si el servidor MCP local esta operativo.",
        "alternative": "Haz un health check del transporte MCP, sin consultar identidades.",
        "indirect": [
            "Antes de empezar necesito saber si el canal local de herramientas esta vivo.",
            "Verifica que el servicio de herramientas responde y despues dime su estado.",
        ],
        "missing": [
            "Comprueba si el espacio Genie que tengo en mente esta sano.",
            "Valida la salud de ese recurso de Databricks, pero no te digo cual es.",
        ],
    },
    "get_current_user": {
        "purpose": "obtener la identidad Databricks autenticada actual",
        "closest": "get_user_name_from_id",
        "arguments": {},
        "positive": "Dime que usuario de Databricks esta autenticado en esta sesion.",
        "alternative": "Necesito el nombre y estado de la cuenta con la que estoy conectado ahora.",
        "indirect": [
            "Antes de continuar, identifica al propietario de estas credenciales.",
            "Personaliza la respuesta indicando primero quien soy en el workspace.",
        ],
        "missing": [
            "Dime quien es el usuario del que hablamos antes.",
            "Identifica a esa persona en Databricks sin que te proporcione su ID.",
        ],
    },
    "list_available_genie_spaces": {
        "purpose": "listar los Genie Spaces visibles para el usuario",
        "closest": "find_genie_spaces_by_tag",
        "arguments": {"limit": 5},
        "positive": "Lista como maximo 5 Genie Spaces disponibles para mi usuario.",
        "alternative": "Ensenname los primeros 5 espacios Genie a los que tengo acceso, sin filtrar por tags.",
        "indirect": [
            "Quiero elegir un espacio Genie; dame un menu inicial de 5 opciones visibles.",
            "Necesito descubrir que espacios puedo usar. Limita la respuesta a 5.",
        ],
        "missing": [
            "Abre el espacio Genie de Finanzas.",
            "Entra en el espacio que usamos ayer, pero no recuerdo su ID ni su nombre exacto.",
        ],
    },
    "get_genie_space_details": {
        "purpose": "obtener los metadatos de un Genie Space concreto",
        "closest": "list_genie_space_tags",
        "arguments": {"space_id": SPACE_ID},
        "positive": f"Muestra los detalles del Genie Space {SPACE_ID}.",
        "alternative": f"Recupera los metadatos generales del espacio {SPACE_ID}, no solamente sus tags.",
        "indirect": [
            f"Necesito entender que es el espacio {SPACE_ID}; traeme su ficha.",
            f"Antes de trabajar con {SPACE_ID}, dame su informacion descriptiva.",
        ],
        "missing": [
            "Muestra los detalles del espacio de Ventas.",
            "Dame la ficha completa de ese Genie Space, aunque no te he dado su ID.",
        ],
    },
    "list_genie_space_tags": {
        "purpose": "listar los tags asignados a un Genie Space concreto",
        "closest": "find_genie_spaces_by_tag",
        "arguments": {"space_id": SPACE_ID},
        "positive": f"Lista los tags asignados al Genie Space {SPACE_ID}.",
        "alternative": f"Para el espacio {SPACE_ID}, devuelve sus etiquetas; no busques otros espacios.",
        "indirect": [
            f"Quiero saber como esta clasificado {SPACE_ID} mediante etiquetas del workspace.",
            f"Comprueba las marcas organizativas que tiene el espacio {SPACE_ID}.",
        ],
        "missing": [
            "Que tags tiene el espacio de Finanzas?",
            "Lista las etiquetas de ese Genie Space sin que te facilite su ID.",
        ],
    },
    "find_genie_spaces_by_tag": {
        "purpose": "buscar Genie Spaces por una clave y valor de tag",
        "closest": "list_genie_space_tags",
        "arguments": {"tag_key": "environment", "tag_value": "sandbox", "limit": 5},
        "positive": "Encuentra hasta 5 Genie Spaces con el tag environment=sandbox.",
        "alternative": "Busca espacios etiquetados exactamente con clave environment y valor sandbox; devuelve 5.",
        "indirect": [
            "Necesito un espacio de pruebas: localiza 5 candidatos cuya etiqueta environment sea sandbox.",
            "Filtra el inventario Genie usando environment=sandbox y limita a 5 resultados.",
        ],
        "missing": [
            "Busca los espacios etiquetados como produccion.",
            "Encuentra Genie Spaces por tag, pero no recuerdo ni la clave ni el valor.",
        ],
    },
    "find_atlan_assets_by_databricks_table": {
        "purpose": "localizar assets de Atlan que correspondan a una tabla Databricks",
        "closest": "get_atlan_context_for_databricks_table",
        "arguments": {"table_identifier": TABLE_ID, "limit": 5},
        "positive": f"Busca hasta 5 assets de Atlan que coincidan con la tabla {TABLE_ID}.",
        "alternative": f"Identifica los assets y GUIDs asociados a {TABLE_ID}; no necesito su contexto de negocio.",
        "indirect": [
            f"Quiero comprobar como se resuelve {TABLE_ID} dentro del catalogo de Atlan.",
            f"Localiza la representacion de {TABLE_ID} en Atlan y limita a 5 coincidencias.",
        ],
        "missing": [
            "Busca en Atlan la tabla de pedidos.",
            "Localiza el asset de eval_orders, pero no conozco catalogo ni schema.",
        ],
    },
    "get_atlan_context_for_databricks_table": {
        "purpose": "obtener contexto de negocio y glosario de Atlan para una tabla",
        "closest": "find_atlan_assets_by_databricks_table",
        "arguments": {"table_identifier": TABLE_ID, "limit": 1},
        "positive": f"Obtiene el contexto de negocio de Atlan para {TABLE_ID}, inspeccionando 1 asset.",
        "alternative": f"Trae glosario, descripcion y README de Atlan para {TABLE_ID}; limita a 1 coincidencia.",
        "indirect": [
            f"Necesito entender el significado empresarial de {TABLE_ID} usando Atlan.",
            f"Dame material de grounding de Atlan para explicar {TABLE_ID}; usa como maximo 1 asset.",
        ],
        "missing": [
            "Dame el contexto de negocio de la tabla de pedidos.",
            "Consulta Atlan para eval_orders sin saber en que catalogo y schema esta.",
        ],
    },
    "get_user_name_from_id": {
        "purpose": "resolver un ID de usuario Databricks a su nombre",
        "closest": "get_current_user",
        "arguments": {"user_id": USER_ID},
        "positive": f"Resuelve el ID de usuario Databricks {USER_ID} a su nombre.",
        "alternative": f"Averigua que username corresponde al identificador {USER_ID}; no busco mi identidad actual.",
        "indirect": [
            f"En un registro aparece el actor {USER_ID}; traducelo a un nombre de usuario.",
            f"Necesito hacer legible este ID de usuario: {USER_ID}.",
        ],
        "missing": [
            "Dime el nombre del usuario de ese evento.",
            "Resuelve un usuario de Databricks, pero no dispongo de su ID.",
        ],
    },
    "list_genie_space_conversations": {
        "purpose": "listar conversaciones pertenecientes a un Genie Space",
        "closest": "list_genie_conversation_messages",
        "arguments": {"space_id": SPACE_ID, "include_all": False, "limit": 5},
        "positive": f"Lista 5 conversaciones recientes del Genie Space {SPACE_ID}.",
        "alternative": f"Dame el indice de conversaciones de {SPACE_ID}, include_all=false y limite 5; no leas mensajes.",
        "indirect": [
            f"Quiero escoger un hilo dentro de {SPACE_ID}; ensename 5 conversaciones recientes.",
            f"Descubre los IDs de hasta 5 conversaciones recientes del espacio {SPACE_ID}.",
        ],
        "missing": [
            "Lista las conversaciones recientes del espacio de Ventas.",
            "Muestra los hilos de ese Genie Space sin que te de su ID.",
        ],
    },
    "list_genie_conversation_messages": {
        "purpose": "listar mensajes de una unica conversacion Genie",
        "closest": "list_genie_messages_for_conversations",
        "arguments": {"space_id": SPACE_ID, "conversation_id": CONVERSATION_ID, "limit": 10},
        "positive": f"Lista hasta 10 mensajes de la conversacion {CONVERSATION_ID} del espacio {SPACE_ID}.",
        "alternative": f"Lee un solo hilo: {CONVERSATION_ID} en {SPACE_ID}, con limite 10.",
        "indirect": [
            f"Reconstruye el dialogo del hilo {CONVERSATION_ID} dentro de {SPACE_ID}; trae 10 mensajes.",
            f"Necesito revisar que se dijo en {CONVERSATION_ID} del espacio {SPACE_ID}, maximo 10 entradas.",
        ],
        "missing": [
            f"Muestra los mensajes de una conversacion del espacio {SPACE_ID}, pero no se cual.",
            f"Lee la conversacion {CONVERSATION_ID}, aunque no conozco el space_id que la contiene.",
        ],
    },
    "list_genie_messages_for_conversations": {
        "purpose": "listar mensajes de varias conversaciones Genie",
        "closest": "list_genie_conversation_messages",
        "arguments": {
            "space_id": SPACE_ID,
            "conversation_ids": [CONVERSATION_ID, CONVERSATION_ID_2],
            "limit_per_conversation": 10,
        },
        "positive": f"Lista hasta 10 mensajes por conversacion para {CONVERSATION_ID} y {CONVERSATION_ID_2} en {SPACE_ID}.",
        "alternative": f"Recupera en lote los mensajes de los hilos {CONVERSATION_ID} y {CONVERSATION_ID_2} del espacio {SPACE_ID}.",
        "indirect": [
            f"Compara lo hablado en {CONVERSATION_ID} y {CONVERSATION_ID_2}, ambos de {SPACE_ID}, usando 10 mensajes por hilo.",
            f"Necesito dos historiales a la vez en {SPACE_ID}: {CONVERSATION_ID} y {CONVERSATION_ID_2}.",
        ],
        "missing": [
            f"Trae mensajes en lote del espacio {SPACE_ID}, pero no tengo los IDs de las conversaciones.",
            f"Compara {CONVERSATION_ID} y {CONVERSATION_ID_2}, aunque no se a que espacio pertenecen.",
        ],
    },
    "get_genie_usage_metrics": {
        "purpose": "ejecutar y esperar las metricas de uso de un Genie Space",
        "closest": "start_genie_usage_metrics_query",
        "arguments": {
            "space_id": SPACE_ID,
            "lookback_days": 7,
            "timeout_seconds": 30,
            "poll_interval_seconds": 2,
        },
        "positive": f"Obtiene las metricas de uso de {SPACE_ID} para 7 dias, esperando hasta 30 segundos y consultando cada 2.",
        "alternative": f"Quiero el resultado sincronico de uso de {SPACE_ID}: lookback 7, timeout 30, poll 2.",
        "indirect": [
            f"Calcula ahora la actividad semanal de {SPACE_ID} y espera como maximo medio minuto.",
            f"Necesito usuarios e interacciones de los ultimos 7 dias de {SPACE_ID} en una sola operacion bloqueante.",
        ],
        "missing": [
            "Dame las metricas de uso del espacio de Finanzas durante una semana.",
            "Calcula el uso reciente de ese Genie Space sin que te facilite su ID.",
        ],
    },
    "start_genie_usage_metrics_query": {
        "purpose": "iniciar de forma asincrona una consulta de metricas Genie",
        "closest": "get_genie_usage_metrics",
        "arguments": {"space_id": SPACE_ID, "lookback_days": 7},
        "positive": f"Inicia la consulta asincrona de metricas de {SPACE_ID} para los ultimos 7 dias y devuelve el statement ID.",
        "alternative": f"Lanza sin esperar el calculo de uso semanal de {SPACE_ID}; no uses la version sincronica.",
        "indirect": [
            f"Prepara el calculo de actividad de {SPACE_ID} para 7 dias y dame un identificador que pueda consultar despues.",
            f"No bloquees: arranca la query semanal de uso de {SPACE_ID} y vuelve rapido.",
        ],
        "missing": [
            "Inicia las metricas de uso del espacio de Finanzas para 7 dias.",
            "Arranca una consulta de uso, pero no se para que Genie Space.",
        ],
    },
    "get_genie_usage_metrics_query_result": {
        "purpose": "consultar el resultado de un statement de metricas ya iniciado",
        "closest": "start_genie_usage_metrics_query",
        "arguments": {"statement_id": STATEMENT_ID},
        "positive": f"Consulta el estado y resultado del statement de metricas {STATEMENT_ID}.",
        "alternative": f"Ya existe la query {STATEMENT_ID}; recupera su resultado sin iniciar otra.",
        "indirect": [
            f"Comprueba si termino el calculo cuyo ticket SQL es {STATEMENT_ID}.",
            f"Continua el flujo asincrono usando el statement ID {STATEMENT_ID}.",
        ],
        "missing": [
            "Comprueba si ya termino la consulta de metricas.",
            "Recupera el resultado de la query anterior, pero no tengo su statement ID.",
        ],
    },
    "list_genie_benchmark_runs": {
        "purpose": "listar ejecuciones de benchmark de un Genie Space",
        "closest": "get_genie_benchmark_run",
        "arguments": {"space_id": SPACE_ID, "limit": 5},
        "positive": f"Lista hasta 5 ejecuciones de benchmark del Genie Space {SPACE_ID}.",
        "alternative": f"Dame el historial de 5 eval runs de {SPACE_ID}; no consultes un run concreto.",
        "indirect": [
            f"Quiero elegir una evaluacion de {SPACE_ID}; ensename 5 runs disponibles.",
            f"Descubre los IDs de las ultimas 5 ejecuciones de benchmark en {SPACE_ID}.",
        ],
        "missing": [
            "Lista los benchmarks del espacio de Ventas.",
            "Muestra las ejecuciones de evaluacion de ese espacio sin su ID.",
        ],
    },
    "get_genie_benchmark_run": {
        "purpose": "obtener el estado y detalle de una ejecucion de benchmark",
        "closest": "list_genie_benchmark_runs",
        "arguments": {"space_id": SPACE_ID, "run_id": BENCHMARK_RUN_ID},
        "positive": f"Obtiene el detalle del benchmark run {BENCHMARK_RUN_ID} del espacio {SPACE_ID}.",
        "alternative": f"Consulta el estado de la evaluacion {BENCHMARK_RUN_ID} en {SPACE_ID}; no listes todos los runs.",
        "indirect": [
            f"Necesito saber como termino la evaluacion {BENCHMARK_RUN_ID} de {SPACE_ID}.",
            f"Abre la ficha del eval run {BENCHMARK_RUN_ID} perteneciente a {SPACE_ID}.",
        ],
        "missing": [
            f"Consulta el benchmark actual del espacio {SPACE_ID}, pero no tengo el run ID.",
            f"Dame el detalle del run {BENCHMARK_RUN_ID} sin conocer el space_id.",
        ],
    },
    "list_genie_benchmark_run_results": {
        "purpose": "listar filas de resultado de una ejecucion de benchmark",
        "closest": "get_genie_benchmark_result_details",
        "arguments": {"space_id": SPACE_ID, "run_id": BENCHMARK_RUN_ID, "limit": 5},
        "positive": f"Lista hasta 5 resultados del benchmark {BENCHMARK_RUN_ID} en {SPACE_ID}.",
        "alternative": f"Dame las primeras 5 filas de resultados de {BENCHMARK_RUN_ID} en {SPACE_ID}; no abras un resultado concreto.",
        "indirect": [
            f"Quiero escoger un caso fallido del run {BENCHMARK_RUN_ID} de {SPACE_ID}; ensename 5 resultados.",
            f"Descubre los IDs de 5 resultados de la evaluacion {BENCHMARK_RUN_ID} en {SPACE_ID}.",
        ],
        "missing": [
            f"Lista resultados de benchmark para {SPACE_ID}, pero no se que run usar.",
            f"Muestra las filas del run {BENCHMARK_RUN_ID} sin su space_id.",
        ],
    },
    "get_genie_benchmark_result_details": {
        "purpose": "obtener el detalle de un resultado individual de benchmark",
        "closest": "list_genie_benchmark_run_results",
        "arguments": {
            "space_id": SPACE_ID,
            "run_id": BENCHMARK_RUN_ID,
            "result_id": BENCHMARK_RESULT_ID,
        },
        "positive": f"Obtiene el detalle del resultado {BENCHMARK_RESULT_ID} del run {BENCHMARK_RUN_ID} en {SPACE_ID}.",
        "alternative": f"Abre exactamente el caso {BENCHMARK_RESULT_ID} de la evaluacion {BENCHMARK_RUN_ID} y espacio {SPACE_ID}.",
        "indirect": [
            f"Investiga por que fallo el resultado {BENCHMARK_RESULT_ID} del run {BENCHMARK_RUN_ID} en {SPACE_ID}.",
            f"Necesito la ficha profunda del resultado {BENCHMARK_RESULT_ID}, asociado a {BENCHMARK_RUN_ID} y {SPACE_ID}.",
        ],
        "missing": [
            f"Abre un resultado del run {BENCHMARK_RUN_ID} en {SPACE_ID}, pero no se su result ID.",
            f"Consulta {BENCHMARK_RESULT_ID}, aunque no tengo el run ID ni el space ID.",
        ],
    },
    "list_genie_space_permissions": {
        "purpose": "listar permisos y principals de un Genie Space",
        "closest": "grant_space_permissions",
        "arguments": {"space_id": SPACE_ID},
        "positive": f"Lista los permisos actuales del Genie Space {SPACE_ID}.",
        "alternative": f"Audita quienes tienen acceso a {SPACE_ID}; no concedas ningun permiso.",
        "indirect": [
            f"Quiero revisar la ACL de {SPACE_ID} antes de hacer cambios.",
            f"Dime que usuarios, grupos o service principals pueden usar {SPACE_ID}.",
        ],
        "missing": [
            "Lista los permisos del espacio de Finanzas.",
            "Revisa la ACL de ese Genie Space sin que te facilite su ID.",
        ],
    },
    "grant_space_permissions": {
        "purpose": "preparar o efectuar una concesion de permisos en un Genie Space",
        "closest": "list_genie_space_permissions",
        "arguments": {
            "space_id": SPACE_ID,
            "user_name_list": [USER_NAME],
            "permission_level": "CAN_READ",
        },
        "positive": f"Prepara la concesion CAN_READ sobre {SPACE_ID} para {USER_NAME}; no inventes la confirmacion.",
        "alternative": f"Solicita acceso de lectura para {USER_NAME} en {SPACE_ID} y devuelve el requisito de confirmacion.",
        "indirect": [
            f"Quiero que {USER_NAME} pueda consultar {SPACE_ID}; llega solo hasta pedir aprobacion.",
            f"Genera el paso previo para compartir {SPACE_ID} en modo lectura con {USER_NAME}.",
        ],
        "missing": [
            f"Da acceso a {USER_NAME} en {SPACE_ID}, pero no indico el nivel.",
            f"Concede CAN_READ en {SPACE_ID}, aunque no digo a que usuario.",
        ],
    },
    "start_genie_serialization_job": {
        "purpose": "preparar o iniciar la serializacion de un Genie Space",
        "closest": "get_genie_serialization_job_run",
        "arguments": {"space_id": SPACE_ID},
        "positive": f"Prepara la serializacion del Genie Space {SPACE_ID}; no inventes la confirmacion.",
        "alternative": f"Solicita iniciar el job de serializacion solo para {SPACE_ID} y devuelve el requisito de aprobacion.",
        "indirect": [
            f"Necesito crear un snapshot serializado de {SPACE_ID}; llega hasta el paso de confirmacion.",
            f"Deja preparada la ejecucion que exportaria el espacio {SPACE_ID}, sin aprobarla por mi.",
        ],
        "missing": [
            "Serializa los espacios de produccion, pero no indico un space ID ni una tag key exacta.",
            "Lanza la serializacion con el alcance por defecto, aunque no he definido el alcance.",
        ],
    },
    "get_genie_serialization_job_run": {
        "purpose": "consultar el estado de un job run de serializacion",
        "closest": "get_genie_restore_points_job_run",
        "arguments": {"run_id": SERIALIZATION_RUN_ID},
        "positive": f"Consulta el estado del job run de serializacion {SERIALIZATION_RUN_ID}.",
        "alternative": f"El run {SERIALIZATION_RUN_ID} procede de serializacion; dame su estado, no el de restauracion.",
        "indirect": [
            f"Comprueba si termino la exportacion cuyo run ID es {SERIALIZATION_RUN_ID}.",
            f"Sigue la ejecucion de serializacion identificada por {SERIALIZATION_RUN_ID}.",
        ],
        "missing": [
            "Comprueba el ultimo job de serializacion.",
            "Dime si termino la serializacion anterior, pero no conservo su run ID.",
        ],
    },
    "list_genie_space_restore_points": {
        "purpose": "obtener restore points de un Genie Space mediante el job configurado",
        "closest": "get_genie_restore_points_job_run",
        "arguments": {"space_id": SPACE_ID, "timeout_minutes": 1, "poll_interval_seconds": 5},
        "positive": f"Solicita los restore points de {SPACE_ID}, con timeout 1 minuto y polling cada 5 segundos.",
        "alternative": f"Inicia la busqueda de snapshots disponibles para {SPACE_ID}; espera como maximo 1 minuto.",
        "indirect": [
            f"Necesito conocer a que fechas podria volver {SPACE_ID}; usa polling de 5 segundos y timeout 1 minuto.",
            f"Descubre los puntos de recuperacion de {SPACE_ID} mediante el job correspondiente.",
        ],
        "missing": [
            "Lista los restore points del espacio de Finanzas.",
            "Busca snapshots recuperables, pero no especifico el Genie Space.",
        ],
    },
    "get_genie_restore_points_job_run": {
        "purpose": "consultar un job run que lista restore points",
        "closest": "get_genie_space_restore_job_run",
        "arguments": {"run_id": RESTORE_POINTS_RUN_ID},
        "positive": f"Consulta el run {RESTORE_POINTS_RUN_ID} que estaba listando restore points.",
        "alternative": f"Recupera estado y salida del job de puntos de restauracion {RESTORE_POINTS_RUN_ID}; no es un restore.",
        "indirect": [
            f"Comprueba si ya tenemos las fechas recuperables del ticket de job {RESTORE_POINTS_RUN_ID}.",
            f"Continua la consulta asincrona de restore points usando el run ID {RESTORE_POINTS_RUN_ID}.",
        ],
        "missing": [
            "Comprueba si termino el job que listaba restore points.",
            "Recupera la salida de puntos de restauracion anterior sin su run ID.",
        ],
    },
    "start_genie_space_restore_job": {
        "purpose": "preparar o iniciar la restauracion de un Genie Space a una fecha",
        "closest": "list_genie_space_restore_points",
        "arguments": {"space_id": SPACE_ID, "snapshot_date": SNAPSHOT_DATE},
        "positive": f"Prepara restaurar {SPACE_ID} al snapshot {SNAPSHOT_DATE}; no inventes la confirmacion.",
        "alternative": f"Solicita el restore de {SPACE_ID} a {SNAPSHOT_DATE} y devuelve la confirmacion exacta requerida.",
        "indirect": [
            f"Quiero volver {SPACE_ID} al estado del {SNAPSHOT_DATE}; llega solo hasta pedir aprobacion.",
            f"Deja preparada la recuperacion de {SPACE_ID} usando la fecha exacta {SNAPSHOT_DATE}.",
        ],
        "missing": [
            f"Restaura {SPACE_ID} a la ultima fecha disponible.",
            f"Vuelve el espacio al snapshot {SNAPSHOT_DATE}, pero no indico el space ID.",
        ],
    },
    "get_genie_space_restore_job_run": {
        "purpose": "consultar el estado de un job run de restauracion de espacio",
        "closest": "get_genie_restore_points_job_run",
        "arguments": {"run_id": RESTORE_RUN_ID},
        "positive": f"Consulta el estado del job run de restauracion {RESTORE_RUN_ID}.",
        "alternative": f"El run {RESTORE_RUN_ID} esta restaurando un espacio; no esta listando restore points.",
        "indirect": [
            f"Comprueba si termino la recuperacion cuyo run ID es {RESTORE_RUN_ID}.",
            f"Sigue la ejecucion de restore identificada por {RESTORE_RUN_ID} y trae su salida si termino.",
        ],
        "missing": [
            "Comprueba si finalizo la restauracion del espacio.",
            "Dame el estado del restore anterior, pero no tengo el run ID.",
        ],
    },
}

TOOL_PROFILES = {
    name: ALL_TOOL_PROFILES[name] for names in EVALUATION_TOOL_GROUPS.values() for name in names
}


def _expected_arguments(
    arguments: dict[str, Any], *, absent: tuple[str, ...] = ()
) -> dict[str, dict[str, Any]]:
    expected = {name: {"matcher": "exact", "value": value} for name, value in arguments.items()}
    expected.update({name: {"matcher": "absent"} for name in absent})
    return expected


def _expected_tool_arguments(
    tool_name: str, arguments: dict[str, Any]
) -> dict[str, dict[str, Any]]:
    absent = ("confirmation",) if tool_name in CONFIRMATION_GATED_TOOLS else ()
    return _expected_arguments(arguments, absent=absent)


def _case(
    *,
    case_id: str,
    category: str,
    prompt: str,
    tool_under_test: str | None,
    expected_tools: list[str],
    forbidden_tools: list[str],
    should_use_tool: bool,
    expected_arguments: dict[str, Any] | None = None,
    expected_sequence: list[str] | None = None,
    allowed_sequences: list[list[str]] | None = None,
    tags: list[str] | None = None,
    expected_response_behavior: str = "answer",
) -> dict[str, Any]:
    return {
        "id": case_id,
        "category": category,
        "tool_under_test": tool_under_test,
        "tags": tags or [],
        "user_prompt": prompt,
        "expected_tools": expected_tools,
        "allowed_tools": [],
        "forbidden_tools": forbidden_tools,
        "expected_tool_sequence": expected_sequence or [],
        "allowed_tool_sequences": allowed_sequences or [],
        "should_use_tool": should_use_tool,
        "expected_arguments": expected_arguments or {},
        "expected_response_behavior": expected_response_behavior,
    }


DIRECT_CASE_SPECS: dict[str, dict[str, Any]] = {
    "find_atlan_assets_by_databricks_table": {
        "prompt": f"Find up to 5 Atlan assets that represent `{TABLE_ID}`. I only need the matching assets and GUIDs.",
        "arguments": {"table_identifier": TABLE_ID, "limit": 5},
        "forbidden": ["get_atlan_context_for_databricks_table"],
        "purpose": "Find Atlan assets that represent a Databricks table.",
        "closest": "get_atlan_context_for_databricks_table",
    },
    "get_atlan_context_for_databricks_table": {
        "prompt": f"Give me the business description, glossary terms, and README context for `{TABLE_ID}` from Atlan. Use at most one matching asset.",
        "arguments": {"table_identifier": TABLE_ID, "limit": 1},
        "forbidden": ["find_atlan_assets_by_databricks_table"],
        "purpose": "Retrieve business context from Atlan for a Databricks table.",
        "closest": "find_atlan_assets_by_databricks_table",
    },
    "list_genie_space_conversations": {
        "prompt": f"Show me the 5 most recent conversations in Genie Space `{SPACE_ID}`. I only need the conversation list for now.",
        "arguments": {"space_id": SPACE_ID, "include_all": False, "limit": 5},
        "forbidden": ["list_genie_conversation_messages"],
        "purpose": "List conversations in a Genie Space.",
        "closest": "list_genie_conversation_messages",
    },
    "list_genie_conversation_messages": {
        "prompt": f"Show up to 10 messages from conversation `{CONVERSATION_ID}` in Genie Space `{SPACE_ID}`.",
        "arguments": {"space_id": SPACE_ID, "conversation_id": CONVERSATION_ID, "limit": 10},
        "forbidden": ["list_genie_messages_for_conversations"],
        "purpose": "List messages from one Genie conversation.",
        "closest": "list_genie_messages_for_conversations",
    },
    "list_genie_messages_for_conversations": {
        "prompt": f"Fetch up to 10 messages for each of these conversations in one batch: `{CONVERSATION_ID}` and `{CONVERSATION_ID_2}`. They belong to Genie Space `{SPACE_ID}`.",
        "arguments": {
            "space_id": SPACE_ID,
            "conversation_ids": [CONVERSATION_ID, CONVERSATION_ID_2],
            "limit_per_conversation": 10,
        },
        "forbidden": ["list_genie_conversation_messages"],
        "purpose": "List messages for several Genie conversations in one call.",
        "closest": "list_genie_conversation_messages",
    },
    "get_genie_usage_metrics": {
        "prompt": f"Give me the completed usage metrics for Genie Space `{SPACE_ID}` over the last 7 days. Wait up to 30 seconds and poll every 2 seconds.",
        "arguments": {
            "space_id": SPACE_ID,
            "lookback_days": 7,
            "timeout_seconds": 30,
            "poll_interval_seconds": 2,
        },
        "forbidden": ["start_genie_usage_metrics_query"],
        "purpose": "Retrieve completed Genie usage metrics synchronously.",
        "closest": "start_genie_usage_metrics_query",
    },
    "start_genie_usage_metrics_query": {
        "prompt": f"Start a 7-day usage-metrics query for Genie Space `{SPACE_ID}` and return immediately with the statement ID.",
        "arguments": {"space_id": SPACE_ID, "lookback_days": 7},
        "forbidden": ["get_genie_usage_metrics"],
        "purpose": "Start an asynchronous Genie usage-metrics query.",
        "closest": "get_genie_usage_metrics",
    },
    "get_genie_usage_metrics_query_result": {
        "prompt": f"Check whether usage-metrics statement `{STATEMENT_ID}` has finished and return its result if available.",
        "arguments": {"statement_id": STATEMENT_ID},
        "forbidden": ["start_genie_usage_metrics_query"],
        "purpose": "Retrieve the status and result of a started metrics statement.",
        "closest": "start_genie_usage_metrics_query",
    },
    "list_genie_benchmark_runs": {
        "prompt": f"List the 5 most recent benchmark runs for Genie Space `{SPACE_ID}` so I can choose one to review.",
        "arguments": {"space_id": SPACE_ID, "limit": 5},
        "forbidden": ["get_genie_benchmark_run"],
        "purpose": "List benchmark runs for a Genie Space.",
        "closest": "get_genie_benchmark_run",
    },
    "get_genie_benchmark_run": {
        "prompt": f"Show the status and summary of benchmark run `{BENCHMARK_RUN_ID}` in Genie Space `{SPACE_ID}`.",
        "arguments": {"space_id": SPACE_ID, "run_id": BENCHMARK_RUN_ID},
        "forbidden": ["list_genie_benchmark_runs"],
        "purpose": "Retrieve one Genie benchmark run.",
        "closest": "list_genie_benchmark_runs",
    },
    "list_genie_benchmark_run_results": {
        "prompt": f"List the first 5 result rows from benchmark run `{BENCHMARK_RUN_ID}` in Genie Space `{SPACE_ID}`.",
        "arguments": {"space_id": SPACE_ID, "run_id": BENCHMARK_RUN_ID, "limit": 5},
        "forbidden": ["get_genie_benchmark_result_details"],
        "purpose": "List result rows from one benchmark run.",
        "closest": "get_genie_benchmark_result_details",
    },
    "get_genie_benchmark_result_details": {
        "prompt": f"Open benchmark result `{BENCHMARK_RESULT_ID}` from run `{BENCHMARK_RUN_ID}` in Genie Space `{SPACE_ID}` and show its details.",
        "arguments": {
            "space_id": SPACE_ID,
            "run_id": BENCHMARK_RUN_ID,
            "result_id": BENCHMARK_RESULT_ID,
        },
        "forbidden": ["list_genie_benchmark_run_results"],
        "purpose": "Retrieve the details of one benchmark result.",
        "closest": "list_genie_benchmark_run_results",
    },
    "start_genie_serialization_job": {
        "prompt": f"Prepare a serialization request for Genie Space `{SPACE_ID}`, but do not execute it. Stop at the approval step and tell me what confirmation is required.",
        "arguments": {"space_id": SPACE_ID},
        "forbidden": ["start_genie_space_restore_job"],
        "purpose": "Prepare serialization of a Genie Space without approving execution.",
        "closest": "get_genie_serialization_job_run",
    },
    "get_genie_serialization_job_run": {
        "prompt": f"Check the current status of serialization run `{SERIALIZATION_RUN_ID}`.",
        "arguments": {"run_id": SERIALIZATION_RUN_ID},
        "forbidden": ["get_genie_space_restore_job_run"],
        "purpose": "Check a serialization Job run.",
        "closest": "get_genie_space_restore_job_run",
    },
    "list_genie_space_restore_points": {
        "prompt": f"Find the restore points available for Genie Space `{SPACE_ID}`. Wait no more than 1 minute and poll every 5 seconds.",
        "arguments": {"space_id": SPACE_ID, "timeout_minutes": 1, "poll_interval_seconds": 5},
        "forbidden": ["start_genie_space_restore_job"],
        "purpose": "List available restore points for a Genie Space.",
        "closest": "start_genie_space_restore_job",
    },
    "get_genie_restore_points_job_run": {
        "prompt": f"Check restore-point discovery run `{RESTORE_POINTS_RUN_ID}` and return its output if it has completed.",
        "arguments": {"run_id": RESTORE_POINTS_RUN_ID},
        "forbidden": ["get_genie_space_restore_job_run"],
        "purpose": "Check a Job run that discovers restore points.",
        "closest": "get_genie_space_restore_job_run",
    },
    "start_genie_space_restore_job": {
        "prompt": f"Prepare a restore of Genie Space `{SPACE_ID}` to `{SNAPSHOT_DATE}`, but do not execute it. Stop at the approval step and report the required confirmation.",
        "arguments": {"space_id": SPACE_ID, "snapshot_date": SNAPSHOT_DATE},
        "forbidden": ["list_genie_space_restore_points"],
        "purpose": "Prepare a Genie Space restore without approving execution.",
        "closest": "list_genie_space_restore_points",
    },
    "get_genie_space_restore_job_run": {
        "prompt": f"Check the current status of Genie Space restore run `{RESTORE_RUN_ID}` and include its output if complete.",
        "arguments": {"run_id": RESTORE_RUN_ID},
        "forbidden": ["get_genie_restore_points_job_run"],
        "purpose": "Check a Genie Space restore Job run.",
        "closest": "get_genie_restore_points_job_run",
    },
}


def _direct_cases() -> list[dict[str, Any]]:
    return [
        _case(
            case_id=f"workflow-{name.replace('_', '-')}-direct",
            category="single_tool",
            prompt=spec["prompt"],
            tool_under_test=name,
            expected_tools=[name],
            forbidden_tools=spec["forbidden"],
            should_use_tool=True,
            expected_arguments=_expected_tool_arguments(name, spec["arguments"]),
            expected_sequence=[name],
            allowed_sequences=[[]] if name in CONFIRMATION_GATED_TOOLS else None,
            tags=["positive", "workflow"],
        )
        for name, spec in DIRECT_CASE_SPECS.items()
    ]


def _disambiguation_cases() -> list[dict[str, Any]]:
    specs = [
        (
            "workflow-disambiguate-atlan-assets",
            f"We are reconciling catalog records for `{TABLE_ID}`. Return the matching Atlan assets and their GUIDs; do not build a business-context summary yet.",
            "find_atlan_assets_by_databricks_table",
            {"table_identifier": TABLE_ID},
            "get_atlan_context_for_databricks_table",
        ),
        (
            "workflow-disambiguate-atlan-context",
            f"I already know the table is `{TABLE_ID}`. Give me its business meaning, glossary terms, and documentation from Atlan rather than a list of candidate assets.",
            "get_atlan_context_for_databricks_table",
            {"table_identifier": TABLE_ID},
            "find_atlan_assets_by_databricks_table",
        ),
        (
            "workflow-disambiguate-single-conversation",
            f"Review only conversation `{CONVERSATION_ID}` in Genie Space `{SPACE_ID}` and show its first 10 messages.",
            "list_genie_conversation_messages",
            {"space_id": SPACE_ID, "conversation_id": CONVERSATION_ID, "limit": 10},
            "list_genie_messages_for_conversations",
        ),
        (
            "workflow-disambiguate-batch-conversations",
            f"Compare messages from `{CONVERSATION_ID}` and `{CONVERSATION_ID_2}` in Genie Space `{SPACE_ID}` in one batch, using up to 10 messages per conversation.",
            "list_genie_messages_for_conversations",
            {
                "space_id": SPACE_ID,
                "conversation_ids": [CONVERSATION_ID, CONVERSATION_ID_2],
                "limit_per_conversation": 10,
            },
            "list_genie_conversation_messages",
        ),
        (
            "workflow-disambiguate-synchronous-usage",
            f"I need the completed 7-day usage numbers for Genie Space `{SPACE_ID}` in this response, not a statement ID to poll later.",
            "get_genie_usage_metrics",
            {"space_id": SPACE_ID, "lookback_days": 7},
            "start_genie_usage_metrics_query",
        ),
        (
            "workflow-disambiguate-asynchronous-usage",
            f"Start the 7-day usage calculation for Genie Space `{SPACE_ID}` and return immediately so another process can poll it later.",
            "start_genie_usage_metrics_query",
            {"space_id": SPACE_ID, "lookback_days": 7},
            "get_genie_usage_metrics",
        ),
        (
            "workflow-disambiguate-benchmark-results",
            f"Show up to 5 result rows from benchmark run `{BENCHMARK_RUN_ID}` in Genie Space `{SPACE_ID}`. Do not open an individual result yet.",
            "list_genie_benchmark_run_results",
            {"space_id": SPACE_ID, "run_id": BENCHMARK_RUN_ID, "limit": 5},
            "get_genie_benchmark_result_details",
        ),
        (
            "workflow-disambiguate-restore-points-run",
            f"Run `{RESTORE_POINTS_RUN_ID}` was discovering available restore points. Check that run, not the run that performs a restore.",
            "get_genie_restore_points_job_run",
            {"run_id": RESTORE_POINTS_RUN_ID},
            "get_genie_space_restore_job_run",
        ),
    ]
    return [
        _case(
            case_id=case_id,
            category="disambiguation",
            prompt=prompt,
            tool_under_test=tool,
            expected_tools=[tool],
            forbidden_tools=[forbidden],
            should_use_tool=True,
            expected_arguments=_expected_tool_arguments(tool, arguments),
            expected_sequence=[tool],
            tags=["positive", "confusion", "workflow"],
        )
        for case_id, prompt, tool, arguments, forbidden in specs
    ]


def _ambiguous_cases() -> list[dict[str, Any]]:
    specs = [
        (
            "workflow-missing-atlan-table",
            "Find the Atlan entry for our orders table.",
            "find_atlan_assets_by_databricks_table",
        ),
        (
            "workflow-missing-space-conversations",
            "Show me the latest Genie conversations for the sales team.",
            "list_genie_space_conversations",
        ),
        (
            "workflow-missing-conversation-id",
            f"Show the messages from the conversation in Genie Space `{SPACE_ID}` that we discussed earlier.",
            "list_genie_conversation_messages",
        ),
        (
            "workflow-missing-statement-id",
            "Check whether the usage-metrics query has finished.",
            "get_genie_usage_metrics_query_result",
        ),
        (
            "workflow-missing-benchmark-run",
            f"Open the benchmark run for Genie Space `{SPACE_ID}` that failed yesterday.",
            "get_genie_benchmark_run",
        ),
        (
            "workflow-missing-benchmark-result",
            f"Open the failed result from benchmark run `{BENCHMARK_RUN_ID}` in Genie Space `{SPACE_ID}`.",
            "get_genie_benchmark_result_details",
        ),
        (
            "workflow-missing-serialization-run",
            "Check whether the serialization job has finished.",
            "get_genie_serialization_job_run",
        ),
        (
            "workflow-missing-restore-date",
            f"Prepare to restore Genie Space `{SPACE_ID}` to the snapshot we approved.",
            "start_genie_space_restore_job",
        ),
    ]
    return [
        _case(
            case_id=case_id,
            category="ambiguous",
            prompt=prompt,
            tool_under_test=tool,
            expected_tools=[],
            forbidden_tools=[tool],
            should_use_tool=False,
            tags=["not_selected", "missing_parameters", "workflow"],
            expected_response_behavior="ask_clarification",
        )
        for case_id, prompt, tool in specs
    ]


def _abstention_cases() -> list[dict[str, Any]]:
    specs = [
        (
            "workflow-no-tool-summarize-metrics",
            "The weekly usage report says 42 questions, 11 active users, and 3 negative feedback events. Summarize those figures for my status update.",
            ["get_genie_usage_metrics"],
        ),
        (
            "workflow-no-tool-benchmark-email",
            "Draft a short email from these benchmark results: 18 passed, 2 failed, and both failures were timeout-related.",
            ["list_genie_benchmark_run_results", "get_genie_benchmark_result_details"],
        ),
        (
            "workflow-no-tool-rewrite-transcript",
            "Rewrite this conversation note more clearly: 'user asked sales total; assistant returned 4.2M; user requested regional split.'",
            ["list_genie_conversation_messages"],
        ),
        (
            "workflow-no-tool-compare-definitions",
            "Compare these supplied definitions: 'gross revenue is before deductions' and 'net revenue is after discounts and returns.'",
            ["get_atlan_context_for_databricks_table"],
        ),
        (
            "workflow-no-tool-explain-restore-point",
            "Explain in plain English what a restore point is and why a team might keep one. Do not inspect any workspace resources.",
            ["list_genie_space_restore_points"],
        ),
        (
            "workflow-no-tool-draft-checklist",
            "Create a generic pre-deployment checklist for taking a snapshot and planning a rollback. Do not start any jobs.",
            ["start_genie_serialization_job", "start_genie_space_restore_job"],
        ),
    ]
    return [
        _case(
            case_id=case_id,
            category="abstention",
            prompt=prompt,
            tool_under_test=None,
            expected_tools=[],
            forbidden_tools=forbidden,
            should_use_tool=False,
            tags=["not_selected", "workflow"],
            expected_response_behavior="answer_without_tools",
        )
        for case_id, prompt, forbidden in specs
    ]


def _sequence_cases() -> list[dict[str, Any]]:
    return [
        _case(
            case_id="workflow-sequence-atlan-context",
            category="tool_sequence",
            prompt=f"Locate the Atlan asset for `{TABLE_ID}`, then use it to give me the table's business context.",
            tool_under_test=None,
            expected_tools=[
                "find_atlan_assets_by_databricks_table",
                "get_atlan_context_for_databricks_table",
            ],
            forbidden_tools=[],
            should_use_tool=True,
            expected_sequence=[
                "find_atlan_assets_by_databricks_table",
                "get_atlan_context_for_databricks_table",
            ],
            allowed_sequences=[["get_atlan_context_for_databricks_table"]],
            expected_arguments={
                "find_atlan_assets_by_databricks_table": _expected_arguments(
                    {"table_identifier": TABLE_ID}
                ),
                "get_atlan_context_for_databricks_table": _expected_arguments(
                    {"table_identifier": TABLE_ID}
                ),
            },
            tags=["sequence", "workflow", "alternative_sequence"],
        ),
        _case(
            case_id="workflow-sequence-conversation-review",
            category="tool_sequence",
            prompt=f"In Genie Space `{SPACE_ID}`, list the 5 most recent conversations and show up to 10 messages from the first one.",
            tool_under_test=None,
            expected_tools=["list_genie_space_conversations", "list_genie_conversation_messages"],
            forbidden_tools=[],
            should_use_tool=True,
            expected_sequence=[
                "list_genie_space_conversations",
                "list_genie_conversation_messages",
            ],
            allowed_sequences=[
                ["list_genie_space_conversations", "list_genie_messages_for_conversations"]
            ],
            expected_arguments={
                "list_genie_space_conversations": _expected_arguments(
                    {"space_id": SPACE_ID, "include_all": False, "limit": 5}
                ),
                "list_genie_conversation_messages": _expected_arguments(
                    {"space_id": SPACE_ID, "conversation_id": CONVERSATION_ID, "limit": 10}
                ),
                "list_genie_messages_for_conversations": _expected_arguments(
                    {
                        "space_id": SPACE_ID,
                        "conversation_ids": [CONVERSATION_ID],
                        "limit_per_conversation": 10,
                    }
                ),
            },
            tags=["sequence", "workflow", "alternative_sequence"],
        ),
        _case(
            case_id="workflow-sequence-conversation-batch-review",
            category="tool_sequence",
            prompt=f"List recent conversations in Genie Space `{SPACE_ID}`, then fetch up to 10 messages for the first two conversations in one batch.",
            tool_under_test=None,
            expected_tools=[
                "list_genie_space_conversations",
                "list_genie_messages_for_conversations",
            ],
            forbidden_tools=["list_genie_conversation_messages"],
            should_use_tool=True,
            expected_sequence=[
                "list_genie_space_conversations",
                "list_genie_messages_for_conversations",
            ],
            expected_arguments={
                "list_genie_space_conversations": _expected_arguments({"space_id": SPACE_ID}),
                "list_genie_messages_for_conversations": _expected_arguments(
                    {
                        "space_id": SPACE_ID,
                        "conversation_ids": [CONVERSATION_ID, CONVERSATION_ID_2],
                        "limit_per_conversation": 10,
                    }
                ),
            },
            tags=["sequence", "workflow"],
        ),
        _case(
            case_id="workflow-sequence-usage-async",
            category="tool_sequence",
            prompt=f"Start a 7-day usage query for Genie Space `{SPACE_ID}`, then check the returned statement once and report its current result.",
            tool_under_test=None,
            expected_tools=[
                "start_genie_usage_metrics_query",
                "get_genie_usage_metrics_query_result",
            ],
            forbidden_tools=[],
            should_use_tool=True,
            expected_sequence=[
                "start_genie_usage_metrics_query",
                "get_genie_usage_metrics_query_result",
            ],
            allowed_sequences=[["get_genie_usage_metrics"]],
            expected_arguments={
                "start_genie_usage_metrics_query": _expected_arguments(
                    {"space_id": SPACE_ID, "lookback_days": 7}
                ),
                "get_genie_usage_metrics_query_result": _expected_arguments(
                    {"statement_id": STATEMENT_ID}
                ),
                "get_genie_usage_metrics": _expected_arguments(
                    {"space_id": SPACE_ID, "lookback_days": 7}
                ),
            },
            tags=["sequence", "workflow", "alternative_sequence"],
        ),
        _case(
            case_id="workflow-sequence-usage-and-conversations",
            category="tool_sequence",
            prompt=f"For Genie Space `{SPACE_ID}`, get the completed 7-day usage metrics and then list the 5 most recent conversations for follow-up.",
            tool_under_test=None,
            expected_tools=["get_genie_usage_metrics", "list_genie_space_conversations"],
            forbidden_tools=[],
            should_use_tool=True,
            expected_sequence=["get_genie_usage_metrics", "list_genie_space_conversations"],
            expected_arguments={
                "get_genie_usage_metrics": _expected_arguments(
                    {"space_id": SPACE_ID, "lookback_days": 7}
                ),
                "list_genie_space_conversations": _expected_arguments(
                    {"space_id": SPACE_ID, "limit": 5}
                ),
            },
            tags=["sequence", "workflow"],
        ),
        _case(
            case_id="workflow-sequence-benchmark-drilldown",
            category="tool_sequence",
            prompt=f"In Genie Space `{SPACE_ID}`, list the 5 latest benchmark runs, list 5 results from the first run, and open the first result in detail.",
            tool_under_test=None,
            expected_tools=[
                "list_genie_benchmark_runs",
                "list_genie_benchmark_run_results",
                "get_genie_benchmark_result_details",
            ],
            forbidden_tools=[],
            should_use_tool=True,
            expected_sequence=[
                "list_genie_benchmark_runs",
                "list_genie_benchmark_run_results",
                "get_genie_benchmark_result_details",
            ],
            expected_arguments={
                "list_genie_benchmark_runs": _expected_arguments(
                    {"space_id": SPACE_ID, "limit": 5}
                ),
                "list_genie_benchmark_run_results": _expected_arguments(
                    {"space_id": SPACE_ID, "run_id": BENCHMARK_RUN_ID, "limit": 5}
                ),
                "get_genie_benchmark_result_details": _expected_arguments(
                    {
                        "space_id": SPACE_ID,
                        "run_id": BENCHMARK_RUN_ID,
                        "result_id": BENCHMARK_RESULT_ID,
                    }
                ),
            },
            tags=["sequence", "workflow"],
        ),
        _case(
            case_id="workflow-sequence-benchmark-run-review",
            category="tool_sequence",
            prompt=f"List the recent benchmark runs for Genie Space `{SPACE_ID}`, then open the first run and summarize its status.",
            tool_under_test=None,
            expected_tools=["list_genie_benchmark_runs", "get_genie_benchmark_run"],
            forbidden_tools=[],
            should_use_tool=True,
            expected_sequence=["list_genie_benchmark_runs", "get_genie_benchmark_run"],
            expected_arguments={
                "list_genie_benchmark_runs": _expected_arguments({"space_id": SPACE_ID}),
                "get_genie_benchmark_run": _expected_arguments(
                    {"space_id": SPACE_ID, "run_id": BENCHMARK_RUN_ID}
                ),
            },
            tags=["sequence", "workflow"],
        ),
        _case(
            case_id="workflow-sequence-snapshot-readiness",
            category="tool_sequence",
            prompt=f"Check serialization run `{SERIALIZATION_RUN_ID}`, then find restore points for Genie Space `{SPACE_ID}` and follow the discovery run if it is still pending.",
            tool_under_test=None,
            expected_tools=[
                "get_genie_serialization_job_run",
                "list_genie_space_restore_points",
                "get_genie_restore_points_job_run",
            ],
            forbidden_tools=["start_genie_space_restore_job"],
            should_use_tool=True,
            expected_sequence=[
                "get_genie_serialization_job_run",
                "list_genie_space_restore_points",
                "get_genie_restore_points_job_run",
            ],
            expected_arguments={
                "get_genie_serialization_job_run": _expected_arguments(
                    {"run_id": SERIALIZATION_RUN_ID}
                ),
                "list_genie_space_restore_points": _expected_arguments({"space_id": SPACE_ID}),
                "get_genie_restore_points_job_run": _expected_arguments(
                    {"run_id": RESTORE_POINTS_RUN_ID}
                ),
            },
            tags=["sequence", "workflow", "synthetic_only"],
        ),
        _case(
            case_id="workflow-sequence-restore-points-status",
            category="tool_sequence",
            prompt=f"Find restore points for Genie Space `{SPACE_ID}` and, if the discovery times out, check the returned run once for its output.",
            tool_under_test=None,
            expected_tools=["list_genie_space_restore_points", "get_genie_restore_points_job_run"],
            forbidden_tools=["start_genie_space_restore_job"],
            should_use_tool=True,
            expected_sequence=[
                "list_genie_space_restore_points",
                "get_genie_restore_points_job_run",
            ],
            expected_arguments={
                "list_genie_space_restore_points": _expected_arguments({"space_id": SPACE_ID}),
                "get_genie_restore_points_job_run": _expected_arguments(
                    {"run_id": RESTORE_POINTS_RUN_ID}
                ),
            },
            tags=["sequence", "workflow", "synthetic_only"],
        ),
        _case(
            case_id="workflow-sequence-restore-preflight",
            category="tool_sequence",
            prompt=f"Find the restore points for Genie Space `{SPACE_ID}`, follow the discovery run if needed, and prepare a restore to the first available date. Do not approve or execute the restore.",
            tool_under_test=None,
            expected_tools=[
                "list_genie_space_restore_points",
                "get_genie_restore_points_job_run",
                "start_genie_space_restore_job",
            ],
            forbidden_tools=[],
            should_use_tool=True,
            expected_sequence=[
                "list_genie_space_restore_points",
                "get_genie_restore_points_job_run",
                "start_genie_space_restore_job",
            ],
            allowed_sequences=[
                ["list_genie_space_restore_points", "get_genie_restore_points_job_run"]
            ],
            expected_arguments={
                "list_genie_space_restore_points": _expected_arguments({"space_id": SPACE_ID}),
                "get_genie_restore_points_job_run": _expected_arguments(
                    {"run_id": RESTORE_POINTS_RUN_ID}
                ),
                "start_genie_space_restore_job": _expected_arguments(
                    {"space_id": SPACE_ID, "snapshot_date": SNAPSHOT_DATE}, absent=("confirmation",)
                ),
            },
            tags=["sequence", "workflow", "synthetic_only"],
        ),
    ]


def _synthetic_results() -> dict[str, dict[str, Any]]:
    results = {
        "health": {"status": "healthy"},
        "get_current_user": {"user_name": USER_NAME, "active": True},
        "list_available_genie_spaces": {
            "spaces": [{"space_id": SPACE_ID, "title": "Evaluation Space"}]
        },
        "get_genie_space_details": {"space": {"space_id": SPACE_ID, "title": "Evaluation Space"}},
        "list_genie_space_tags": {
            "space": {"space_id": SPACE_ID},
            "tags": [{"tag_key": "environment", "tag_value": "sandbox"}],
        },
        "find_genie_spaces_by_tag": {
            "spaces": [{"space_id": SPACE_ID, "title": "Evaluation Space"}]
        },
        "find_atlan_assets_by_databricks_table": {
            "matches": [{"guid": "eval-asset-guid", "qualified_name": TABLE_ID}]
        },
        "get_atlan_context_for_databricks_table": {
            "combined_context_text": "Synthetic business context for evaluation."
        },
        "get_user_name_from_id": {"user_id": USER_ID, "user_name": USER_NAME, "found": True},
        "list_genie_space_conversations": {
            "conversations": [
                {"conversation_id": CONVERSATION_ID},
                {"conversation_id": CONVERSATION_ID_2},
            ]
        },
        "list_genie_conversation_messages": {
            "conversation_id": CONVERSATION_ID,
            "messages": [{"message_id": "eval-message-1", "content": "Synthetic message"}],
        },
        "list_genie_messages_for_conversations": {
            "results": [
                {"conversation_id": CONVERSATION_ID, "messages": []},
                {"conversation_id": CONVERSATION_ID_2, "messages": []},
            ]
        },
        "get_genie_usage_metrics": {"succeeded": True, "metrics": {"total_questions": 7}},
        "start_genie_usage_metrics_query": {
            "statement_id": STATEMENT_ID,
            "done": False,
            "result_tool": "get_genie_usage_metrics_query_result",
        },
        "get_genie_usage_metrics_query_result": {
            "statement_id": STATEMENT_ID,
            "done": True,
            "succeeded": True,
            "metrics": {"total_questions": 7},
        },
        "list_genie_benchmark_runs": {
            "benchmark_runs": [{"id": BENCHMARK_RUN_ID, "state": "COMPLETED"}]
        },
        "get_genie_benchmark_run": {
            "benchmark_run": {"id": BENCHMARK_RUN_ID, "state": "COMPLETED"}
        },
        "list_genie_benchmark_run_results": {
            "results": [{"id": BENCHMARK_RESULT_ID, "status": "FAILED"}]
        },
        "get_genie_benchmark_result_details": {
            "result": {"id": BENCHMARK_RESULT_ID, "status": "FAILED"}
        },
        "list_genie_space_permissions": {
            "permissions": [{"user_name": USER_NAME, "permission_level": "CAN_READ"}]
        },
        "grant_space_permissions": {
            "executed": False,
            "confirmation_required": True,
            "required_confirmation": "CONFIRM GRANT GENIE SPACE PERMISSIONS",
        },
        "start_genie_serialization_job": {
            "executed": False,
            "synthetic_run_id": SERIALIZATION_RUN_ID,
            "confirmation_required": True,
        },
        "get_genie_serialization_job_run": {
            "run": {"run_id": SERIALIZATION_RUN_ID, "state": "TERMINATED"}
        },
        "list_genie_space_restore_points": {
            "executed": False,
            "run_id": RESTORE_POINTS_RUN_ID,
            "completed": False,
            "timed_out": True,
            "run": {"run_id": RESTORE_POINTS_RUN_ID, "state": "RUNNING"},
            "output": None,
        },
        "get_genie_restore_points_job_run": {
            "run": {"run_id": RESTORE_POINTS_RUN_ID, "state": "TERMINATED"},
            "output": {"restore_points": [SNAPSHOT_DATE]},
        },
        "start_genie_space_restore_job": {
            "executed": False,
            "synthetic_run_id": RESTORE_RUN_ID,
            "confirmation_required": True,
        },
        "get_genie_space_restore_job_run": {
            "run": {"run_id": RESTORE_RUN_ID, "state": "TERMINATED"}
        },
    }
    return {name: results[name] for name in TOOL_PROFILES}


def build_dataset() -> dict[str, Any]:
    cases = [
        *_direct_cases(),
        *_disambiguation_cases(),
        *_ambiguous_cases(),
        *_abstention_cases(),
        *_sequence_cases(),
    ]
    for case in cases:
        expected_sequence = case["expected_tool_sequence"] or case["expected_tools"]
        allowed_tools = {
            tool_name for sequence in case["allowed_tool_sequences"] for tool_name in sequence
        }
        if CONFIRMATION_GATED_TOOLS.intersection((*expected_sequence, *allowed_tools)):
            case["expected_response_behavior"] = "confirmation_required"
    return {
        "version": 4,
        "name": "mcp_workflow_tool_selection_50",
        "description": (
            f"Fifty curated English workflow cases for {len(TOOL_PROFILES)} MCP tools covering "
            "Atlan, conversations, usage, benchmarks, serialization, and restore flows."
        ),
        "review_status": "pending_user_review",
        "execution_policy": {
            "mode": "synthetic_only",
            "real_tool_execution": False,
            "reason": "Evaluate selection and arguments without API calls, SQL, Jobs, or mutations.",
        },
        "defaults": {
            "model": DEFAULT_MODEL,
            "max_turns": 6,
            "model_retries": 0,
            "mcp_retries": 0,
        },
        "coverage_requirements_per_tool": {"positive": 1},
        "identifiers": {
            "space_id": SPACE_ID,
            "conversation_id": CONVERSATION_ID,
            "benchmark_run_id": BENCHMARK_RUN_ID,
            "benchmark_result_id": BENCHMARK_RESULT_ID,
            "statement_id": STATEMENT_ID,
            "serialization_run_id": SERIALIZATION_RUN_ID,
            "restore_points_run_id": RESTORE_POINTS_RUN_ID,
            "restore_run_id": RESTORE_RUN_ID,
            "table_identifier": TABLE_ID,
            "snapshot_date": SNAPSHOT_DATE,
        },
        "tool_groups": {group: list(names) for group, names in EVALUATION_TOOL_GROUPS.items()},
        "tool_inventory": list(TOOL_PROFILES),
        "tool_profiles": {
            name: {
                "purpose": spec["purpose"],
                "closest_tool": spec["closest"],
                "example_arguments": spec["arguments"],
            }
            for name, spec in DIRECT_CASE_SPECS.items()
        },
        "synthetic_results": _synthetic_results(),
        "cases": cases,
    }


def _validate_coverage(dataset: dict[str, Any]) -> None:
    requirements = dataset["coverage_requirements_per_tool"]
    for tool_name in dataset["tool_inventory"]:
        cases = [case for case in dataset["cases"] if case["tool_under_test"] == tool_name]
        counts = {tag: sum(tag in case["tags"] for case in cases) for tag in requirements}
        missing = {
            tag: f"{counts[tag]}/{minimum}"
            for tag, minimum in requirements.items()
            if counts[tag] < minimum
        }
        if missing:
            raise ValueError(f"Coverage missing for {tool_name}: {missing}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the editable tool-selection dataset")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).with_name("tool_selection_dataset.yaml"),
    )
    args = parser.parse_args()

    dataset = build_dataset()
    _validate_coverage(dataset)
    args.output.write_text(
        yaml.safe_dump(dataset, sort_keys=False, allow_unicode=True, width=110),
        encoding="utf-8",
    )
    print(f"Wrote {len(dataset['cases'])} cases to {args.output}")


if __name__ == "__main__":
    main()
