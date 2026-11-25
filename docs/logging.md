# Logs del proyecto: guía de uso e integración

Esta guía explica **cómo registra eventos el sistema, dónde se guardan, cómo
leerlos y cómo añadirlos**.

> **Resumen**
> - Cada proceso escribe sus eventos en **`logs/<proceso>.jsonl`**: un objeto
>   JSON por línea, en el momento en que ocurren.
> - Se registra con `log = get_logger(__name__)` y
>   `log.info("componente.evento", campo=valor)`.
> - **Nunca** se debe registrar el texto de los mensajes, contraseñas, claves, enlaces ni
>   alias. El formateador los tacha si se escapan, y una prueba lo verifica.
> - Para leerlos en vivo: `tail -f logs/client-alice.jsonl | python tools/logview.py`.

## Utilidad de los logs

1. **Depurar un sistema distribuido.** Un mensaje pasa por el cliente emisor,
   tres nodos y el cliente receptor, en procesos distintos. No hay un punto
   donde poner un depurador: los logs son la única forma de reconstruir qué
   pasó y en qué orden.
2. **Servir de evidencia.** La metodología del curso pide *"logs
   estructurados e IDs estables"* y que la evidencia de los experimentos sean
   *"logs, métricas, trazas e historias legibles por máquina"*, no capturas.
3. **Defender el proyecto.** En la defensa individual pueden pedirnos
   *"interpretar una traza"*. Con `tools/logview.py --msg <id>` se ve el
   recorrido de un mensaje por todos los procesos.

## Dónde se guardan

| Proceso | Archivo | Quién lo configura |
|---|---|---|
| Un nodo de servicio (actor de Ray) | `logs/node-<id>.jsonl` | El `__init__` del actor (ver sección 7) |
| Un cliente (actor de Ray) | `logs/client-<dispositivo>.jsonl` | `RayClientFactory`, al construir el cliente dentro del actor |

- La carpeta es **`logs/`**, relativa al directorio desde el que se lanza.
  Se cambia con el argumento `log_dir` o con la variable de entorno
  **`P08_LOG_DIR`**. Con Ray, usad una **ruta absoluta** (sección 7).
- **`logs/` y `*.jsonl` están en `.gitignore`**: los logs nunca se suben al repo.
- Las ejecuciones **se añaden** al mismo archivo, sin borrar lo anterior. Cada
  ejecución lleva su propio **`run`** (8 caracteres aleatorios), así que se
  pueden separar.
- Cada evento **se escribe en el disco en el momento**, así que el archivo se
  puede seguir en vivo mientras el sistema funciona.

## Cómo leerlos

```bash
# En vivo, mientras funciona un cliente (en OTRA terminal):
tail -f logs/client-alice.jsonl | python tools/logview.py

# Todos los procesos juntos, ordenados por tiempo:
python tools/logview.py logs/*.jsonl

# El recorrido de UN mensaje por todos los procesos (basta un prefijo del id):
python tools/logview.py logs/*.jsonl --msg 9c1e04b7

# Solo avisos y errores, o solo ciertos eventos:
python tools/logview.py logs/*.jsonl --level WARNING
python tools/logview.py logs/*.jsonl --event delivery. --event receive.

# Solo lo de un cliente o un nodo:
python tools/logview.py logs/*.jsonl --who node-2
```

`logview` muestra cada evento en una línea corta:

```
22:28:04.512 INFO 3fa2c1d0   delivery.mailbox_accepted  msg=9c1e04b7a2f14e0d8b6c5a4f3e2d1c0b seq=4 attempt=0 confirmed_by=node-1,node-3
```

Con **`jq`** también se puede filtrar el JSON directamente:

```bash
jq -c 'select(.event == "receive.rejected") | {ts, client, reason}' logs/client-alice.jsonl
jq -c 'select(.level == "WARNING" or .level == "ERROR")' logs/*.jsonl
```

## Formato de cada línea

Una línea real del archivo:

```json
{"ts":"2026-11-23T22:28:04.512Z","level":"INFO","logger":"p08.delivery","event":"delivery.mailbox_accepted","pid":308344,"process":"client-alice","run":"ea8ae5fd","client":"3fa2c1d0","msg":"9c1e04b7a2f14e0d8b6c5a4f3e2d1c0b","seq":4,"attempt":0,"confirmed_by":["node-1","node-3"]}
```

**Campos que pone el sistema**:

| Campo | Contenido |
|---|---|
| `ts` | Hora **UTC**, ISO 8601 con milisegundos |
| `level` | `DEBUG`, `INFO`, `WARNING` o `ERROR` |
| `logger` | `p08.<módulo>`, por ejemplo `p08.delivery` |
| `event` | El nombre del evento (sección 5) |
| `pid` | El proceso del sistema operativo (distingue reinicios de un actor) |
| `process`, `run` | El proceso lógico (`client-alice`, `node-2`…) y la ejecución |
| `client` / `node` | **Quién emite** (sección 6) |
| `error`, `traceback` | Solo con `log.exception(...)` |

**Campos de cada evento**, con estos nombres siempre que apliquen (los pasa el desarrollador):

| Campo | Qué es | Formato en el log |
|---|---|---|
| `msg` | `MessageId` | **Completo**: es aleatorio y es lo que permite seguir un mensaje entre procesos |
| `peer`, `user` | `UserId` del otro usuario | **Acortado a 8 caracteres** automáticamente |
| `conv` | `ConversationId` | Acortado a 8 automáticamente |
| `node`, `nodes`, `confirmed_by` | `NodeId` o listas de ellos | Tal cual |
| `seq`, `attempt` | Enteros | Tal cual |
| `duration_ms`, `retry_in_ms` | Milisegundos | Tal cual |
| `outcome`, `reason`, `status` | Resultado o motivo | El valor del `Enum` (`"accepted"`, `"bad_signature"`…) |

Pasa los **objetos del dominio** (`UserId`, `MessageId`, `Enum`…):
el formateador sabe cómo escribir cada uno y cuáles acortar.

## Cómo registrar un evento

```python
from source.application.observability import get_logger

log = get_logger(__name__)  # una vez por módulo

log.info(
    "delivery.mailbox_accepted",
    msg=message.message_id,
    seq=message.seq,
    attempt=message.attempts,
    confirmed_by=result.confirmed_by,
)

log.warning(
    "receive.rejected",
    msg=envelope.message_id,
    peer=envelope.sender,
    reason=ReceiveStatus.BAD_SIGNATURE,
)

try:
    ...
except StoreError:
    log.exception(
        "store.commit_failed", conv=conversation_id
    )  # añade error y traceback
```

**Reglas para nombrar eventos:**

- Formato **`componente.acción_o_resultado`**, en minúsculas, en inglés y con
  `_`: `delivery.retry_scheduled`, `node.state_changed`, `contact.accepted`.
- **Un nombre no cambia nunca**: se usa para buscar, contar y comparar
  ejecuciones. Si cambia el significado, se crea otro nombre.
- **Nada de frases sueltas** (`log.info(f"sent {x} to {y}")`): todo dato va
  en un campo con nombre.
- Los **campos**, también en inglés y en `snake_case`.

## Quién emite: el contexto

No hace falta pasar "quién soy" en cada evento. Se añade solo, a partir de dos
contextos:

- **Contexto de proceso**, que se fija una vez al arrancar.
  `configure_logging` pone `process` y `run`; un nodo añade `node`:
  ```python
  configure_logging(process="node-2", node="node-2")
  ```
- **Contexto de llamada**, que vale mientras dura un bloque. Hace falta
  cuando varios clientes comparten un proceso, como en las pruebas:
  ```python
  from source.application.observability import log_context, short

  with log_context(client=short(user_id)):
      ...  # todo evento aquí dentro lleva client=3fa2c1d0
  ```
  Los bloques se anidan y se restauran al salir. Si Bob recibe un envío
  directo **dentro** de una llamada de Alice, los eventos de Bob salen con su
  propio `client`.

## Configuración por proceso, Ray

Cada proceso llama **una vez** a `configure_logging` (en
`source/composition/logging_setup.py`):

```python
from source.composition.logging_setup import configure_logging

configure_logging(process="client-alice")  # → logs/client-alice.jsonl
configure_logging(process="node-2", node="node-2")  # → logs/node-2.jsonl
```

Qué hace:
- Reemplaza cualquier handler anterior, así que llamarla dos veces no duplica
  líneas.
- Fija el nivel: el argumento `level`, si no **`P08_LOG_LEVEL`**, si no `INFO`.
- Silencia el ruido de librerías (`ray`, `grpc`…) por debajo de `WARNING`.
- **No escribe nada en la consola.**

### ⚠️ Con Ray: configurad el logging DENTRO de cada actor

Cada actor de Ray es **su propio proceso**. La configuración del proceso que
lanza el clúster **no llega a los actores**. Si un actor no llama a
`configure_logging` en su `__init__`, **sus eventos se pierden**:

```python
@ray.remote(max_restarts=-1)
class NodeActor:
    def __init__(self, node_id: str) -> None:
        configure_logging(process=f"node-{node_id}", node=node_id)  # ← lo primero
        log.info("actor.started", node=node_id)
        ...
```

Y desde el lanzador, pasad la carpeta **absoluta** y el nivel a todos los
procesos con variables de entorno:

```python
ray.init(
    runtime_env={
        "env_vars": {
            "P08_LOG_DIR": str(Path("logs").resolve()),  # ruta absoluta, común a todos
            "P08_LOG_LEVEL": "INFO",
        }
    }
)
```

Cuando un actor se reinicia (`max_restarts`), su `__init__` vuelve a
ejecutarse: el log continúa en el mismo archivo, con un `run` nuevo y otro
`pid`. Así se **ve** el reinicio en el log.

## Niveles: cuándo usar cada uno

| Nivel | Cuándo | Ejemplos |
|---|---|---|
| `DEBUG` | El detalle de cada paso, útil solo para depurar | Cada sobre recibido, **incluidos los duplicados**; cada `tick`; cada llamada del quórum |
| `INFO` | Un cambio de estado que importa | Mensaje encolado, aceptado o entregado; contacto aceptado; presencia publicada; nodo que vuelve |
| `WARNING` | Algo anómalo pero esperable, que el sistema maneja | Firma inválida, hueco saltado, quórum no alcanzado, solicitud repetida rechazada, nodo `DEAD`, posible suplantación |
| `ERROR` | Algo que no debería pasar | Fallo del almacén, estado corrupto, excepción inesperada en un actor |

**Regla contra el ruido:** lo que se repite por diseño va en `DEBUG`. Por
ejemplo, cada mensaje llega unas 3 veces por las réplicas, así que los
duplicados son `DEBUG`, nunca `INFO`.

## Privacidad:

**Nunca** registrar:

- el **texto** de un mensaje, ni en claro ni cifrado;
- **contraseñas, claves, firmas**;
- **enlaces de contacto** o solicitudes completas;
- **alias** (son datos personales: usa el id acortado).

Hay tres defensas, por si a alguien se le escapa:

1. **El formateador tacha** (`"[redacted]"`) cualquier campo con un nombre
   sensible: `body`, `text`, `content`, `plaintext`, `ciphertext`, `password`,
   `secret`, `private_key`, `key`, `signature`, `link`, `envelope`, `request`,
   `alias`, `name`. Lo hace también dentro de diccionarios anidados.
2. **Los `bytes` nunca se escriben**, solo su tamaño (`"<64 bytes>"`).
3. **Una prueba de privacidad** (`tests/composition/test_log_privacy.py`)
   ejecuta una sesión completa con nivel `DEBUG` y comprueba que ninguna frase,
   contraseña, enlace, alias, huella ni id de usuario completo aparece en el
   log. Cubre automáticamente cada evento nuevo.

## Dónde se registra, según la capa

| Capa | ¿Registra? | Qué |
|---|---|---|
| **Dominio** (`source/domain`) | ❌ **Nunca.** import-linter lo prohíbe: importar `logging` ahí hace fallar el CI | Es puro: **devuelve** sus decisiones (`Arrival.LATE`, `GapResult`…) y quien lo llama las registra |
| **Aplicación** (casos de uso) | ✅ | **En el punto donde se toma cada decisión, una sola vez.** Si el caso de uso lo registra, quien lo llama no lo repite |
| **Adaptadores** | ✅ | Fallos de infraestructura en la frontera: SQLite, tiempos agotados y reinicios de Ray |
| **Composición** | Solo configura | `configure_logging`, una vez por proceso |

Como los casos de uso ya devuelven resultados con nombre (`ReceiveStatus`,
`DeliveryOutcome`, `AddOutcome`…), integrar los logs suele ser **una línea
junto a cada `return`**.

## Catálogo de eventos

Este es el contrato de eventos. **Si añades uno, añádelo aquí.**
✅ implementado · ⏳ pendiente.

### Cliente (Persona A)

| Evento | Nivel | Campos | Dónde | |
|---|---|---|---|---|
| `account.created` | INFO | `user` | `IdentityService.register` | ⏳ |
| `login.succeeded` / `login.failed` | INFO / WARNING | `user` | `IdentityService.unlock` | ⏳ |
| `session.started` / `session.ended` | INFO | `user` | `ClientApp` | ⏳ |
| `message.queued` | INFO | `msg`, `conv`, `seq`, `kind` | `SendMessageService.queue` | ⏳ |
| `delivery.direct_delivered` | INFO | `msg`, `seq` | `DeliveryService` | ⏳ |
| `delivery.mailbox_accepted` | INFO | `msg`, `seq`, `attempt`, `confirmed_by` | `DeliveryService` | ⏳ |
| `delivery.retry_scheduled` | WARNING | `msg`, `seq`, `attempt`, `retry_in_ms` | `DeliveryService` | ⏳ |
| `delivery.expired` | WARNING | `msg`, `seq`, `attempt` | `DeliveryService` | ⏳ |
| `delivery.waiting_for_earlier` | DEBUG | `msg`, `seq` | `DeliveryService` | ⏳ |
| `receive.stored` | INFO | `msg`, `peer`, `seq`, `late` | `ReceiveMessagesService` | ⏳ |
| `receive.buffered` / `receive.duplicate` | DEBUG | `msg`, `peer`, `seq` | `ReceiveMessagesService` | ⏳ |
| `receive.rejected` | WARNING | `msg`, `peer`, `reason` (`bad_signature`, `unreadable`, `unknown_sender`…) | `ReceiveMessagesService` | ⏳ |
| `receive.gap_skipped` | WARNING | `peer`, `conv`, `from_seq`, `to_seq` | `ReceiveMessagesService.expire_gaps` | ⏳ |
| `contact.request_sent` / `contact.request_received` | INFO | `peer`, `outcome` | `ContactsService` | ⏳ |
| `contact.accepted` / `contact.rejected` / `contact.removed` | INFO | `peer` | `ContactsService` | ⏳ |
| `contact.look_alike` | WARNING | `peer`, `look_alikes` | `ContactsService` | ⏳ |
| `client.tick` | DEBUG | `collected`, `stored`, `gaps`, `delivered` | `ClientApp.tick` | ⏳ |

### Nodos y Ray (Persona B)

| Evento | Nivel | Campos | Dónde | |
|---|---|---|---|---|
| `quorum.write` | DEBUG | `kind` (`deposit`/`publish`), `msg` (deposit) o `user` (publish), `asked`, `confirmed_by`, `outcome` (`accepted`) | `QuorumClient`, si hubo quórum | ✅ |
| `quorum.write_failed` | WARNING | ídem, con `outcome` = `no_quorum` | `QuorumClient`, si no lo hubo (en lugar de `quorum.write`, nunca los dos) | ✅ |
| `node.state_changed` | INFO / WARNING (si pasa a `dead`) | `node`, `from_state`, `to_state` (`alive`/`suspect`/`dead`) | `MembershipService`, cuando nota el cambio (un nodo nunca contactado cuenta como `suspect`) | ✅ |
| `presence.published` / `presence.renew_failed` | DEBUG / WARNING | `version`, `expires_in_ms` (publicado: lo que dura la nueva presencia; fallido: lo que le queda a la anterior, `0` si ya no hay) | `DirectoryClient.publish` | ✅ |
| `record.accepted` / `record.rejected` | DEBUG / WARNING | `user`, `version` (solo si tienen el tipo correcto), y `outcome` (`stored`/`unchanged`) o `reason` (`rejected_malformed`, `rejected_expired`, `rejected_invalid_signature`, `rejected_stale`…) | `DirectoryService.publish` (nodo) | ✅ |
| `mailbox.deposited` | DEBUG | `msg`, `outcome` (`stored`/`already_present`) | `MailboxService` (nodo) | ⏳ |
| `mailbox.collected` / `mailbox.acknowledged` | DEBUG | `user`, `count` | `MailboxService` (nodo) | ⏳ |
| `mailbox.request_refused` | WARNING | `user`, `reason` (`replay`, `bad_signature`, `stale`) | `MailboxService` (nodo) | ⏳ |
| `actor.started` / `actor.restarted` | INFO / WARNING | `node` | Adaptadores de Ray | ⏳ |
| `actor.call_timeout` | WARNING | `node`, `method`, `timeout_ms` | Gateway de Ray | ⏳ |

### Infraestructura e interfaz (ya implementados)

| Evento | Nivel | Campos | Dónde | |
|---|---|---|---|---|
| `logging.configured` | INFO | `log_level`, `file` | `configure_logging` | ✅ |
| `client.online` | INFO | `user` | Actor del cliente, al registrarse o iniciar sesión | ✅ |
| `client.tick_failed` | ERROR | (traza) | Bucle del actor del cliente | ✅ |
| `direct.listening` / `direct.stopped` | INFO | `user` | `RayPeerTransport` | ✅ |
| `direct.unreachable` | DEBUG | `to`, `msg`, `reason` | `RayPeerTransport` | ✅ |
| `direct.not_settled` | DEBUG | `msg`, `reason` | `RayPeerTransport` | ✅ |
| `direct.rejected` | WARNING | `reason` | `RayPeerTransport` | ✅ |
| `cli.store_failed` | ERROR | `screen` | Pantallas de la CLI | ✅ |

## Pruebas

**Comprobar que un caso de uso emite el evento correcto** (los eventos también
quedan bajo contrato):

```python
import logging
from tests.support.logs import events, has_event


def test_a_forged_envelope_is_logged(caplog):
    caplog.set_level(logging.DEBUG, logger="p08")
    ...  # recibir un sobre con firma falsa
    assert has_event(caplog, "receive.rejected", reason=ReceiveStatus.BAD_SIGNATURE)
```

- `events(caplog)` devuelve `[(evento, campos), ...]`; `contexts(caplog)`, el
  contexto de cada uno.
- `tests/conftest.py` **restaura la configuración de logging** después de cada
  prueba, así que puedes llamar a `configure_logging` en una prueba sin afectar
  a las demás.
- La prueba de privacidad (sección 9) se ejecuta sola en cada `pytest`.

## Lista de comprobación al añadir un evento

- [ ] El nombre sigue `componente.acción`, en inglés, y no existía ya con otro significado.
- [ ] Se registra **donde se toma la decisión**, una sola vez.
- [ ] El nivel sigue la tabla de la sección 8 (lo repetitivo, en `DEBUG`).
- [ ] Los campos usan los nombres estándar (`msg`, `peer`, `conv`, `seq`, `reason`…) y se pasan como objetos del dominio.
- [ ] **No** incluye texto de mensajes, claves, contraseñas, enlaces ni alias.
- [ ] Hay una prueba con `has_event(...)` para los eventos importantes.
- [ ] Está en el catálogo de la sección 11.

## Archivos de referencia

| Archivo | Qué es |
|---|---|
| `source/application/observability.py` | La API: `get_logger`, `log_context`, `set_process_context`, `short` |
| `source/adapters/system/json_logs.py` | El formateador JSON Lines y las reglas de privacidad |
| `source/composition/logging_setup.py` | `configure_logging`: destino, nivel y variables de entorno |
| `tools/logview.py` | Lector y trazador de logs |
| `tests/support/logs.py` | Ayudas para comprobar eventos en las pruebas |
| `tests/composition/test_log_privacy.py` | La prueba de privacidad |
