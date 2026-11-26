# P08 · Mensajería segura descentralizada

[English](README.md) · **Español**

Un servicio de mensajería en el que los usuarios pueden estar desconectados,
ninguna base de datos central guarda las conversaciones y solo las dos personas
de una conversación pueden leerla.

Proyecto final de *Sistemas Distribuidos* (2026), Facultad de Matemática y
Computación, Universidad de La Habana.

## El problema

Cuando los usuarios pueden estar desconectados y no se permite almacenar
permanentemente toda la conversación en una base de datos central, la
mensajería exige entrega diferida, descubrimiento, orden, reintentos, supresión
de duplicados, persistencia local, identidad y privacidad.

**Objetivo:** construir un servicio de mensajería descentralizada con identidad
de usuario, descubrimiento de contactos, conversaciones uno a uno, entrega a
destinatarios desconectados y comunicación autenticada y cifrada.

**Alcance que exige el enunciado**

| Requisito | Cómo lo cumple este proyecto |
|---|---|
| Identidad de usuarios y descubrimiento de contactos | Un usuario **es** un par de claves: su id es el hash de la clave pública, así que nadie lo asigna ni lo aprueba. Los contactos se añaden compartiendo un **enlace de contacto firmado**. Un directorio replicado indica quién está conectado en cada momento |
| Conversaciones uno a uno | Cada pareja de usuarios comparte una conversación |
| Entrega a destinatarios sin conexión | Los mensajes esperan en **buzones** replicados en 3 nodos de servicio |
| Entrega diferida y *store-and-forward* | Cada cliente guarda una cola de salida duradera y reintenta, con espera exponencial y aleatoria, hasta que un quórum de buzones (2 de 3 nodos) acepta el mensaje |
| Ordenación de mensajes | Orden FIFO por emisor con detección de huecos, más **relojes de Lamport** para que los dos participantes vean el mismo historial, en orden causal, digan lo que digan sus relojes |
| Supresión de duplicados | Cada mensaje tiene un id único; el receptor recuerda qué ids ya guardó y los nodos guardan cada sobre una sola vez |
| Persistencia local del historial | SQLite en cada dispositivo: historial, cola de salida, contactos y estado de orden sobreviven a un reinicio |
| Autenticación y cifrado | De extremo a extremo: cifrar y después firmar, con libsodium (PyNaCl). Los nodos solo ven quién escribe a quién, nunca el texto, y no pueden falsificar ni alterar un mensaje |
| Relés temporales, sin base de datos central permanente | Los buzones solo guardan un mensaje hasta que su receptor lo recoge o hasta que caduca |

## Estado: Fase 1, prototipo sobre Ray

El curso tiene tres fases. **Este repositorio está en la Fase 1**: todo el
sistema funciona distribuido sobre actores de [Ray](https://www.ray.io/), con
cada nodo de servicio y cada dispositivo de usuario en su propio proceso. En la
Fase 2 Ray se sustituye por comunicación propia. La arquitectura mantiene Ray
dentro de unos pocos adaptadores, así que el resto del código no cambia.

**Lo que ya funciona**

- Cuentas protegidas con contraseña (Argon2id), contactos por enlace firmado y solicitudes de contacto.
- Entrega directa entre usuarios conectados, con caída automática a los buzones.
- Buzones replicados en 3 nodos con quórum 2 de 3. Cualquier nodo puede fallar sin que se pierdan mensajes.
- Recogida al reconectarse, confirmación a los nodos y supresión de duplicados.
- Orden FIFO con huecos ("mensaje todavía no disponible") e historial ordenado por Lamport.
- Un lanzador que levanta el clúster desde un archivo de configuración, y herramientas para tumbar y reiniciar nodos.
- Una interfaz de texto para cada usuario, y logs estructurados en JSON de cada proceso.

**Pendiente / límites conocidos**

- **Acuses de recibo por el buzón.** Un mensaje entregado directamente muestra ✓✓, pero uno que pasó por un buzón se queda en ✓ aunque ya se haya leído.
- **Borrado de datos caducados en los nodos.** Los mensajes caducados nunca se entregan, pero todavía no se borran del disco de los nodos.

## Cómo funciona

```
   dispositivo "alice"                   dispositivo "bob"
 ┌───────────────────┐                ┌───────────────────┐
 │ ClientApp + SQLite│── directo ────▶│ ClientApp + SQLite│   ambos conectados: directo a Bob
 └────────┬──────────┘                └─────────▲─────────┘
          │ depósito (2 de 3)                   │ recoge al conectarse
          ▼                                     │
 ┌──────────┐  ┌──────────┐  ┌──────────┐       │
 │  node-1  │  │  node-2  │  │  node-3  │ ──────┘   buzones + directorio de presencia
 └──────────┘  └──────────┘  └──────────┘           (solo ven sobres cifrados)
```

- **Los clientes** (uno por dispositivo) guardan las claves, el historial y la
  cola de salida. Cada mensaje se cifra para su receptor y lo firma su emisor
  antes de salir del dispositivo.
- **Los nodos de servicio** guardan dos cosas, replicadas con quórums: los
  **buzones** (sobres cifrados que esperan a usuarios desconectados) y el
  **directorio de presencia** (registros firmados del tipo "estoy conectado
  hasta…").
- Si el receptor está conectado, el mensaje va **directo**. Si no, se deposita
  en sus buzones y lo recoge al conectarse.

El código sigue una **arquitectura hexagonal**: el dominio y los casos de uso no
saben nada de Ray, SQLite ni PyNaCl. Esos son adaptadores detrás de puertos, y
una comprobación automática (import-linter) lo garantiza.

```
source/
  domain/         reglas: formato del sobre, orden, Lamport, contactos, quórums
  application/    casos de uso (identidad, enviar, recibir, contactos, entrega,
                  buzón, directorio, quórum) y los puertos que necesitan
  adapters/       memory (pruebas), system (SQLite, PyNaCl, reloj, actores de Ray), cli
  composition/    montaje: cliente, red de Ray, lanzador, fallos, entrada de la CLI
tests/            pruebas unitarias, de contrato, de integración (Ray) y de extremo a extremo
tools/logview.py  lee y junta los logs JSON de todos los procesos
config/           cluster.json: la configuración por defecto del sistema
docs/             documentación técnica
```

## Requisitos

- **Python 3.10, 3.11 o 3.12**
- **Linux o macOS** (en Windows, Ray funciona mejor a través de WSL)
- Unos 2 GB de RAM libres: Ray arranca varios procesos

## Instalación

```bash
git clone https://github.com/Bazan49/Distributed-Systems-Project.git
cd Distributed-Systems-Project

python3 -m venv .venv
source .venv/bin/activate          # hay que ejecutarlo en cada terminal nueva

pip install -e ".[dev]"            # la aplicación (Ray, PyNaCl) y las herramientas de desarrollo
```

## Cómo ponerlo en marcha, paso a paso

Usa una terminal para los nodos de servicio y otra por cada usuario, todas en
la carpeta del proyecto y con `source .venv/bin/activate` ejecutado antes.

### 1. Arrancar los nodos de servicio (terminal 1)

```bash
python -m source.composition.launcher
```

Espera a que aparezca `READY`. El lanzador arranca Ray en esta máquina y los 3
nodos de servicio descritos en
[config/cluster.json](config/cluster.json), la configuración por defecto.
Déjalo funcionando:
es la infraestructura (buzones y directorio).

### 2. Abrir una interfaz por usuario (terminales 2, 3…)

```bash
python -m source.composition.cli --device alice
```

```bash
python -m source.composition.cli --device bob
```

Sirve cualquier nombre (minúsculas, dígitos, `-` y `_`), y puede haber tantos
usuarios como terminales: no hace falta declarar a nadie antes. Cada comando se
une al clúster en marcha y enciende ese **dispositivo**, cuya cuenta e
historial se guardan en `data/cluster/devices/<nombre>.db`. Cerrar la interfaz apaga
el dispositivo: el usuario queda desconectado y sus mensajes esperan en los
buzones. Al abrirla de nuevo con el mismo nombre se vuelve a la misma cuenta.

### 3. (Opcional) Seguir los logs en vivo (otra terminal)

```bash
tail -f logs/*.jsonl | python -m tools.logview
```

### 4. Pararlo todo

Cierra las interfaces (`0. Salir`) y pulsa `Ctrl+C` en el lanzador: para los
nodos y Ray. Las cuentas, los contactos, los historiales y los mensajes
pendientes se quedan en `data/`. Al lanzarlo otra vez, todo está como lo
dejaste.

### El archivo de configuración

```json
{
  "namespace": "mensajeria",
  "data_dir": "../data/cluster",
  "nodes": ["node-1", "node-2", "node-3"],
  "seeds": ["node-1", "node-2"],
  "clients": []
}
```

| Clave | Significado |
|---|---|
| `nodes` | Nodos de servicio que se arrancan. Tres es el mínimo para el quórum 2 de 3: puede fallar cualquiera de ellos |
| `seeds` | Los nodos que se comunican a los clientes. Un cliente pide a cualquier semilla viva la lista completa de nodos |
| `namespace` | Espacio de nombres de Ray, donde cada actor se busca por su nombre. El lanzador y las interfaces deben usar el mismo |
| `data_dir` | Carpeta de las bases de datos de los nodos, relativa al archivo de configuración (aquí, `data/cluster/`) |
| `clients` | Dispositivos que se arrancan **junto con los nodos**, para experimentos automatizados (por ejemplo `[{"id": "alice"}]`). Siguen funcionando al cerrar su interfaz, como un móvil con la pantalla apagada, y guardan sus datos en el mismo sitio que cualquier otro dispositivo, así que un nombre es siempre la misma cuenta Para el uso normal se deja vacío y los dispositivos se arrancan desde la interfaz |
| `ray_address` | Opcional. Sin ella, el lanzador usa el clúster de Ray que ya esté en marcha en esta máquina o arranca uno. Con una dirección, se une a un clúster de otra máquina |

La interfaz lee **el mismo archivo**, así que los dos coinciden siempre en el
namespace, los nodos y dónde guarda sus datos cada dispositivo. Para probar
otros valores, cópialo a `config/cluster.local.json` (git lo ignora) y pásaselo
a los dos: `python -m source.composition.launcher config/cluster.local.json` y
`python -m source.composition.cli --device alice --config config/cluster.local.json`.

## Pruebas y controles de calidad

```bash
pytest                       # todo: unas 1700 pruebas, incluidas las de Ray (~2,5 min)
pytest -m "not integration"  # sin Ray: unos segundos
ruff check . && ruff format --check .   # estilo y formato
mypy .                       # tipos estrictos
lint-imports                 # reglas de arquitectura (capas, sin E/S en el dominio)
```

Los mismos controles se ejecutan en cada *push* con GitHub Actions, en Python
3.10, 3.11 y 3.12. La cobertura debe ser del 90 % o más.

```bash
pre-commit install           # una vez: pasa los controles antes de cada commit
```

## Logs

Cada proceso escribe eventos estructurados, un objeto JSON por línea, en
`logs/<proceso>.jsonl`: `node-1.jsonl`, `client-alice.jsonl`… Nunca se escriben
textos de mensajes, contraseñas, claves ni alias. Para seguir un mensaje por
todos los procesos:

```bash
python -m tools.logview logs/*.jsonl --msg <id del mensaje o un prefijo>
```
