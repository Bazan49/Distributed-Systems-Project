"""Basic test to verify Ray is working (Task 2.1)."""

import pytest
import ray


# 1. El Actor de prueba (Un mini-servidor que hace eco)
@ray.remote
class EchoActor:
    def echo(self, message: str) -> str:
        return f"Echo: {message}"


# 2. La Fixture (Arranca y apaga Ray automáticamente para toda la sesión de pruebas)
@pytest.fixture(scope="session", autouse=True)
def ray_cluster() -> None:
    """Start a local Ray cluster before the tests, shut it down after."""
    if not ray.is_initialized():
        # include_dashboard=False hace que arranque más rápido en tests
        ray.init(ignore_reinit_error=True, include_dashboard=False)

    yield  # Aquí es donde se ejecutan los tests

    ray.shutdown()


# 3. El Test (El criterio de "Listo Cuando")
@pytest.mark.integration
def test_ray_actor_echo() -> None:
    """Verify we can create an actor and receive its response."""
    # Creamos el actor en el clúster (no en nuestro proceso actual)
    actor = EchoActor.remote()  # type: ignore[attr-defined]

    # Le enviamos un mensaje (esto devuelve un ObjectRef, como un ticket)
    ref = actor.echo.remote("Hola Ray")

    # ray.get() espera a que el actor responda y nos da el valor real
    result = ray.get(ref)

    assert result == "Echo: Hola Ray"
