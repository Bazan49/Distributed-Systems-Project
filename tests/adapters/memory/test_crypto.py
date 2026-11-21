import pytest

from source.adapters.memory.crypto import FakeCrypto
from source.application.ports.crypto import CryptoPort
from tests.contracts.crypto_contract import CryptoContract


class TestFakeCryptoContract(CryptoContract):
    @pytest.fixture
    def crypto(self) -> CryptoPort:
        return FakeCrypto()


# ----------------------------- behaviour specific to the fake ---------------
def test_it_satisfies_the_port() -> None:
    """The fake crypto adapter is a valid implementation of the CryptoPort."""
    assert isinstance(FakeCrypto(), CryptoPort)


def test_it_is_clearly_marked_as_insecure() -> None:
    """The fake crypto adapter is not suitable for production use."""
    assert FakeCrypto.INSECURE is True


def test_each_instance_produces_distinct_keys() -> None:
    """Two instances of the fake crypto adapter do not share state."""
    assert FakeCrypto().generate_key_pair() != FakeCrypto().generate_key_pair()


def test_a_single_instance_produces_distinct_keys_on_each_call() -> None:
    crypto = FakeCrypto()
    assert crypto.generate_key_pair() != crypto.generate_key_pair()


def test_keys_have_the_documented_sizes() -> None:
    """The public key is 32 bytes, the private key is 64 bytes (public + secret)."""
    pair = FakeCrypto().generate_key_pair()
    assert len(pair.public_key) == 32 and len(pair.private_key) == 64
    assert pair.private_key.startswith(pair.public_key)
