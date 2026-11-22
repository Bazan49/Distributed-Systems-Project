from pathlib import Path

import pytest

from source.adapters.system.mailbox_store import SQLiteMailboxStore
from source.application.ports import MailboxStorePort
from tests.contracts.mailbox_store_contract import MailboxStoreContract


class TestSqliteMailboxStore(MailboxStoreContract):
    """Runs the exact same contract tests, but against the real SQLite adapter."""

    @pytest.fixture
    def store(self, tmp_path: Path) -> MailboxStorePort:
        db_file = tmp_path / "mailbox_test.db"
        return SQLiteMailboxStore(db_file)
