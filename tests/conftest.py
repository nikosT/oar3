# coding: utf-8

import os
import shutil
import tempfile
from codecs import open

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import Column, Integer, String, text

from oar.lib.database import EngineConnector, reflect_base

# from oar.lib import config, db
from oar.lib.globals import init_config, init_oar
from oar.lib.models import DeferredReflectionModel, Model

from . import DEFAULT_CONFIG


def op(engine):
    ctx = MigrationContext.configure(engine.connect())
    return Operations(ctx)


@pytest.fixture(scope="session", autouse=True)
def setup_config(request):
    config = init_config()

    config.update(DEFAULT_CONFIG.copy())
    tempdir = tempfile.mkdtemp()
    config["LOG_FILE"] = os.path.join(tempdir, "oar.log")

    db_type = os.environ.get("DB_TYPE", "memory")
    os.environ.setdefault("DB_TYPE", db_type)

    if db_type not in ("memory", "sqlite", "postgresql"):
        pytest.exit("Unsupported database '%s'" % db_type)

    if db_type == "sqlite":
        config["DB_BASE_FILE"] = os.path.join(tempdir, "db.sqlite")
        config["DB_TYPE"] = "sqlite"
    elif db_type == "memory":
        config["DB_TYPE"] = "sqlite"
        config["DB_BASE_FILE"] = ":memory:"
    else:
        config["DB_TYPE"] = "Pg"
        config["DB_PORT"] = "5432"
        config["DB_BASE_NAME"] = os.environ.get("POSTGRES_DB", "oar")
        config["DB_BASE_PASSWD"] = os.environ.get("POSTGRES_PASSWORD", "oar")
        config["DB_BASE_LOGIN"] = os.environ.get("POSTGRES_USER", "oar")
        config["DB_BASE_PASSWD_RO"] = os.environ.get("POSTGRES_PASSWORD", "oar_ro")
        config["DB_BASE_LOGIN_RO"] = os.environ.get("POSTGRES_USER_RO", "oar_ro")
        config["DB_HOSTNAME"] = os.environ.get("POSTGRES_HOST", "localhost")

    config, engine, _ = init_oar(config=config, no_reflect=True)

    # Model.metadata.drop_all(bind=engine)
    kw = {"nullable": True}

    Model.metadata.create_all(bind=engine)

    op(engine).add_column("resources", Column("core", Integer, **kw))
    op(engine).add_column("resources", Column("cpu", Integer, **kw))
    op(engine).add_column("resources", Column("host", String(255), **kw))
    op(engine).add_column("resources", Column("mem", Integer, **kw))

    # reflect_base(Model.metadata, DeferredReflectionModel, engine)
    DeferredReflectionModel.prepare(engine)
    # db.reflect(Model.metadata, bind=engine)
    yield config, True, engine

    # db.close()
    engine.dispose()

    shutil.rmtree(tempdir)
