# -*- coding: utf-8 -*-
import pytest
from flask import url_for

from oar.kao.meta_sched import meta_schedule
from oar.lib import db
from oar.lib.job_handling import insert_job

# TODO test PAGINATION
# nodes / resource


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for i in range(15):
            db["Resource"].create(network_address="localhost")

        db["Queue"].create(name="default")
        yield


# TODO: Test is not sufficient at all
@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_jobs_get_all(client):
    insert_job(res=[(60, [("resource_id=4", "")])], properties="")
    insert_job(res=[(60, [("resource_id=4", "")])], properties="")

    meta_schedule("internal")

    res = client.get(url_for("gantt_predictions.index"))
    print(res.json, len(res.json["items"]))
    assert len(res.json["items"]) == 2
