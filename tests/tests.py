import os
import sys

import pytest
from airflow.models import DagBag

sys.path.append(os.path.join(os.path.dirname(__file__), "../dags"))


@pytest.fixture(params=["../dags/"])
def dag_bag(request):
    return DagBag(dag_folder=request.param, include_examples=False)


def test_no_import_errors(dag_bag):
    assert not dag_bag.import_errors


def test_requires_tags(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert dag.tags


def test_requires_specific_tag(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        try:
            assert dag.tags.index("dag test demo") >= 0
        except ValueError:
            assert dag.tags.index("unit test demo") >= 0


def test_desc_len_greater_than_fifteen(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert len(dag.description) > 500


def test_owner_len_greater_than_five(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert len(dag.owner) > 5


def test_owner_not_airflow(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert str.lower(dag.owner) != "airflow"


def test_no_emails_on_retry(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert not dag.default_args["email_on_retry"]


def test_no_emails_on_failure(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert not dag.default_args["email_on_failure"]


def test_three_or_less_retries(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert dag.default_args["retries"] <= 3

