"""Honor the architecture's basetemp ini key, absent from pytest's built-in ini API."""

from pathlib import Path

import pytest


def pytest_addoption(parser):
    parser.addini("basetemp", "Repository-local test temporary directory", default=".pytest_tmp")


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config):
    if config.option.basetemp is None:
        config.option.basetemp = str(config.rootpath / config.getini("basetemp"))


@pytest.fixture(autouse=True)
def verify_repository_local_temp_directory(tmp_path, pytestconfig):
    expected = (pytestconfig.rootpath / pytestconfig.getini("basetemp")).resolve()
    assert Path(tmp_path).resolve().is_relative_to(expected)
