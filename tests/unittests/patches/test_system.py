import pytest

import sarc.patch.system as patch_mod
from sarc.patch.system import PatchSystem, declare_patch, load, register

# --- PatchSystem unit tests ---


def test_create_and_call():
    ps = PatchSystem()
    ps.create("ep")
    results = []
    ps.register("ep", results.append)
    ps.call("ep", (42,), {})
    assert results == [42]


def test_create_duplicate_raises():
    ps = PatchSystem()
    ps.create("ep")
    with pytest.raises(AssertionError):
        ps.create("ep")


def test_register_unknown_endpoint_raises():
    ps = PatchSystem()
    with pytest.raises(ValueError, match="non-existent endpoint"):
        ps.register("nope", lambda: None)


def test_call_unknown_endpoint_raises():
    ps = PatchSystem()
    with pytest.raises(ValueError, match="non-existent endpoint"):
        ps.call("nope", (), {})


def test_call_with_no_callbacks_is_noop():
    ps = PatchSystem()
    ps.create("ep")
    ps.call("ep", (), {})  # must not raise


def test_call_passes_args_and_kwargs():
    ps = PatchSystem()
    ps.create("ep")
    results = []
    ps.register("ep", lambda x, y=0: results.append((x, y)))
    ps.call("ep", (1,), {"y": 2})
    assert results == [(1, 2)]


# --- declare_patch and register (against a fresh system via monkeypatch) ---


@pytest.fixture
def fresh_system(monkeypatch):
    ps = PatchSystem()
    monkeypatch.setattr(patch_mod, "system", ps)
    return ps


def test_declare_patch_creates_endpoint(fresh_system):
    @declare_patch
    def my_ep(x: int) -> None:
        pass

    assert "my_ep" in fresh_system.callbacks


def test_declare_patch_calls_registered_handlers(fresh_system):
    @declare_patch
    def my_ep(x: int) -> None:
        pass

    results = []
    fresh_system.register("my_ep", results.append)

    my_ep(99)
    assert results == [99]


def test_register_as_decorator_with_function(fresh_system):
    fresh_system.create("ep")
    results = []

    @register
    def ep(x):
        results.append(x)

    fresh_system.call("ep", (7,), {})
    assert results == [7]


def test_register_as_decorator_with_name(fresh_system):
    fresh_system.create("ep")
    results = []

    @register("ep")
    def handler(x):
        results.append(x)

    fresh_system.call("ep", (5,), {})
    assert results == [5]


def test_register_direct_call(fresh_system):
    fresh_system.create("ep")
    results = []

    def handler(x):
        results.append(x)

    register("ep", handler)
    fresh_system.call("ep", (3,), {})
    assert results == [3]


def test_register_direct_call_returns_none(fresh_system):
    fresh_system.create("ep")

    result = register("ep", lambda: None)
    assert result is None


# --- load ---


def test_load_executes_python_files(tmp_path, fresh_system):
    fresh_system.create("ep")
    (tmp_path / "patch_a.py").write_text(
        "from sarc.patch.system import register\n@register('ep')\ndef h(x): pass\n"
    )

    load(tmp_path)
    assert len(fresh_system.callbacks["ep"]) == 1


def test_load_nonexistent_directory_is_noop(tmp_path, fresh_system):
    # rglob on a missing path yields nothing — must not raise
    load(tmp_path / "does_not_exist")


def test_load_sets_file_variable(tmp_path, fresh_system):
    """Exec'd patches receive __file__ set to their path."""
    fresh_system.create("ep")
    patch_file = tmp_path / "check_file.py"
    patch_file.write_text(
        "import sarc.patch.system as m\nm.system.register('ep', lambda: __file__)\n"
    )
    load(tmp_path)
    # callback was registered — __file__ was available in exec context
    fresh_system.call("ep", (), {})
