import pytest

from mypr_mcp.protocol import check_compatibility, descriptor, runtime_info


def test_compatible_protocol_ignores_release_number():
    state = {**descriptor(), "version": "0.1.0", "generation": "existing"}
    assert check_compatibility(state) is state
    info = runtime_info(state, "0.9.0", include_instructions=True)
    assert info["update_pending"]
    assert info["generation"] == "existing"
    assert info["instructions"] == state["instructions"]
    assert not runtime_info(state, "0.1.0")["update_pending"]


def test_legacy_profile_does_not_advertise_restart():
    legacy = check_compatibility({"version": "0.9.0"})
    assert legacy["legacy"]
    assert "restart" not in legacy["capabilities"]
    assert "system" not in legacy["capabilities"]
    assert "unavailable" in legacy["instructions"]


@pytest.mark.parametrize(
    "state",
    [
        {"version": "0.8.0"},
        {"version": "99.0", "protocol_version": 2},
        {"version": "0.9.0", "protocol_version": True},
    ],
)
def test_unknown_protocol_does_not_silently_attach(state):
    with pytest.raises(RuntimeError, match="mypr-mcp restart"):
        check_compatibility(state)


def test_supported_protocol_requires_actual_instructions():
    with pytest.raises(RuntimeError, match="descriptor"):
        check_compatibility({"version": "0.9.0", "protocol_version": 1})
