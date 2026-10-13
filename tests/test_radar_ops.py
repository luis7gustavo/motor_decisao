"""RT-014/015; TST-046/070: refuse unsafe targets before mutation."""
import pytest

from scripts.radar_ops import dump_tables, validate_target, validate_revision_path


@pytest.mark.parametrize("target", ["motor_decisao", "test", "rehearsal_existing", "x;drop", "", "rehearsal_20261013_abcdef012345"])
def test_rt015_unowned_target_is_rejected(target):
    with pytest.raises(ValueError):
        validate_target(target, "motor_decisao", {})


def test_rt015_principal_rejected_even_when_registered():
    target = "rehearsal_20261013_abcdef012345"
    with pytest.raises(ValueError):
        validate_target(target, target, {target: 42})


def test_rt015_only_created_temporary_target_is_accepted():
    target = "rehearsal_20261013_abcdef012345"
    validate_target(target, "motor_decisao", {target: 42})


@pytest.mark.parametrize("target", ["head", "base", "-1", "old", "origin"])
def test_rt014_rejects_symbolic_legacy_or_empty_paths(target):
    with pytest.raises(ValueError):
        validate_revision_path("origin", target, {"new": "origin", "origin": "old", "old": None})


def test_rt014_accepts_only_descendants_of_origin():
    assert validate_revision_path("origin", "new", {"new": "origin", "origin": None}) == ["new"]


def test_rt014_rejects_merge_and_divergence():
    with pytest.raises(ValueError):
        validate_revision_path("origin", "new", {"new": ("origin", "other")})


def test_rt014_dump_inventory_excludes_data_and_attach_entries():
    toc = "\n".join([
        "1; 1259 10 TABLE public alembic_version motor",
        "2; 0 10 TABLE DATA public alembic_version motor",
        "3; 1259 11 TABLE bronze synthetic motor",
        "4; 0 11 TABLE ATTACH bronze synthetic motor",
    ])
    assert dump_tables(toc) == {"public.alembic_version", "bronze.synthetic"}
