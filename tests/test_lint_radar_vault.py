from pathlib import Path

from scripts.lint_radar_vault import lint


def _notes(tmp_path: Path) -> Path:
    notes = tmp_path / "notes"
    (notes / "prs").mkdir(parents=True)
    (notes / "prs" / "P32.md").write_text(
        "---\nid: P32\ntipo: pr\nestado: em_revisao\n"
        "atualizado_em: 2026-11-18\n---\n# Cartão\n"
        "[Controle](../CONTROLE_DE_PRS.md#fila)\n",
        encoding="utf-8",
    )
    (notes / "CONTROLE_DE_PRS.md").write_text(
        "# Controle\n## Fila\n[P32](prs/P32.md)\n", encoding="utf-8"
    )
    (notes / "DECISOES.md").write_text(
        "| ID | Estado | Autor e data | Decisão | Efeito |\n"
        "| --- | --- | --- | --- | --- |\n"
        "| EX-01 | Confirmado | Luis, 18/11/2026, conversa de aprovação | Método | Execução |\n",
        encoding="utf-8",
    )
    return notes


def test_valid_notes_pass_and_fenced_example_is_ignored(tmp_path: Path):
    notes = _notes(tmp_path)
    with (notes / "README.md").open("w", encoding="utf-8") as output:
        output.write("```md\n[Exemplo](inexistente.md)\n```\n")

    assert lint(notes, check_git=False) == []


def test_missing_link_and_anchor_are_reported(tmp_path: Path):
    notes = _notes(tmp_path)
    card = notes / "prs" / "P32.md"
    card.write_text(
        card.read_text(encoding="utf-8")
        + "[Falta](../inexistente.md) [Âncora](../CONTROLE_DE_PRS.md#ausente)\n",
        encoding="utf-8",
    )

    assert {issue.code for issue in lint(notes, check_git=False)} == {
        "broken_link", "broken_anchor"
    }


def test_duplicate_card_id_and_bad_date_are_reported(tmp_path: Path):
    notes = _notes(tmp_path)
    other = notes / "prs" / "P31.md"
    other.write_text(
        "---\nid: P32\ntipo: pr\nestado: planejado\n"
        "atualizado_em: 2026-13-18\n---\n# Outro\n",
        encoding="utf-8",
    )

    assert {issue.code for issue in lint(notes, check_git=False)} == {
        "duplicate_id", "card_id", "updated_date", "control_card"
    }


def test_confirmed_decision_requires_signer_date_and_reference(tmp_path: Path):
    notes = _notes(tmp_path)
    (notes / "DECISOES.md").write_text(
        "| ID | Estado | Autor e data | Decisão | Efeito |\n"
        "| --- | --- | --- | --- | --- |\n"
        "| EX-01 | Confirmado | agente | Aceito | Execução |\n",
        encoding="utf-8",
    )

    assert {issue.code for issue in lint(notes, check_git=False)} == {
        "decision_signer", "decision_reference"
    }


def test_bad_hash_and_commit_reference_are_reported(tmp_path: Path, monkeypatch):
    notes = _notes(tmp_path)
    monkeypatch.setattr(
        "scripts.lint_radar_vault.subprocess.run",
        lambda *args, **kwargs: type("Result", (), {"returncode": 1})(),
    )
    card = notes / "prs" / "P32.md"
    card.write_text(
        card.read_text(encoding="utf-8")
        + f"SHA-256: `{'a' * 63}`\nCommit `{'f' * 40}`\n",
        encoding="utf-8",
    )
    codes = {issue.code for issue in lint(notes, check_git=True)}

    assert "data_hash" in codes
    assert "commit_ref" in codes


def test_unresolved_wikilink_is_reported(tmp_path: Path):
    notes = _notes(tmp_path)
    (notes / "README.md").write_text("[[Nota ausente]]\n", encoding="utf-8")

    assert {issue.code for issue in lint(notes, check_git=False)} == {"broken_wikilink"}
