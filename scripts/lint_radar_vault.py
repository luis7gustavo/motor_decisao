"""Offline lint for the versioned Radar Licita execution notes.

This checks structure and references. It never opens operational data or
asserts that a cited snapshot, source hash, decision, or gate is valid.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import unquote


DEFAULT_NOTES = Path(__file__).resolve().parents[1] / "plans" / "radar-licita-v4"
CARD_ID = re.compile(r"P\d{2}")
MARKDOWN_LINK = re.compile(r"!?\[[^\]]+\]\((<?[^)]+>?)\)")
WIKI_LINK = re.compile(r"!?\[\[([^\]]+)\]\]")
HEX_REF = re.compile(r"`([0-9a-fA-F]{7,})`")
LONG_REF = re.compile(r"`([A-Za-z0-9]{41,})`")
DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
HUMAN_DATE = re.compile(r"\b\d{2}/\d{2}/\d{4}\b")
EXTERNAL = re.compile(r"^[a-z][a-z0-9+.-]*:", re.I)
LEGACY_UNSTRUCTURED = {
    "README.md", "CONTROLE_DE_PRS.md", "DECISOES.md",
    "INSTRUCOES_SESSOES.md", "PACOTE_REVISAO_P30.md",
    "PERMISSOES_E_FONTES.md", "PROPOSTA_V4_20261117.md",
    "MODELO_PR.md", "MODELO_REVISAO.md",
}


@dataclass(frozen=True, order=True)
class Issue:
    path: str
    line: int
    code: str
    message: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: {self.code}: {self.message}"


def _content_lines(text: str) -> list[tuple[int, str]]:
    """Ignore fenced examples and inline code when reading links."""
    result = []
    fence = None
    for number, line in enumerate(text.splitlines(), 1):
        marker = re.match(r"^\s*(`{3,}|~{3,})", line)
        if marker:
            token = marker.group(1)
            if fence is None:
                fence = token
            elif token[0] == fence[0] and len(token) >= len(fence):
                fence = None
            continue
        if fence is None:
            result.append((number, re.sub(r"`[^`]*`", "", line)))
    return result


def _frontmatter(text: str) -> tuple[dict[str, str], list[str]]:
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return {}, ["frontmatter ausente"]
    try:
        end = lines.index("---", 1)
    except ValueError:
        return {}, ["frontmatter sem fechamento"]
    fields: dict[str, str] = {}
    errors = []
    for line in lines[1:end]:
        match = re.fullmatch(r"([a-z_]+):\s*(.*?)\s*", line)
        if not match:
            errors.append(f"campo YAML inválido: {line}")
        elif match.group(1) in fields:
            errors.append(f"campo duplicado: {match.group(1)}")
        else:
            fields[match.group(1)] = match.group(2).strip('"\'')
    return fields, errors


def _slug(heading: str) -> str:
    heading = re.sub(r"`([^`]*)`", r"\1", heading).lower()
    heading = re.sub(r"[^\w\s-]", "", heading, flags=re.UNICODE)
    return re.sub(r"[\s-]+", "-", heading.strip()).strip("-")


def _anchors(text: str) -> set[str]:
    counts: dict[str, int] = {}
    anchors = set()
    for _, line in _content_lines(text):
        heading = re.match(r"^#{1,6}\s+(.+?)\s*#*\s*$", line)
        if heading:
            base = _slug(heading.group(1))
            count = counts.get(base, 0)
            anchors.add(f"{base}-{count}" if count else base)
            counts[base] = count + 1
    return anchors


def _link_target(base: Path, destination: str) -> tuple[Path, str]:
    destination = unquote(destination.strip("<>"))
    path, _, fragment = destination.partition("#")
    return (base.parent / path).resolve() if path else base, unquote(fragment)


def _check_decisions(text: str, rel: str) -> list[Issue]:
    issues = []
    seen = set()
    for number, line in _content_lines(text):
        if not line.startswith("| ") or line.startswith("| ID ") or line.startswith("| ---"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) != 5:
            issues.append(Issue(rel, number, "decision_columns", "decisão deve ter cinco colunas"))
            continue
        identifier, state, author, decision, effect = cells
        if identifier in seen:
            issues.append(Issue(rel, number, "decision_id", f"ID de decisão duplicado: {identifier}"))
        seen.add(identifier)
        if not all((identifier, state, author, decision, effect)):
            issues.append(Issue(rel, number, "decision_field", "decisão com campo vazio"))
        if any(word in state.lower() for word in ("confirmado", "aprovado")):
            if not HUMAN_DATE.search(author) or not re.search(r"\bLuis\b|\bCarlos\b", author):
                issues.append(Issue(rel, number, "decision_signer", "decisão confirmada exige autor e data"))
            if not re.search(r"conversa|resposta|commit|PR #|documento|aprovação|correção|pedido|mesma|“", author + " " + decision, re.I):
                issues.append(Issue(rel, number, "decision_reference", "decisão confirmada exige referência ao registro examinado"))
    return issues


def lint(notes: Path, *, check_git: bool = True) -> list[Issue]:
    notes = notes.resolve()
    if not notes.is_dir():
        return [Issue(str(notes), 0, "directory", "diretório de notas inexistente")]
    files = sorted(notes.rglob("*.md"))
    texts = {path: path.read_text(encoding="utf-8-sig") for path in files}
    issues: list[Issue] = []
    ids: dict[str, Path] = {}
    commit_cache: dict[str, bool] = {}

    def commit_exists(ref: str) -> bool:
        if ref not in commit_cache:
            try:
                result = subprocess.run(
                    ["git", "cat-file", "-e", f"{ref}^{{commit}}"],
                    cwd=notes, capture_output=True, check=False,
                )
            except FileNotFoundError as exc:
                raise RuntimeError("git não disponível; instale git ou use --no-git") from exc
            commit_cache[ref] = result.returncode == 0
        return commit_cache[ref]

    for path, body in texts.items():
        rel = path.relative_to(notes).as_posix()
        is_card = path.parent == notes / "prs"
        # Keep the known P30-era pages readable as-is, while requiring every
        # new note (including a newly added root-level page) to have metadata.
        is_record = (
            is_card or path.parent != notes or path.name not in LEGACY_UNSTRUCTURED
            or (body.startswith("---\n") and path.name not in {"MODELO_PR.md", "PROPOSTA_V4_20261117.md"})
        )
        if is_record:
            fields, errors = _frontmatter(body)
            issues.extend(Issue(rel, 1, "frontmatter", error) for error in errors)
            for required in ("id", "tipo", "estado", "atualizado_em"):
                if not fields.get(required):
                    issues.append(Issue(rel, 1, "frontmatter", f"campo obrigatório ausente: {required}"))
            identifier = fields.get("id")
            if identifier:
                if identifier in ids:
                    issues.append(Issue(rel, 2, "duplicate_id", f"{identifier} também em {ids[identifier].relative_to(notes).as_posix()}"))
                ids[identifier] = path
            if is_card and (identifier != path.stem or not CARD_ID.fullmatch(path.stem) or fields.get("tipo") != "pr"):
                issues.append(Issue(rel, 2, "card_id", "cartão deve ter ID PNN igual ao nome do arquivo e tipo pr"))
            updated = fields.get("atualizado_em", "")
            try:
                if not DATE.fullmatch(updated):
                    raise ValueError
                date.fromisoformat(updated)
            except ValueError:
                issues.append(Issue(rel, 1, "updated_date", "atualizado_em deve ser uma data ISO válida"))
            for key in ("base_commit", "repo_commit"):
                value = fields.get(key)
                if value and value != "null" and not re.fullmatch(r"[0-9a-fA-F]{7,40}", value):
                    issues.append(Issue(rel, 1, "commit_ref", f"{key} deve conter hash de commit"))
                elif check_git and value and value != "null" and not commit_exists(value):
                    issues.append(Issue(rel, 1, "commit_ref", f"commit não encontrado: {value}"))

        for number, line in _content_lines(body):
            for match in MARKDOWN_LINK.finditer(line):
                dest = match.group(1).strip().split(" ", 1)[0]
                if EXTERNAL.match(dest) or dest.startswith("//"):
                    continue
                target, fragment = _link_target(path, dest)
                if not target.exists():
                    issues.append(Issue(rel, number, "broken_link", f"arquivo não encontrado: {dest}"))
                elif fragment and target.is_file() and fragment not in _anchors(
                    texts[target] if target in texts else target.read_text(encoding="utf-8-sig")
                ):
                    issues.append(Issue(rel, number, "broken_anchor", f"âncora não encontrada: {dest}"))
            for match in WIKI_LINK.finditer(line):
                name = match.group(1).split("|", 1)[0].split("#", 1)[0]
                if not name:
                    continue
                candidate = (path.parent / (name if Path(name).suffix else name + ".md")).resolve()
                if not candidate.is_file() and not any(p.stem == name for p in files):
                    issues.append(Issue(rel, number, "broken_wikilink", f"nota não encontrada: {name}"))
        # A short commit reference can be resolved in this Git checkout. A
        # 64-character digest is a data hash and can only be checked for form.
        if check_git:
            for number, line in enumerate(body.splitlines(), 1):
                if "commit" not in line.lower() and "base" not in line.lower():
                    continue
                for match in HEX_REF.finditer(line):
                    ref = match.group(1)
                    if len(ref) > 40:
                        continue
                    if not commit_exists(ref):
                        issues.append(Issue(rel, number, "commit_ref", f"commit não encontrado: {ref}"))
        for number, line in enumerate(body.splitlines(), 1):
            for match in LONG_REF.finditer(line):
                digest = match.group(1)
                if not re.fullmatch(r"[0-9a-fA-F]{64}", digest):
                    issues.append(Issue(rel, number, "data_hash", "hash de dados deve ter 64 dígitos hexadecimais"))
        if path.name == "DECISOES.md":
            issues.extend(_check_decisions(body, rel))

    cards = {p.stem for p in files if p.parent == notes / "prs"}
    control = texts.get(notes / "CONTROLE_DE_PRS.md", "")
    listed = re.findall(r"\[P(\d{2})\]\(prs/P\1\.md\)", control)
    listed_ids = ["P" + item for item in listed]
    for card in sorted(cards - set(listed_ids)):
        issues.append(Issue("CONTROLE_DE_PRS.md", 1, "control_card", f"cartão ausente da fila: {card}"))
    for card in sorted(set(listed_ids) - cards):
        issues.append(Issue("CONTROLE_DE_PRS.md", 1, "control_card", f"cartão listado inexistente: {card}"))
    for card in sorted({item for item in listed_ids if listed_ids.count(item) > 1}):
        issues.append(Issue("CONTROLE_DE_PRS.md", 1, "control_card", f"cartão repetido na fila: {card}"))
    return sorted(issues)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("notes", nargs="?", type=Path, default=DEFAULT_NOTES)
    parser.add_argument("--no-git", action="store_true", help="pular resolução local de commits")
    args = parser.parse_args(argv)
    try:
        issues = lint(args.notes, check_git=not args.no_git)
    except RuntimeError as exc:
        print(f"erro: {exc}", file=sys.stderr)
        return 2
    for issue in issues:
        print(issue)
    if issues:
        print(f"{len(issues)} problema(s) em {args.notes}", file=sys.stderr)
        return 1
    print(f"OK: {args.notes}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
