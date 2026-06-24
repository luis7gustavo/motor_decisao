"""
Extrai produtos do catálogo PDF da Mega Mix e salva em JSON + CSV.
Uso: python scripts/extract_megamix_pdf.py <caminho_do_pdf>
"""
from __future__ import annotations

import csv
import json
import os
import re
import sys

import pdfplumber

COL_SPLIT = 300  # divisor horizontal entre coluna esquerda e direita (em pts)


def words_to_lines(words: list, col_x_min: float, col_x_max: float) -> list[str]:
    """Agrupa palavras de uma coluna em linhas pelo eixo vertical (top)."""
    col_words = [w for w in words if col_x_min <= w["x0"] < col_x_max]
    line_buckets: dict[int, list[str]] = {}
    for w in col_words:
        key = round(w["top"])
        line_buckets.setdefault(key, []).append(w["text"])
    return [" ".join(line_buckets[k]) for k in sorted(line_buckets)]


def parse_column_lines(lines: list[str]) -> list[dict]:
    """Extrai produtos de uma lista de linhas de uma coluna."""
    products = []
    i = 0
    while i < len(lines):
        line = lines[i]
        cod_match = re.match(r"C.d:(\d+)\s+Cod\.\s*de\s*Barra:(\S+)", line)
        if not cod_match:
            i += 1
            continue

        codigo = cod_match.group(1)
        barcode = cod_match.group(2)
        i += 1

        nome = None
        modelo = None
        preco = None
        unidade = None

        while i < len(lines):
            nl = lines[i]
            # Próximo produto — encerra bloco atual
            if re.match(r"C.d:\d+", nl):
                break
            # Pula linhas de cabeçalho/rodapé
            if re.match(r"(www\.|Telefone|Vig.ncia)", nl, re.IGNORECASE):
                i += 1
                continue
            # Observação: pula
            if re.match(r"Observa", nl, re.IGNORECASE):
                i += 1
                continue
            # Preço: ex. "R$2,99 UN:UN"
            price_match = re.search(r"R\x24\s*(\d+[.,]\d+)", nl)
            if price_match:
                preco_str = price_match.group(1).replace(",", ".")
                try:
                    preco = float(preco_str)
                except ValueError:
                    pass
                unit_match = re.search(r"UN:(\w+)", nl)
                unidade = unit_match.group(1) if unit_match else None
                i += 1
                continue
            # Unidade em linha separada
            if re.match(r"UN:", nl):
                unit_match = re.search(r"UN:(\w+)", nl)
                unidade = unit_match.group(1) if unit_match else None
                i += 1
                continue
            # Nome ou modelo: primeira linha não-especial é o nome, segunda é modelo
            candidate = nl.lstrip("*").strip()
            if not candidate:
                i += 1
                continue
            if nome is None:
                nome = candidate
            elif modelo is None:
                modelo = candidate
            i += 1

        if codigo and nome and preco is not None:
            products.append(
                {
                    "codigo": codigo,
                    "barcode": barcode,
                    "nome": nome,
                    "modelo": modelo,
                    "preco": preco,
                    "unidade": unidade,
                    "fonte": "megamix",
                }
            )

    return products


def extract_catalog(pdf_path: str) -> list[dict]:
    all_products: list[dict] = []
    seen: set[str] = set()

    with pdfplumber.open(pdf_path) as pdf:
        total = len(pdf.pages)
        print(f"Processando {total} páginas...", flush=True)
        for idx, page in enumerate(pdf.pages):
            try:
                words = page.extract_words()
                if not words:
                    continue
                left_lines = words_to_lines(words, 0, COL_SPLIT)
                right_lines = words_to_lines(words, COL_SPLIT, 9999)
                for lines in (left_lines, right_lines):
                    for p in parse_column_lines(lines):
                        if p["codigo"] not in seen:
                            seen.add(p["codigo"])
                            all_products.append(p)
            except Exception as exc:  # noqa: BLE001
                print(f"  Erro página {idx + 1}: {exc}", flush=True)

            if (idx + 1) % 30 == 0:
                print(f"  {idx + 1}/{total} páginas — {len(all_products)} produtos", flush=True)

    return all_products


def main() -> None:
    if len(sys.argv) < 2:
        pdf_path = r"C:\Users\luisg\Downloads\Catálogo Mega Mix 21.05.pdf"
    else:
        pdf_path = sys.argv[1]

    if not os.path.exists(pdf_path):
        print(f"Arquivo não encontrado: {pdf_path}")
        sys.exit(1)

    products = extract_catalog(pdf_path)
    print(f"\nTotal extraído: {len(products)} produtos únicos")

    if not products:
        print("Nenhum produto extraído. Verifique o PDF.")
        sys.exit(1)

    precos = [p["preco"] for p in products]
    print(f"Preço mín: R${min(precos):.2f} | máx: R${max(precos):.2f} | médio: R${sum(precos)/len(precos):.2f}")

    # Diretório de saída
    out_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data",
        "raw",
    )
    os.makedirs(out_dir, exist_ok=True)

    json_path = os.path.join(out_dir, "megamix_catalog_raw.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(products, f, ensure_ascii=False, indent=2)
    print(f"JSON salvo: {json_path}")

    csv_path = os.path.join(out_dir, "megamix_catalog_raw.csv")
    with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["codigo", "barcode", "nome", "modelo", "preco", "unidade", "fonte"])
        writer.writeheader()
        writer.writerows(products)
    print(f"CSV salvo:  {csv_path}")

    # Mostra amostra
    print("\n--- AMOSTRA (10 produtos) ---")
    for p in products[:10]:
        print(f"  [{p['codigo']}] {p['nome'][:50]:<50} R${p['preco']:>7.2f}  {p['modelo'] or ''}")


if __name__ == "__main__":
    main()
