#!/usr/bin/env python3
from pathlib import Path
import json
import re
import sys
import uuid
from collections import Counter

ROOT = Path("tools/regulatory-corpus")
EXTRACTED = ROOT / "extracted"
OUT = ROOT / "manifest"
NAMESPACE = uuid.UUID("6f3e18b7-a5e0-4cc5-a14a-94c7b43e1b10")

SOURCES = [
    {
        "file": "conassif-12-21-v07.txt",
        "code": "CONASSIF 12-21",
        "version": "v07",
        "start": 22,
        "end": 68,
        "expected": list(range(1, 62)),
        "url": "https://www.sugef.fi.cr/normativa/normativa_transversal/documentos/CONASSIF%2012-21%20%28v07%2010%20de%20noviembre%20de%202022%29.pdf",
        "cut_before": "disposiciones derogatorias",
    },
    {
        "file": "sugef-11-18-v11.txt",
        "code": "SUGEF 11-18",
        "version": "v11",
        "start": 8,
        "end": 33,
        "expected": list(range(1, 26)),
        "url": "https://www.sugef.fi.cr/normativa/normativa_vigente/SUGEF%2011-18%20%28v11%2023%20de%20octubre%20de%202023%29.pdf",
    },
    {
        "file": "sugef-13-19-v6.txt",
        "code": "SUGEF 13-19",
        "version": "v6",
        "start": 10,
        "end": 30,
        "expected": list(range(1, 44)),
        "url": "https://www.sugef.fi.cr/normativa/normativa_vigente/SUGEF%2013-19%20%28v6%2024%20de%20mayo%202024%29.pdf",
    },
]

PAGE_RE = re.compile(r"^===== PAGINA PDF (\d+) =====$", re.I)
ARTICLE_RE = re.compile(
    r"^\s*(?:(?:Uso\s+Interno)\s+)?"
    r"(?:\[[^\]]+\]\s*)?"
    r"(?:Acuerdo\s+CONASSIF\s+12-21\s+)?"
    r"Art[ií]culo\s+(\d+)\s*[.)]\s*(.*?)\s*$",
    re.I,
)

def clean_line(line, code):
    line = line.rstrip()
    if not line.strip() or re.fullmatch(r"\s*\d+\s*", line):
        return ""
    line = re.sub(r"^\s*Uso\s+Interno\s+", "", line, flags=re.I)
    line = re.sub(
        r"^\s*Acuerdo\s+(?:Sugef\s+11-18|CONASSIF\s+12-21)\s*",
        "", line, flags=re.I
    )
    return line.strip()

def extract_pages(source):
    path = EXTRACTED / source["file"]
    if not path.is_file():
        raise RuntimeError("No existe el archivo extraído: %s" % path)

    pages = []
    page_no = None
    page_lines = []

    def flush():
        if page_no is not None and source["start"] <= page_no <= source["end"]:
            for raw in page_lines:
                pages.append((page_no, clean_line(raw, source["code"])))

    for raw in path.read_text(encoding="utf-8").splitlines():
        match = PAGE_RE.match(raw.strip())
        if match:
            flush()
            page_no = int(match.group(1))
            page_lines = []
        elif page_no is not None:
            page_lines.append(raw)
    flush()
    return pages

def build(source):
    rows = extract_pages(source)
    headings = []

    for idx, (page, line) in enumerate(rows):
        match = ARTICLE_RE.match(line)
        if match:
            headings.append({
                "index": idx,
                "page": page,
                "number": int(match.group(1)),
                "title": re.sub(r"\s+", " ", match.group(2)).strip(),
                "line": line,
            })

    if not headings:
        raise RuntimeError("%s: no se encontraron artículos" % source["code"])

    records = []
    excluded = []

    for pos, heading in enumerate(headings):
        start = heading["index"]
        end = headings[pos + 1]["index"] if pos + 1 < len(headings) else len(rows)
        chunk = rows[start:end]

        # El artículo 61 de CONASSIF continúa en la página 68.
        # Cortar antes de la derogatoria y no incorporarla al artículo.
        if heading["number"] == 61 and source.get("cut_before"):
            for i, (_, line) in enumerate(chunk):
                if line.strip().lower().startswith(source["cut_before"]):
                    chunk = chunk[:i]
                    break

        title = heading["title"]
        # La marca [Eliminado] puede aparecer en la línea siguiente
        # al encabezado debido a los saltos de línea del PDF extraído.
        heading_block = "\\n".join(line for _, line in chunk[:4])
        if re.search(r"\[\s*Eliminado\s*\]", heading_block, re.I):
            excluded.append({
                "article": heading["number"],
                "page": heading["page"],
                "title": title,
            })
            continue

        content = "\n".join(line for _, line in chunk if line.strip()).strip()
        if not content:
            raise RuntimeError(
                "%s artículo %d: contenido vacío" %
                (source["code"], heading["number"])
            )

        content_pages = [page for page, line in chunk if line.strip()]
        page_start = min(content_pages) if content_pages else heading["page"]
        page_end = max(content_pages) if content_pages else heading["page"]

        stable_key = "%s|%s|%d|%d|%s" % (
            source["code"], source["version"], heading["number"],
            page_start, content
        )

        records.append({
            "id": str(uuid.uuid5(NAMESPACE, stable_key)),
            "source": source["url"],
            "content": content,
            "metadata": {
                "document_type": "regulation",
                "jurisdiction": "CR",
                "verified": False,
                "verification_status": "pending_legal_review",
                "regulation_code": source["code"],
                "article": str(heading["number"]),
                "title": title,
                "document_version": source["version"],
                "effective_from": None,
                "effective_to": None,
                "source_url": source["url"],
                "source_file": source["file"],
                "page_start": page_start,
                "page_end": page_end,
                "extraction_method": "pdf_text_by_page_v2",
            },
            "embedding": None,
        })

    counts = Counter(int(r["metadata"]["article"]) for r in records)
    duplicates = sorted(n for n, count in counts.items() if count > 1)
    observed = set(counts)
    expected = set(source["expected"])
    missing = sorted(expected - observed)
    unexpected = sorted(observed - expected)

    if duplicates or missing or unexpected:
        raise RuntimeError(
            "%s: validación fallida: duplicados=%s faltantes=%s inesperados=%s; "
            "encabezados detectados=%s; eliminados=%s"
            % (
                source["code"], duplicates, missing, unexpected,
                [h["number"] for h in headings], excluded
            )
        )

    if len(records) != len(source["expected"]):
        raise RuntimeError("%s: cantidad de artículos inesperada" % source["code"])

    for record in records:
        meta = record["metadata"]
        if meta["verified"] is not False:
            raise RuntimeError("Documento marcado como verificado inesperadamente")
        if meta["effective_from"] is not None or meta["effective_to"] is not None:
            raise RuntimeError("Se asignó una vigencia no validada")
        if not meta["source_url"].startswith("https://"):
            raise RuntimeError("La fuente no utiliza HTTPS")
        if record["embedding"] is not None:
            raise RuntimeError("No se esperaban embeddings en esta fase")

    return records, excluded

def write_jsonl(path, records):
    with path.open("w", encoding="utf-8") as stream:
        for record in records:
            stream.write(json.dumps(
                record, ensure_ascii=False, sort_keys=True
            ) + "\n")

def main():
    OUT.mkdir(parents=True, exist_ok=True)
    all_records = []
    total_excluded = 0

    for source in SOURCES:
        records, excluded = build(source)
        output = OUT / (Path(source["file"]).stem + ".jsonl")
        write_jsonl(output, records)
        all_records.extend(records)
        total_excluded += len(excluded)

        print("\n%s (%s)" % (source["code"], source["version"]))
        print("  Artículos conservados: %d" % len(records))
        print("  Duplicados: ninguno")
        print("  Artículos faltantes: ninguno")
        print("  Artículos eliminados excluidos: %s" % (
            excluded if excluded else "ninguno"
        ))
        print("  Archivo: %s" % output)

    ids = [record["id"] for record in all_records]
    if len(ids) != len(set(ids)):
        raise RuntimeError("Hay UUID duplicados en el manifiesto combinado")

    combined = OUT / "regulatory-corpus.jsonl"
    write_jsonl(combined, all_records)

    print("\nTOTAL: %d fragmentos" % len(all_records))
    print("UUID duplicados: ninguno")
    print("Artículos eliminados excluidos: %d" % total_excluded)
    print("Embeddings: no generados")
    print("Inserciones PostgreSQL: ninguna")
    print("verified=true: ninguno")
    print("Vigencias legales: pendientes de validación")
    print("Manifiesto combinado: %s" % combined)

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("ERROR: %s" % exc, file=sys.stderr)
        sys.exit(1)
