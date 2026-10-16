#!/usr/bin/env python3
"""Extrae texto por página de los PDF regulatorios sin modificar el corpus actual."""

import argparse
import re
import sys
from pathlib import Path

from pypdf import PdfReader

ROOT = Path(__file__).resolve().parent
PDF_DIR = ROOT / "downloads"

DOCUMENTS = (
    "conassif-12-21-v07.pdf",
    "sugef-11-18-v11.pdf",
    "sugef-13-19-v6.pdf",
)

KNOWN_SPLITS = (
    "opera ciones",
    "transaccion es",
    "financie ro",
    "terror istas",
    "activid ades",
    "estupefac ientes",
    "financiam iento",
    "capita les",
    "super visión",
    "obligad os",
    "preven ción",
    "regula ción",
    "entida des",
    "transac ciones",
    "prolifera ción",
    "respon sabilidades",
    "artícu los",
)


def extract_page(page, mode):
    if mode == "layout":
        return page.extract_text(extraction_mode="layout") or ""
    return page.extract_text() or ""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=("plain", "layout", "both"),
        default="both",
        help="Método de extracción de pypdf",
    )
    parser.add_argument(
        "--output",
        default=str(ROOT / "extracted-candidate"),
        help="Directorio de salida; nunca sobrescribe los TXT actuales",
    )
    args = parser.parse_args()

    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    modes = ("plain", "layout") if args.mode == "both" else (args.mode,)

    for filename in DOCUMENTS:
        pdf_path = PDF_DIR / filename
        if not pdf_path.is_file():
            print(f"ERROR: falta el PDF: {pdf_path}", file=sys.stderr)
            return 2

        reader = PdfReader(str(pdf_path))
        print(f"\nPDF: {filename}")
        print(f"Páginas detectadas: {len(reader.pages)}")

        for mode in modes:
            page_blocks = []
            failed_pages = []

            for page_number, page in enumerate(reader.pages, start=1):
                try:
                    text = extract_page(page, mode)
                except Exception as exc:
                    failed_pages.append((page_number, str(exc)))
                    text = ""

                page_blocks.append(
                    f"===== PAGINA PDF {page_number} =====\n{text.rstrip()}\n"
                )

            document_text = "\n".join(page_blocks) + "\n"
            output_path = output / f"{Path(filename).stem}.{mode}.txt"
            output_path.write_text(document_text, encoding="utf-8")

            lower = document_text.lower()
            counts = {
                pattern: lower.count(pattern)
                for pattern in KNOWN_SPLITS
                if lower.count(pattern)
            }

            nonempty_pages = sum(
                bool(block.split("\n", 1)[1].strip())
                for block in page_blocks
            )

            print(f"\n  Modo: {mode}")
            print(f"  Archivo candidato: {output_path}")
            print(f"  Páginas con texto: {nonempty_pages}/{len(reader.pages)}")
            print(f"  Caracteres extraídos: {len(document_text)}")
            print(f"  Coincidencias de palabras partidas: {sum(counts.values())}")

            for pattern, count in counts.items():
                print(f"    {pattern}: {count}")

            if failed_pages:
                print(f"  ERRORES DE EXTRACCIÓN: {len(failed_pages)}")
                for page_number, error in failed_pages[:10]:
                    print(f"    Página {page_number}: {error}")

            # Muestra una página del cuerpo normativo para revisión manual.
            sample_page = {
                "conassif-12-21-v07.pdf": 22,
                "sugef-11-18-v11.pdf": 8,
                "sugef-13-19-v6.pdf": 10,
            }[filename]

            if sample_page <= len(reader.pages):
                sample = page_blocks[sample_page - 1]
                print(f"\n  Muestra de la página PDF {sample_page}:")
                lines = [
                    line.strip()
                    for line in sample.splitlines()[1:]
                    if line.strip()
                ]
                for line in lines[:8]:
                    print(f"    {line[:180]}")

    print("\nExtracción terminada. Los TXT originales y los manifiestos no se modificaron.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
