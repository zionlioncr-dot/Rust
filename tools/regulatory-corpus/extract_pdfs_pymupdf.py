#!/usr/bin/env python3
"""Extrae PDFs regulatorios con PyMuPDF en un directorio candidato aislado."""

import re
import sys
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parent
PDF_DIR = ROOT / "downloads"
OUTPUT = ROOT / "extracted-candidate-pymupdf"

DOCUMENTS = (
    "conassif-12-21-v07.pdf",
    "sugef-11-18-v11.pdf",
    "sugef-13-19-v6.pdf",
)

KNOWN_SPLITS = (
    "m onitoreo",
    "característica s",
    "operaci ones",
    "int entadas",
    "diligenc ia",
    "beneficiari os",
    "transacci ones",
    "or igen",
)


def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    failed = False

    for filename in DOCUMENTS:
        pdf_path = PDF_DIR / filename

        if not pdf_path.is_file():
            print("ERROR: PDF no encontrado:", pdf_path)
            failed = True
            continue

        document = fitz.open(pdf_path)
        page_blocks = []
        empty_pages = []
        errors = []

        for page_number, page in enumerate(document, start=1):
            try:
                text = page.get_text("text", sort=True) or ""
                if not text.strip():
                    empty_pages.append(page_number)
            except Exception as exc:
                text = ""
                errors.append((page_number, str(exc)))

            page_blocks.append(
                "===== PAGINA PDF %d =====\n%s\n"
                % (page_number, text.rstrip())
            )

        output_path = OUTPUT / (Path(filename).stem + ".txt")
        output_path.write_text(
            "\n".join(page_blocks) + "\n",
            encoding="utf-8",
        )

        content = output_path.read_text(encoding="utf-8")
        split_counts = {
            pattern: len(re.findall(pattern, content, re.IGNORECASE))
            for pattern in KNOWN_SPLITS
        }
        split_counts = {
            pattern: count
            for pattern, count in split_counts.items()
            if count
        }

        print("\nDOCUMENTO:", filename)
        print("Páginas:", len(document))
        print("Páginas vacías:", empty_pages)
        print("Páginas con errores:", len(errors))
        print("Caracteres:", len(content))
        print("Coincidencias de palabras partidas:", sum(split_counts.values()))
        print("Salida:", output_path)

        for pattern, count in split_counts.items():
            print(" ", repr(pattern), ":", count)

        for page_number, error in errors[:10]:
            print(" ERROR página", page_number, ":", error)

        if errors:
            failed = True

        document.close()

    print("\nExtracción candidata terminada.")
    print("Directorio:", OUTPUT)

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
