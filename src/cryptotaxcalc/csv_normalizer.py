# csv_normalizer.py
"""
CSV parsing and normalization to our Transaction schema.

Responsibilities:
- Read uploaded CSV bytes safely.
- Normalize header names (case-insensitive).
- Validate required columns are present.
- Convert empty strings to None for optional fields.
- Validate each row using Pydantic (Transaction), returning:
  (valid_rows, errors) so the API can preview and/or persist.

Design choices:
- This module is "pure" (no DB calls). It converts raw bytes -> typed objects.
- Keep error messages helpful for users.
"""

import csv
from io import BytesIO, TextIOWrapper
from typing import List, Tuple, Dict, Any
from pydantic import ValidationError
from .schemas import Transaction

# Expected CSV columns (case-insensitive):
# timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
REQUIRED_COLUMNS = {"timestamp", "type", "base_asset", "base_amount"}
OPTIONAL_COLUMNS = {"quote_asset", "quote_amount", "fee_asset", "fee_amount", "exchange", "memo", "fair_value"}


def _normalize_headers(headers: List[str]) -> List[str]:
    """Lowercase and strip whitespace so headers are matched flexibly."""
    return [h.strip().lower() for h in headers]


def parse_csv(file_bytes: bytes, encoding: str = "utf-8") -> Tuple[List[Transaction], List[Dict[str, Any]]]:
    """
    Parse CSV bytes into a list of Transaction objects.
    Returns:
      valid_rows: list[Transaction]
      errors: list of {row_number, error, raw_row}

    Why bytes? FastAPI gives you raw file bytes; we wrap them in a text stream for csv.DictReader.
    """
    valid: List[Transaction] = []
    errors: List[Dict[str, Any]] = []

    # Wrap bytes with a text stream so csv can read it as lines of text.
    text_stream = TextIOWrapper(BytesIO(file_bytes), encoding=encoding, newline="")
    reader = csv.DictReader(text_stream)

    if reader.fieldnames is None:
        errors.append({"row_number": 0, "error": "CSV has no header", "raw_row": None})
        return valid, errors

    # Normalize header names and build a map original->normalized
    headers = _normalize_headers(reader.fieldnames)
    header_map = {orig: norm for orig, norm in zip(reader.fieldnames, headers)}

    # Check for required headers
    header_set = set(headers)
    missing = REQUIRED_COLUMNS - header_set
    if missing:
        errors.append({"row_number": 0, "error": f"Missing required columns: {sorted(missing)}", "raw_row": None})
        return valid, errors

    # Read and validate each row
    for i, row in enumerate(reader, start=2):  # start=2 because row 1 is the header
        # Build a normalized dict using lowercase keys
        normalized: Dict[str, Any] = {}
        for orig_key, value in row.items():
            key = header_map.get(orig_key, orig_key).lower()
            value = value.strip() if isinstance(value, str) else value  # be forgiving with whitespace
            normalized[key] = value

        # Convert empty strings ("") to None for OPTIONAL fields so Pydantic accepts them
        optional_string_fields = {"quote_asset", "fee_asset", "exchange", "memo"}
        optional_decimal_fields = {"quote_amount", "fee_amount", "fair_value"}

        for k in optional_string_fields:
            if k in normalized and normalized[k] == "":
                normalized[k] = None

        for k in optional_decimal_fields:
            if k in normalized and normalized[k] == "":
                normalized[k] = None

        try:
            tx = Transaction(**normalized)  # Pydantic enforces types (datetime, Decimal, etc.)
            valid.append(tx)
        except ValidationError as ve:
            # Collect all validation errors for this row (clear feedback in API)
            errors.append({"row_number": i, "error": ve.errors(), "raw_row": normalized})

    return valid, errors
