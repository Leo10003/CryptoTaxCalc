# csv_normalizer.py
import csv
from io import BytesIO, TextIOWrapper
from typing import List, Tuple, Dict, Any
from pydantic import ValidationError
from schemas import Transaction

# Expected CSV columns (case-insensitive):
# timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
REQUIRED_COLUMNS = {"timestamp", "type", "base_asset", "base_amount"}
OPTIONAL_COLUMNS = {"quote_asset", "quote_amount", "fee_asset", "fee_amount", "exchange", "memo"}

def _normalize_headers(headers: List[str]) -> List[str]:
    return [h.strip().lower() for h in headers]

def parse_csv(file_bytes: bytes, encoding: str = "utf-8") -> Tuple[List[Transaction], List[Dict[str, Any]]]:
    """
    Parse CSV bytes into a list of Transaction objects.
    Returns (valid_rows, errors)
      - valid_rows: list[Transaction]
      - errors: list of {row_number, error, raw_row}
    """
    valid: List[Transaction] = []
    errors: List[Dict[str, Any]] = []

    # Wrap the uploaded bytes with a text wrapper so csv can read it
    text_stream = TextIOWrapper(BytesIO(file_bytes), encoding=encoding, newline="")
    reader = csv.DictReader(text_stream)

    if reader.fieldnames is None:
        errors.append({"row_number": 0, "error": "CSV has no header", "raw_row": None})
        return valid, errors

    headers = _normalize_headers(reader.fieldnames)
    # Map original header → normalized
    header_map = {orig: norm for orig, norm in zip(reader.fieldnames, headers)}

    # Validate required headers are present
    header_set = set(headers)
    missing = REQUIRED_COLUMNS - header_set
    if missing:
        errors.append({"row_number": 0, "error": f"Missing required columns: {sorted(missing)}", "raw_row": None})
        return valid, errors

    # Read and validate each row  ✅ this line and below are at same indentation level as 'if missing'
    for i, row in enumerate(reader, start=2):  # start=2 (row 1 is header)
        # Build a normalized dict using lowercase keys
        normalized: Dict[str, Any] = {}
        for orig_key, value in row.items():
            key = header_map.get(orig_key, orig_key).lower()
            # Trim whitespace for strings
            value = value.strip() if isinstance(value, str) else value
            normalized[key] = value

        # Convert empty strings ("") to None for OPTIONAL fields so Pydantic accepts them
        optional_string_fields = {"quote_asset", "fee_asset", "exchange", "memo"}
        optional_decimal_fields = {"quote_amount", "fee_amount"}

        for k in optional_string_fields:
            if k in normalized and normalized[k] == "":
                normalized[k] = None

        for k in optional_decimal_fields:
            if k in normalized and normalized[k] == "":
                normalized[k] = None

        try:
            tx = Transaction(**normalized)
            valid.append(tx)
        except ValidationError as ve:
            errors.append({"row_number": i, "error": ve.errors(), "raw_row": normalized})
    return valid, errors
    