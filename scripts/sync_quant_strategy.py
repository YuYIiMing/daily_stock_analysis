#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""One-click quant strategy data refresh utility."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.storage import DatabaseManager
from src.services.quant_data_service import QuantDataService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh quant strategy snapshots and feature tables.")
    parser.add_argument("--as-of-date", type=str, default=None, help="Signal date in YYYY-MM-DD format.")
    parser.add_argument("--history-days", type=int, default=130, help="Lookback days for backfill and features.")
    parser.add_argument(
        "--codes",
        type=str,
        default="",
        help="Comma-separated stock codes. Empty means using the default full-market main-board pool.",
    )
    parser.add_argument(
        "--no-ranked-boards",
        action="store_true",
        help="Disable ranked concept boards fetch.",
    )
    parser.add_argument("--ranking-size", type=int, default=80, help="Concept board ranking size.")
    parser.add_argument(
        "--concept-commit-batch-size",
        type=int,
        default=20,
        help="Commit batch size for concept board history sync.",
    )
    parser.add_argument(
        "--concept-retry-attempts",
        type=int,
        default=2,
        help="Retry attempts for each concept board history fetch.",
    )
    parser.add_argument(
        "--feature-batch-size",
        type=int,
        default=200,
        help="Batch size used when rebuilding stock features.",
    )
    parser.add_argument(
        "--latest-feature-only",
        action="store_true",
        help="Only rebuild quant features for the signal date to reduce memory usage.",
    )
    return parser.parse_args()


def parse_codes(raw_codes: str) -> Optional[List[str]]:
    if not raw_codes.strip():
        return None
    return [code.strip() for code in raw_codes.split(",") if code.strip()]


def main() -> None:
    args = parse_args()
    db = DatabaseManager.get_instance()
    service = QuantDataService(db_manager=db)
    summary = service.refresh_quant_dataset(
        stock_codes=parse_codes(args.codes),
        as_of_date=args.as_of_date,
        history_days=args.history_days,
        include_ranked_boards=not args.no_ranked_boards,
        ranking_size=args.ranking_size,
        concept_commit_batch_size=args.concept_commit_batch_size,
        concept_retry_attempts=args.concept_retry_attempts,
        feature_batch_size=args.feature_batch_size,
        latest_feature_only=args.latest_feature_only,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
