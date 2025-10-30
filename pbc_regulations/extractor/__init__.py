"""Utilities for extracting policy document text artifacts."""

from . import text_pipeline
from .text_pipeline import (
    DocumentCandidate,
    EntryExtraction,
    EntryTextRecord,
    ExtractionAttempt,
    ProcessReport,
    extract_entry,
    process_state_data,
)

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "text_pipeline",
    "DocumentCandidate",
    "EntryExtraction",
    "EntryTextRecord",
    "ExtractionAttempt",
    "ProcessReport",
    "extract_entry",
    "process_state_data",
]
