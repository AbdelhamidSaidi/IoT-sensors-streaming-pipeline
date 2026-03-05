"""Medallion package init for the Kafka mini project.

This file turns `medallion` into a regular Python package so imports
like `from medallion.silver.silver import get_connection` work reliably
when running tools like Streamlit that may change import semantics.
"""

__all__ = ["gold", "silver"]
