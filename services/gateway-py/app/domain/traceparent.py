"""Geração do cabeçalho `traceparent` (W3C Trace Context) que viaja dentro do envelope."""

import secrets


def new_traceparent() -> str:
    """Gera um `traceparent` amostrado, com trace novo.

    Formato: `00-<trace_id 32 hex>-<span_id 16 hex>-01`. Provisório: quando o OpenTelemetry
    entrar (fase 5), o valor passa a vir do span ativo e este módulo some.
    """
    return f"00-{secrets.token_hex(16)}-{secrets.token_hex(8)}-01"
