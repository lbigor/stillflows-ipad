"""
Utilidades — incluindo geração de ID idêntica ao Java.

CRÍTICO: gerar_id() DEVE produzir o mesmo hash que
ColetorPncpService.extrairId() no Java.
"""

import re


def java_string_hashcode(s: str) -> int:
    """Replica exatamente o String.hashCode() do Java.
    Algoritmo: h = s[0]*31^(n-1) + s[1]*31^(n-2) + ... + s[n-1]
    com aritmética de inteiro 32-bit signed.
    """
    h = 0
    for ch in s:
        h = ((h * 31) + ord(ch)) & 0xFFFFFFFF
    # Converter para signed 32-bit
    if h >= 0x80000000:
        h -= 0x100000000
    return h


def gerar_id(cnpj: str, ano: int, sequencial: int) -> str:
    """Gera o ID de licitação idêntico ao Java:
    String raw = cnpj + "-" + ano + "-" + seq;
    return String.format("%016x", raw.hashCode() & 0xFFFFFFFFL).substring(0, 16);
    """
    raw = f"{cnpj}-{ano}-{sequencial}"
    h = java_string_hashcode(raw)
    # Java faz: raw.hashCode() & 0xFFFFFFFFL (converte para unsigned long)
    unsigned = h & 0xFFFFFFFF
    return format(unsigned, '016x')[:16]


def sanitize_filename(name: str) -> str:
    """Remove caracteres especiais de nomes de arquivo (mesma lógica do Java)."""
    return re.sub(r'[^a-zA-Z0-9._\-]', '_', name)


def gerar_link_pncp(numero_controle_pncp: str) -> str:
    """Gera o link para o edital no PNCP."""
    return f"https://pncp.gov.br/app/editais/{numero_controle_pncp}"
