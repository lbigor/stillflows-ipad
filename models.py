"""
Dataclasses que espelham os DTOs Java de ingestão.
"""

from dataclasses import dataclass, field, asdict
from typing import List, Optional


def _strip_none(obj):
    """Remove recursivamente chaves com valor None de dicts/listas."""
    if isinstance(obj, dict):
        return {k: _strip_none(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, list):
        return [_strip_none(i) for i in obj]
    return obj


@dataclass
class IngestItemDTO:
    numeroItem: int = 0
    descricao: str = ""
    quantidade: Optional[float] = None
    unidade: Optional[str] = None
    valorUnitarioEstimado: Optional[float] = None
    valorTotalEstimado: Optional[float] = None
    codigoCatalogo: Optional[str] = None


@dataclass
class IngestResultadoDTO:
    numeroItem: int = 0
    ordemClassificacao: int = 1
    niFornecedor: Optional[str] = None
    nomeFornecedor: Optional[str] = None
    valorUnitarioHomologado: Optional[float] = None
    valorTotalHomologado: Optional[float] = None
    quantidadeHomologada: Optional[float] = None
    porteFornecedorId: Optional[int] = None
    dataResultado: Optional[str] = None


@dataclass
class IngestLicitacaoDTO:
    id: str = ""
    numeroControlePncp: Optional[str] = None
    orgaoCnpj: str = ""
    orgaoNome: str = ""
    orgaoUf: str = ""
    orgaoMunicipio: str = ""
    orgaoEsfera: str = ""
    ano: int = 0
    sequencial: int = 0
    numeroCompra: Optional[str] = None
    modalidadeId: Optional[int] = None
    modalidadeNome: Optional[str] = None
    situacaoId: Optional[int] = None
    situacao: Optional[str] = None
    modoDisputa: Optional[str] = None
    objeto: Optional[str] = None
    objetoCompleto: Optional[str] = None
    valorEstimado: Optional[float] = None
    valorHomologado: Optional[float] = None
    dataPublicacao: Optional[str] = None
    dataAbertura: Optional[str] = None
    dataEncerramento: Optional[str] = None
    linkPncp: Optional[str] = None
    linkSistemaOrigem: Optional[str] = None
    srp: Optional[bool] = None
    contatoResponsavel: Optional[str] = None
    contatoEmail: Optional[str] = None
    itens: List[IngestItemDTO] = field(default_factory=list)
    resultados: List[IngestResultadoDTO] = field(default_factory=list)

    def to_dict(self):
        return _strip_none(asdict(self))


@dataclass
class IngestAnexoOcrDTO:
    licitacaoId: str = ""
    orgaoCnpj: Optional[str] = None
    ano: Optional[int] = None
    sequencial: Optional[int] = None
    sequencialDocumento: int = 0
    titulo: Optional[str] = None
    nomeArquivo: Optional[str] = None
    url: Optional[str] = None
    textoOcr: str = ""
    statusOcr: str = "PROCESSADO"

    def to_dict(self):
        return _strip_none(asdict(self))


@dataclass
class IngestBatchDTO:
    licitacoes: List[IngestLicitacaoDTO] = field(default_factory=list)
    coletorId: str = "ipad-m4"

    def to_dict(self):
        return {
            "coletorId": self.coletorId,
            "licitacoes": [l.to_dict() for l in self.licitacoes]
        }
