"""
StillFlows iPad Coletor — Orquestrador principal.

Modos de operação:
  COLETAR        — Busca editais novos no PNCP, baixa PDFs, OCR, push
  OCR_BINARIOS   — Re-processa PDFs BINARIO do servidor com Apple Vision
  SYNC           — Envia itens pendentes que falharam em pushes anteriores

Uso no Pyto (iPad):
  import main
  main.coletar("2025-01-01", "2025-06-30")
  main.ocr_binarios()
  main.sync()
"""

import sys
import time
import logging
from datetime import datetime
from dataclasses import asdict

import config
from utils import gerar_id, gerar_link_pncp
from models import IngestLicitacaoDTO, IngestItemDTO, IngestResultadoDTO, IngestAnexoOcrDTO, IngestBatchDTO
from pncp_client import PncpClient
from stillflows_client import StillFlowsClient
from coordinator import Coordinator
from checkpoint import CheckpointManager
from pdf_downloader import PdfDownloader
from ocr_vision import ocr_pdf

# ── Logging ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(config.LOG_PATH, encoding="utf-8"),
    ]
)
log = logging.getLogger("main")


def coletar(data_inicio, data_fim, modalidade=None):
    """Modo COLETAR: busca editais, baixa PDFs, OCR, push ao servidor."""
    modalidade = modalidade or config.MODALIDADE_PREGAO
    log.info("═══ COLETA INICIADA [%s → %s] mod=%s ═══", data_inicio, data_fim, modalidade)

    pncp = PncpClient()
    server = StillFlowsClient()
    cp = CheckpointManager()
    coord = Coordinator(server, cp)
    downloader = PdfDownloader(pncp)

    # Verificar servidor
    if not server.is_online():
        log.error("Servidor StillFlows OFFLINE em %s — abortando", config.STILLFLOWS_URL)
        return

    # Retomar checkpoint se existir
    ck = cp.retomar_checkpoint("gap")
    if ck and ck["cursor_de"]:
        log.info("Retomando de checkpoint: cursor=%s", ck["cursor_de"])
        data_inicio = ck["cursor_de"]

    # Planejar chunks
    chunks, server_ids = coord.planejar_coleta(data_inicio, data_fim)

    total_novos = 0
    total_pulados = 0
    total_ocr = 0
    batch_buffer = []

    for chunk in chunks:
        log.info("── Chunk [%s → %s] ──", chunk["de"], chunk["ate"])
        pagina = 1

        while True:
            pubs = pncp.buscar_publicacoes(chunk["de"], chunk["ate"],
                                           modalidade=modalidade, pagina=pagina)
            if not pubs:
                break

            # A API retorna dict com chave "data" ou lista direta
            if isinstance(pubs, dict):
                lista = pubs.get("data", [])
            elif isinstance(pubs, list):
                lista = pubs
            else:
                break

            if not lista:
                break

            for pub in lista:
                try:
                    cnpj = pub.get("orgaoEntidade", {}).get("cnpj", "")
                    ano = pub.get("anoCompra", 0)
                    seq = pub.get("sequencialCompra", 0)
                    ncp = pub.get("numeroControlePNCP", "")

                    lid = gerar_id(cnpj, ano, seq)

                    if not coord.deve_coletar(lid, server_ids):
                        total_pulados += 1
                        continue

                    # Construir DTO de licitação
                    dto = IngestLicitacaoDTO(
                        id=lid,
                        numeroControlePncp=ncp,
                        orgaoCnpj=cnpj,
                        orgaoNome=pub.get("orgaoEntidade", {}).get("razaoSocial", ""),
                        orgaoUf=pub.get("unidadeOrgao", {}).get("ufSigla", ""),
                        orgaoMunicipio=pub.get("unidadeOrgao", {}).get("municipioNome", ""),
                        orgaoEsfera=pub.get("orgaoEntidade", {}).get("esferaId", ""),
                        ano=ano,
                        sequencial=seq,
                        modalidadeId=pub.get("modalidadeId"),
                        modalidadeNome=pub.get("modalidadeNome", ""),
                        situacaoId=pub.get("situacaoCompraId"),
                        situacao=pub.get("situacaoCompraNome", ""),
                        modoDisputa=pub.get("modoDisputaNome", ""),
                        objeto=pub.get("objetoCompra", ""),
                        objetoCompleto=pub.get("informacaoComplementar", ""),
                        valorEstimado=pub.get("valorTotalEstimado"),
                        valorHomologado=pub.get("valorTotalHomologado"),
                        dataPublicacao=pub.get("dataPublicacaoPncp", ""),
                        dataAbertura=pub.get("dataAberturaProposta", ""),
                        dataEncerramento=pub.get("dataEncerramentoProposta", ""),
                        linkPncp=gerar_link_pncp(ncp),
                        srp=pub.get("srp"),
                        contatoResponsavel=pub.get("nomeResponsavel", ""),
                        contatoEmail=pub.get("emailResponsavel", ""),
                    )

                    # Buscar itens
                    if cnpj:
                        itens_json = pncp.buscar_todos_itens(cnpj, ano, seq)
                        for ij in itens_json:
                            catmat = ij.get("catalogoCodigoItem", "") or ij.get("codigoCatalogo", "")
                            dto.itens.append(IngestItemDTO(
                                numeroItem=ij.get("numeroItem", 0),
                                descricao=ij.get("descricao", ""),
                                quantidade=ij.get("quantidade"),
                                unidade=ij.get("unidadeMedida", ""),
                                valorUnitarioEstimado=ij.get("valorUnitarioEstimado"),
                                valorTotalEstimado=ij.get("valorTotal"),
                                codigoCatalogo=catmat if catmat else None,
                            ))

                            # Buscar resultados se disponíveis
                            if ij.get("temResultado"):
                                ress = pncp.buscar_resultados(cnpj, ano, seq, ij.get("numeroItem", 0))
                                if ress and isinstance(ress, list):
                                    for r in ress:
                                        dto.resultados.append(IngestResultadoDTO(
                                            numeroItem=ij.get("numeroItem", 0),
                                            ordemClassificacao=r.get("ordemClassificacaoSrp", 1),
                                            niFornecedor=r.get("niFornecedor", ""),
                                            nomeFornecedor=r.get("nomeRazaoSocialFornecedor", ""),
                                            valorUnitarioHomologado=r.get("valorUnitarioHomologado"),
                                            valorTotalHomologado=r.get("valorTotalHomologado"),
                                            quantidadeHomologada=r.get("quantidadeHomologada"),
                                            porteFornecedorId=r.get("porteFornecedorId"),
                                            dataResultado=r.get("dataResultado", ""),
                                        ))
                            time.sleep(config.RATE_LIMIT_ITEMS_MS / 1000)

                        # Baixar PDFs e fazer OCR
                        anexos = downloader.baixar_anexos(lid, cnpj, ano, seq)
                        for anx in anexos:
                            texto = ocr_pdf(anx["caminho_local"])
                            if texto and len(texto) >= config.OCR_MIN_CHARS:
                                ocr_dto = IngestAnexoOcrDTO(
                                    licitacaoId=lid,
                                    orgaoCnpj=cnpj,
                                    ano=ano,
                                    sequencial=seq,
                                    sequencialDocumento=anx["seq_doc"],
                                    titulo=anx["titulo"],
                                    nomeArquivo=anx["nome_arquivo"],
                                    url=anx["url"],
                                    textoOcr=texto,
                                )
                                # Push OCR individualmente
                                result = server.push_ocr(ocr_dto.to_dict())
                                if result:
                                    cp.marcar_ocr_processado(lid, anx["seq_doc"])
                                    cp.marcar_ocr_enviado(lid, anx["seq_doc"])
                                    total_ocr += 1

                    # Adicionar ao batch
                    batch_buffer.append(dto)
                    cp.marcar_coletada(lid, ncp)
                    total_novos += 1

                    # Enviar batch quando cheio
                    if len(batch_buffer) >= config.BATCH_SIZE:
                        _enviar_batch(server, cp, batch_buffer)
                        batch_buffer = []

                except Exception as e:
                    log.error("Erro processando publicação: %s", e, exc_info=True)

                time.sleep(config.RATE_LIMIT_ITEMS_MS / 1000)

            if len(lista) < config.PAGE_SIZE:
                break
            pagina += 1
            time.sleep(config.RATE_LIMIT_PAGES_MS / 1000)

        # Salvar checkpoint após cada chunk
        cp.salvar_checkpoint("gap", chunk["ate"], data_inicio, data_fim, str(modalidade))

    # Flush do batch restante
    if batch_buffer:
        _enviar_batch(server, cp, batch_buffer)

    cp.finalizar_checkpoint("gap")
    cp.close()

    log.info("═══ COLETA FINALIZADA: %d novos, %d pulados, %d OCR ═══",
             total_novos, total_pulados, total_ocr)


def ocr_binarios():
    """Modo OCR_BINARIOS: re-processa PDFs que o servidor marcou como BINARIO."""
    log.info("═══ OCR BINÁRIOS INICIADO ═══")

    pncp = PncpClient()
    server = StillFlowsClient()
    cp = CheckpointManager()
    downloader = PdfDownloader(pncp)

    if not server.is_online():
        log.error("Servidor offline — abortando")
        return

    binarios = server.get_binarios()
    log.info("Total de BINÁRIOS para processar: %d", len(binarios))

    processados = 0
    erros = 0

    for b in binarios:
        try:
            lid = b["licitacaoId"]
            seq_doc = b["sequencialDocumento"]
            cnpj = b.get("orgaoCnpj", "")
            ano = b.get("ano", 0)
            seq = b.get("sequencial", 0)

            # Download do PDF
            caminho = downloader.baixar_arquivo_unico(cnpj, ano, seq, seq_doc, lid)
            if not caminho:
                log.warning("Não foi possível baixar %s/%d", lid, seq_doc)
                erros += 1
                continue

            # OCR via Apple Vision
            texto = ocr_pdf(caminho)
            if not texto or len(texto) < config.OCR_MIN_CHARS:
                log.info("OCR vazio ou curto para %s/%d (%d chars)", lid, seq_doc, len(texto) if texto else 0)
                continue

            # Push para servidor
            ocr_dto = IngestAnexoOcrDTO(
                licitacaoId=lid,
                orgaoCnpj=cnpj,
                ano=ano,
                sequencial=seq,
                sequencialDocumento=seq_doc,
                titulo=b.get("titulo", ""),
                nomeArquivo=b.get("nomeArquivo", ""),
                url=b.get("url", ""),
                textoOcr=texto,
            )
            result = server.push_ocr(ocr_dto.to_dict())
            if result:
                cp.marcar_ocr_processado(lid, seq_doc)
                cp.marcar_ocr_enviado(lid, seq_doc)
                processados += 1
                log.info("BINÁRIO → PROCESSADO: %s/%d (%d chars)", lid, seq_doc, len(texto))

        except Exception as e:
            log.error("Erro processando BINÁRIO %s: %s", b, e, exc_info=True)
            erros += 1

        time.sleep(0.5)  # Gentil com a API PNCP

    cp.close()
    log.info("═══ OCR BINÁRIOS FINALIZADO: %d processados, %d erros ═══", processados, erros)


def sync():
    """Modo SYNC: envia itens pendentes que falharam em pushes anteriores."""
    log.info("═══ SYNC INICIADO ═══")

    server = StillFlowsClient()
    cp = CheckpointManager()

    if not server.is_online():
        log.error("Servidor offline — abortando sync")
        return

    # Sync licitações pendentes
    pendentes = cp.pendentes_envio()
    log.info("Licitações pendentes de envio: %d", len(pendentes))
    # (implementação de retry ficaria aqui — requer cache local dos DTOs)

    # Sync OCR pendentes
    ocr_pendentes = cp.ocr_pendentes_envio()
    log.info("OCR pendentes de envio: %d", len(ocr_pendentes))

    cp.close()
    log.info("═══ SYNC FINALIZADO ═══")


def status():
    """Mostra status do servidor e do coletor local."""
    server = StillFlowsClient()
    cp = CheckpointManager()

    print(f"\n── Servidor ({config.STILLFLOWS_URL}) ──")
    if server.is_online():
        stats = server.get_stats()
        for k, v in (stats or {}).items():
            print(f"  {k}: {v}")
    else:
        print("  OFFLINE")

    print("\n── Coletor local ──")
    ck = cp.retomar_checkpoint()
    if ck:
        print(f"  Checkpoint ativo: cursor={ck['cursor_de']}, fim={ck['data_fim']}")
    else:
        print("  Sem checkpoint ativo")

    pendentes = cp.pendentes_envio()
    ocr_pend = cp.ocr_pendentes_envio()
    print(f"  Licitações pendentes envio: {len(pendentes)}")
    print(f"  OCR pendentes envio: {len(ocr_pend)}")

    downloader = PdfDownloader(PncpClient())
    print(f"  Storage PDFs: {downloader.espaco_usado_mb():.1f} MB")
    cp.close()


def _enviar_batch(server, cp, buffer):
    """Envia batch de licitações ao servidor."""
    batch = IngestBatchDTO(
        licitacoes=buffer,
        coletorId=config.COLETOR_ID,
    )
    result = server.push_batch(batch.to_dict())
    if result:
        log.info("Batch enviado: %s", result)
        for dto in buffer:
            cp.marcar_enviada(dto.id)
    else:
        log.warning("Falha ao enviar batch de %d licitações — ficam na fila offline", len(buffer))


# ── Entry point ──
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python main.py <modo> [args]")
        print("  coletar <de> <ate> [modalidade]")
        print("  ocr_binarios")
        print("  sync")
        print("  status")
        sys.exit(1)

    modo = sys.argv[1]
    if modo == "coletar":
        de = sys.argv[2] if len(sys.argv) > 2 else "2025-01-01"
        ate = sys.argv[3] if len(sys.argv) > 3 else datetime.now().strftime("%Y-%m-%d")
        mod = int(sys.argv[4]) if len(sys.argv) > 4 else None
        coletar(de, ate, mod)
    elif modo == "ocr_binarios":
        ocr_binarios()
    elif modo == "sync":
        sync()
    elif modo == "status":
        status()
    else:
        print(f"Modo desconhecido: {modo}")
