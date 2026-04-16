"""
Download e gestão de PDFs no iPad (1TB de storage disponível).
"""

import os
import logging

import config
from utils import sanitize_filename
from pncp_client import PncpClient

log = logging.getLogger(__name__)


class PdfDownloader:
    def __init__(self, pncp: PncpClient):
        self.pncp = pncp
        self.base_dir = config.PDF_BASE_DIR
        os.makedirs(self.base_dir, exist_ok=True)

    def baixar_anexos(self, licitacao_id, cnpj, ano, seq):
        """Baixa todos os anexos PDF de uma licitação.
        Retorna lista de {seq_doc, titulo, nome_arquivo, url, caminho_local}.
        """
        arquivos = self.pncp.buscar_arquivos(cnpj, ano, seq)
        if not arquivos:
            return []

        resultados = []
        dir_licitacao = os.path.join(self.base_dir, licitacao_id)
        os.makedirs(dir_licitacao, exist_ok=True)

        for arq in arquivos:
            seq_doc = arq.get("sequencialDocumento", 0)
            titulo = arq.get("titulo", "")
            nome = arq.get("nomeArquivo", f"doc_{seq_doc}")
            url = arq.get("url", "")

            nome_safe = sanitize_filename(nome)
            caminho = os.path.join(dir_licitacao, f"{seq_doc}_{nome_safe}")

            # Pular se já existe
            if os.path.exists(caminho) and os.path.getsize(caminho) > 0:
                resultados.append({
                    "seq_doc": seq_doc,
                    "titulo": titulo,
                    "nome_arquivo": nome,
                    "url": url,
                    "caminho_local": caminho,
                    "ja_existia": True,
                })
                continue

            # Download
            pdf_bytes = self.pncp.download_arquivo(cnpj, ano, seq, seq_doc)
            if pdf_bytes:
                with open(caminho, "wb") as f:
                    f.write(pdf_bytes)
                log.info("PDF salvo: %s (%d bytes)", caminho, len(pdf_bytes))
                resultados.append({
                    "seq_doc": seq_doc,
                    "titulo": titulo,
                    "nome_arquivo": nome,
                    "url": url,
                    "caminho_local": caminho,
                    "ja_existia": False,
                })
            else:
                log.warning("Falha ao baixar anexo %d de %s", seq_doc, licitacao_id)

        return resultados

    def baixar_arquivo_unico(self, cnpj, ano, seq, seq_doc, licitacao_id):
        """Baixa um único arquivo (para modo OCR_BINARIOS)."""
        dir_licitacao = os.path.join(self.base_dir, licitacao_id)
        os.makedirs(dir_licitacao, exist_ok=True)
        caminho = os.path.join(dir_licitacao, f"{seq_doc}.pdf")

        if os.path.exists(caminho) and os.path.getsize(caminho) > 0:
            return caminho

        pdf_bytes = self.pncp.download_arquivo(cnpj, ano, seq, seq_doc)
        if pdf_bytes:
            with open(caminho, "wb") as f:
                f.write(pdf_bytes)
            return caminho
        return None

    def espaco_usado_mb(self):
        """Retorna espaço usado em MB pela pasta de anexos."""
        total = 0
        for dirpath, _, filenames in os.walk(self.base_dir):
            for f in filenames:
                total += os.path.getsize(os.path.join(dirpath, f))
        return total / (1024 * 1024)
