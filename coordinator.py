"""
Coordenação entre iPad e servidor — decide o que coletar, evita duplicatas.
"""

import logging
from datetime import datetime, timedelta

import config
from checkpoint import CheckpointManager
from stillflows_client import StillFlowsClient

log = logging.getLogger(__name__)


class Coordinator:
    def __init__(self, server: StillFlowsClient, cp: CheckpointManager):
        self.server = server
        self.cp = cp

    def planejar_coleta(self, data_inicio, data_fim):
        """Retorna lista de chunks (de, ate) que ainda não foram coletados."""
        # 1. Verificar se servidor tem checkpoint ativo nesse range
        checkpoints = self.server.get_checkpoints()
        for ck in checkpoints:
            ck_inicio = ck.get("rangeInicio", "")
            ck_fim = ck.get("rangeFim", "")
            if ck_inicio and ck_fim:
                if ck_inicio <= data_fim and ck_fim >= data_inicio:
                    log.warning("Servidor tem checkpoint ativo em [%s, %s] — cuidado com sobreposição",
                               ck_inicio, ck_fim)

        # 2. Obter IDs já coletados no servidor
        server_ids = self.server.get_collected_ids(data_inicio, data_fim)
        log.info("Servidor já tem %d licitações em [%s, %s]",
                len(server_ids), data_inicio, data_fim)

        # 3. Dividir em chunks de CHUNK_DAYS dias
        chunks = []
        dt_inicio = datetime.strptime(data_inicio, "%Y-%m-%d")
        dt_fim = datetime.strptime(data_fim, "%Y-%m-%d")

        cursor = dt_inicio
        while cursor <= dt_fim:
            chunk_fim = min(cursor + timedelta(days=config.CHUNK_DAYS - 1), dt_fim)
            chunks.append({
                "de": cursor.strftime("%Y-%m-%d"),
                "ate": chunk_fim.strftime("%Y-%m-%d"),
            })
            cursor = chunk_fim + timedelta(days=1)

        log.info("Plano: %d chunks de %d dias em [%s, %s]",
                len(chunks), config.CHUNK_DAYS, data_inicio, data_fim)
        return chunks, server_ids

    def deve_coletar(self, licitacao_id, server_ids):
        """Verifica se uma licitação deve ser coletada (dedup local + servidor)."""
        if licitacao_id in server_ids:
            return False
        if self.cp.ja_coletada(licitacao_id):
            return False
        return True

    def planejar_ocr_binarios(self):
        """Obtém lista de anexos BINARIO do servidor para re-OCR."""
        binarios = self.server.get_binarios()
        log.info("Servidor tem %d anexos BINARIO para re-OCR", len(binarios))
        return binarios
