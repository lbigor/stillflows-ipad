"""
Cliente da API PNCP — mesmos endpoints que o Java (ColetorPncpService).
"""

import time
import logging
import requests

import config

log = logging.getLogger(__name__)


class PncpClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "StillFlows-iPad/1.0"
        })

    def buscar_publicacoes(self, data_inicial, data_final, modalidade=None, pagina=1, tamanho=None):
        """GET /contratacoes/publicacao — busca editais por período."""
        tamanho = tamanho or config.PAGE_SIZE
        params = {
            "dataInicial": data_inicial,
            "dataFinal": data_final,
            "pagina": pagina,
            "tamanhoPagina": tamanho,
        }
        if modalidade:
            params["codigoModalidadeContratacao"] = modalidade

        url = f"{config.PNCP_CONSULTA_URL}/contratacoes/publicacao"
        return self._get_com_retry(url, params)

    def buscar_itens(self, cnpj, ano, seq, pagina=1, tamanho=20):
        """GET /orgaos/{cnpj}/compras/{ano}/{seq}/itens"""
        url = f"{config.PNCP_API_URL}/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
        params = {"pagina": pagina, "tamanhoPagina": tamanho}
        return self._get_com_retry(url, params)

    def buscar_todos_itens(self, cnpj, ano, seq):
        """Busca todos os itens com paginação automática."""
        todos = []
        pagina = 1
        while True:
            itens = self.buscar_itens(cnpj, ano, seq, pagina=pagina)
            if itens is None:
                break
            if isinstance(itens, list):
                todos.extend(itens)
                if len(itens) < 20:
                    break
            else:
                break
            pagina += 1
            time.sleep(config.RATE_LIMIT_PAGES_MS / 1000)
        return todos

    def buscar_resultados(self, cnpj, ano, seq, numero_item):
        """GET /orgaos/{cnpj}/compras/{ano}/{seq}/itens/{n}/resultados"""
        url = f"{config.PNCP_API_URL}/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{numero_item}/resultados"
        return self._get_com_retry(url)

    def buscar_arquivos(self, cnpj, ano, seq):
        """GET /orgaos/{cnpj}/compras/{ano}/{seq}/arquivos"""
        url = f"{config.PNCP_API_URL}/orgaos/{cnpj}/compras/{ano}/{seq}/arquivos"
        return self._get_com_retry(url)

    def download_arquivo(self, cnpj, ano, seq, seq_doc):
        """GET /orgaos/{cnpj}/compras/{ano}/{seq}/arquivos/{seqDoc} — retorna bytes."""
        url = f"{config.PNCP_API_URL}/orgaos/{cnpj}/compras/{ano}/{seq}/arquivos/{seq_doc}"
        for tentativa in range(1, config.RETRY_MAX + 1):
            try:
                resp = self.session.get(url, timeout=60)
                if resp.status_code == 200:
                    return resp.content
                log.warning("Download %s status %d (tent %d/%d)",
                           url, resp.status_code, tentativa, config.RETRY_MAX)
            except Exception as e:
                log.warning("Download %s erro (tent %d/%d): %s",
                           url, tentativa, config.RETRY_MAX, e)
            time.sleep(config.RETRY_BACKOFF_BASE_S * tentativa)
        return None

    def _get_com_retry(self, url, params=None):
        """GET com retry e backoff."""
        for tentativa in range(1, config.RETRY_MAX + 1):
            try:
                resp = self.session.get(url, params=params, timeout=15)

                if resp.status_code == 429:
                    wait = config.RETRY_429_S * tentativa
                    log.warning("429 Rate Limit em %s — aguardando %ds", url, wait)
                    time.sleep(wait)
                    continue

                if resp.status_code == 200:
                    data = resp.json()
                    return data
                elif resp.status_code == 404:
                    return []

                log.warning("HTTP %d em %s (tent %d/%d)",
                           resp.status_code, url, tentativa, config.RETRY_MAX)
            except requests.exceptions.Timeout:
                wait = 30 * tentativa
                log.warning("Timeout em %s (tent %d/%d) — aguardando %ds",
                           url, tentativa, config.RETRY_MAX, wait)
                time.sleep(wait)
            except Exception as e:
                log.warning("Erro em %s (tent %d/%d): %s",
                           url, tentativa, config.RETRY_MAX, e)

            time.sleep(config.RETRY_BACKOFF_BASE_S * tentativa)
        return None
