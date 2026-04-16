"""
Cliente REST para comunicação com o StillFlows (MacBook).
"""

import logging
import requests

import config

log = logging.getLogger(__name__)


class StillFlowsClient:
    def __init__(self, base_url=None):
        self.base_url = (base_url or config.STILLFLOWS_URL).rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "User-Agent": "StillFlows-iPad/1.0"
        })
        if config.API_KEY:
            self.session.headers["X-Coletor-Key"] = config.API_KEY

    # ── Push (ingestão) ──

    def push_licitacao(self, dto_dict):
        """POST /api/v1/ingest/licitacao"""
        return self._post("/api/v1/ingest/licitacao", dto_dict)

    def push_batch(self, batch_dict):
        """POST /api/v1/ingest/batch"""
        return self._post("/api/v1/ingest/batch", batch_dict)

    def push_ocr(self, ocr_dict):
        """POST /api/v1/ingest/ocr"""
        return self._post("/api/v1/ingest/ocr", ocr_dict)

    def push_ocr_batch(self, ocr_list):
        """POST /api/v1/ingest/ocr/batch"""
        return self._post("/api/v1/ingest/ocr/batch", ocr_list)

    # ── Coordenação ──

    def get_collected_ids(self, de, ate):
        """GET /api/v1/coord/ids — retorna set de IDs já coletados."""
        resp = self._get("/api/v1/coord/ids", params={"de": de, "ate": ate})
        if resp is not None:
            return set(resp)
        return set()

    def get_collected_count(self, de, ate):
        """GET /api/v1/coord/ids/count"""
        return self._get("/api/v1/coord/ids/count", params={"de": de, "ate": ate})

    def get_binarios(self):
        """GET /api/v1/coord/binarios — PDFs que precisam de OCR real."""
        return self._get("/api/v1/coord/binarios") or []

    def get_binarios_count(self):
        """GET /api/v1/coord/binarios/count"""
        return self._get("/api/v1/coord/binarios/count")

    def get_checkpoints(self):
        """GET /api/v1/coord/checkpoints — checkpoints ativos do servidor."""
        return self._get("/api/v1/coord/checkpoints") or []

    def get_stats(self):
        """GET /api/v1/coord/stats — status geral + health check."""
        return self._get("/api/v1/coord/stats")

    def is_online(self):
        """Verifica se o servidor está acessível."""
        try:
            resp = self.session.get(f"{self.base_url}/api/v1/coord/stats", timeout=3)
            return resp.status_code == 200
        except Exception:
            return False

    # ── Helpers ──

    def _post(self, path, data, retries=3):
        url = f"{self.base_url}{path}"
        for tentativa in range(1, retries + 1):
            try:
                resp = self.session.post(url, json=data, timeout=30)
                if resp.status_code == 200:
                    return resp.json()
                log.warning("POST %s → %d (tent %d/%d): %s",
                           path, resp.status_code, tentativa, retries, resp.text[:200])
            except requests.exceptions.ConnectionError:
                log.warning("Servidor offline — POST %s (tent %d/%d)", path, tentativa, retries)
            except Exception as e:
                log.warning("Erro POST %s (tent %d/%d): %s", path, tentativa, retries, e)
            if tentativa < retries:
                import time
                time.sleep(2 * tentativa)
        return None

    def _get(self, path, params=None):
        url = f"{self.base_url}{path}"
        try:
            resp = self.session.get(url, params=params, timeout=15)
            if resp.status_code == 200:
                return resp.json()
            log.warning("GET %s → %d: %s", path, resp.status_code, resp.text[:200])
        except Exception as e:
            log.warning("Erro GET %s: %s", path, e)
        return None
