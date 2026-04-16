"""
Configuração do coletor iPad.
Ajuste STILLFLOWS_URL para o IP do MacBook na rede local.
"""

# ── Servidor StillFlows (MacBook) ──
STILLFLOWS_URL = "http://192.168.1.100:8081"
API_KEY = ""  # Preencher se autenticação for habilitada

# ── API PNCP ──
PNCP_CONSULTA_URL = "https://pncp.gov.br/api/consulta/v1"
PNCP_API_URL = "https://pncp.gov.br/api/pncp/v1"

# ── Rate limits (ms) — mesmos do Java ──
RATE_LIMIT_PAGES_MS = 200
RATE_LIMIT_ITEMS_MS = 80
RETRY_MAX = 5
RETRY_BACKOFF_BASE_S = 5
RETRY_429_S = 60

# ── Coleta ──
CHUNK_DAYS = 3
PAGE_SIZE = 50
BATCH_SIZE = 20  # Licitações por push ao servidor
MODALIDADE_PREGAO = 6

# ── Storage ──
PDF_BASE_DIR = "data/anexos"
DB_PATH = "db/checkpoint.db"
LOG_PATH = "logs/coleta.log"

# ── OCR ──
OCR_LANGUAGES = ["pt-BR", "en"]
OCR_MIN_CHARS = 50  # Mínimo para considerar PROCESSADO

# ── Coletor ──
COLETOR_ID = "ipad-m4"
