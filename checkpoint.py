"""
Checkpoint local via SQLite para retomada e fila offline.
"""

import sqlite3
import os
from datetime import datetime

import config


class CheckpointManager:
    def __init__(self, db_path=None):
        self.db_path = db_path or config.DB_PATH
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self._criar_tabelas()

    def _criar_tabelas(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS coleta_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tipo TEXT NOT NULL,
                data_inicio TEXT NOT NULL,
                data_fim TEXT NOT NULL,
                cursor_de TEXT,
                status TEXT DEFAULT 'ativo',
                modalidades TEXT,
                criado_em TEXT,
                atualizado_em TEXT
            );
            CREATE TABLE IF NOT EXISTS licitacoes_coletadas (
                id TEXT PRIMARY KEY,
                numero_controle_pncp TEXT,
                coletado_em TEXT,
                enviado INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS ocr_processados (
                licitacao_id TEXT,
                seq_doc INTEGER,
                status TEXT,
                enviado INTEGER DEFAULT 0,
                PRIMARY KEY (licitacao_id, seq_doc)
            );
        """)
        self.conn.commit()

    # ── Checkpoint de coleta ──

    def salvar_checkpoint(self, tipo, cursor_de, data_inicio, data_fim, modalidades=None):
        agora = datetime.now().isoformat()
        # Atualizar se existe ativo do mesmo tipo
        existing = self.conn.execute(
            "SELECT id FROM coleta_runs WHERE tipo=? AND status='ativo'", (tipo,)
        ).fetchone()
        if existing:
            self.conn.execute(
                "UPDATE coleta_runs SET cursor_de=?, atualizado_em=? WHERE id=?",
                (cursor_de, agora, existing[0])
            )
        else:
            self.conn.execute(
                "INSERT INTO coleta_runs (tipo, data_inicio, data_fim, cursor_de, status, modalidades, criado_em, atualizado_em) "
                "VALUES (?, ?, ?, ?, 'ativo', ?, ?, ?)",
                (tipo, data_inicio, data_fim, cursor_de, modalidades, agora, agora)
            )
        self.conn.commit()

    def retomar_checkpoint(self, tipo="gap"):
        row = self.conn.execute(
            "SELECT cursor_de, data_fim, modalidades FROM coleta_runs WHERE tipo=? AND status='ativo' ORDER BY atualizado_em DESC LIMIT 1",
            (tipo,)
        ).fetchone()
        if row:
            return {"cursor_de": row[0], "data_fim": row[1], "modalidades": row[2]}
        return None

    def finalizar_checkpoint(self, tipo="gap"):
        agora = datetime.now().isoformat()
        self.conn.execute(
            "UPDATE coleta_runs SET status='finalizado', atualizado_em=? WHERE tipo=? AND status='ativo'",
            (agora, tipo)
        )
        self.conn.commit()

    # ── Tracking de licitações ──

    def marcar_coletada(self, id_licitacao, numero_controle_pncp=None):
        agora = datetime.now().isoformat()
        self.conn.execute(
            "INSERT OR IGNORE INTO licitacoes_coletadas (id, numero_controle_pncp, coletado_em, enviado) VALUES (?, ?, ?, 0)",
            (id_licitacao, numero_controle_pncp, agora)
        )
        self.conn.commit()

    def ja_coletada(self, id_licitacao):
        row = self.conn.execute(
            "SELECT 1 FROM licitacoes_coletadas WHERE id=?", (id_licitacao,)
        ).fetchone()
        return row is not None

    def marcar_enviada(self, id_licitacao):
        self.conn.execute(
            "UPDATE licitacoes_coletadas SET enviado=1 WHERE id=?", (id_licitacao,)
        )
        self.conn.commit()

    def pendentes_envio(self):
        rows = self.conn.execute(
            "SELECT id FROM licitacoes_coletadas WHERE enviado=0"
        ).fetchall()
        return [r[0] for r in rows]

    # ── Tracking de OCR ──

    def marcar_ocr_processado(self, licitacao_id, seq_doc, status="PROCESSADO"):
        self.conn.execute(
            "INSERT OR REPLACE INTO ocr_processados (licitacao_id, seq_doc, status, enviado) VALUES (?, ?, ?, 0)",
            (licitacao_id, seq_doc, status)
        )
        self.conn.commit()

    def marcar_ocr_enviado(self, licitacao_id, seq_doc):
        self.conn.execute(
            "UPDATE ocr_processados SET enviado=1 WHERE licitacao_id=? AND seq_doc=?",
            (licitacao_id, seq_doc)
        )
        self.conn.commit()

    def ocr_pendentes_envio(self):
        rows = self.conn.execute(
            "SELECT licitacao_id, seq_doc FROM ocr_processados WHERE enviado=0"
        ).fetchall()
        return [{"licitacao_id": r[0], "seq_doc": r[1]} for r in rows]

    def close(self):
        self.conn.close()
