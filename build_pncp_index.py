#!/usr/bin/env python3
"""Constrói um índice SQLite dos contratos do PNCP, indexado por CNPJ do
FORNECEDOR.

Motivação
---------
O PNCP não permite consultar contratos por fornecedor: nenhum endpoint
filtra por CNPJ da empresa contratada (a API é organizada em torno do órgão
comprador). Porém, o endpoint /contratos, por janela de data, devolve o feed
NACIONAL inteiro. Este script percorre esse feed e gera um índice compacto
indexado por CNPJ do fornecedor — permitindo responder, no backend:
"em quantas prefeituras/órgãos diferentes esta empresa tem contrato?".

É o mesmo padrão usado para as doações do TSE: roda offline, gera um SQLite,
hospedado como release asset e consultado pelo backend (download 1x + cache).

Atualização periódica
----------------------
Contratos são contínuos (diferente do TSE, que é a cada eleição). Recomenda-se
regenerar mensal ou trimestralmente para manter o índice atualizado.

Uso
---
  python scripts/build_pncp_index.py --dias 365 --out pncp_fornecedores.sqlite
  python scripts/build_pncp_index.py --dias 365 --resume   # retoma de onde parou

O crawl é resumável: um checkpoint registra as páginas já inseridas, então
uma interrupção pode ser retomada sem refazer o trabalho.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

PNCP_CONTRATOS = "https://pncp.gov.br/api/consulta/v1/contratos"
TAM_PAGINA = 500


def _norm_doc(s: Optional[str]) -> str:
    return "".join(ch for ch in (str(s) if s is not None else "") if ch.isdigit())


def _fetch_pagina(url: str, max_retries: int = 4) -> Optional[Dict[str, Any]]:
    """Baixa uma página via curl (evita problemas de SSL/proxy do urllib).
    Retry com backoff em falha/429."""
    for tentativa in range(max_retries):
        proc = subprocess.run(
            ["curl", "-s", "--max-time", "60", url], capture_output=True
        )
        if proc.returncode == 0 and proc.stdout:
            try:
                return json.loads(proc.stdout)
            except json.JSONDecodeError:
                pass  # provável 429/HTML → retry
        time.sleep(1.5 * (2 ** tentativa))
    return None


def _page_url(ini: str, fim: str, pagina: int) -> str:
    return (
        f"{PNCP_CONTRATOS}?dataInicial={ini}&dataFinal={fim}"
        f"&pagina={pagina}&tamanhoPagina={TAM_PAGINA}"
    )


def _rows_da_pagina(body: Dict[str, Any]) -> List[Tuple]:
    rows: List[Tuple] = []
    for c in body.get("data", []) or []:
        doc = _norm_doc(c.get("niFornecedor"))
        if len(doc) not in (11, 14):
            continue
        orgao = c.get("orgaoEntidade") or {}
        unidade = c.get("unidadeOrgao") or {}
        rows.append((
            doc,
            (c.get("nomeRazaoSocialFornecedor") or "").strip(),
            _norm_doc(orgao.get("cnpj")),
            (orgao.get("razaoSocial") or "").strip(),
            (unidade.get("municipioNome") or "").strip(),
            (unidade.get("ufSigla") or "").strip(),
            float(c.get("valorInicial") or 0),
            (c.get("dataAssinatura") or "")[:10],
            c.get("numeroControlePNCP") or "",
        ))
    return rows


def _criar_db(con: sqlite3.Connection) -> None:
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS contratos (
            fornecedor_doc   TEXT NOT NULL,
            fornecedor_nome  TEXT,
            orgao_cnpj       TEXT,
            orgao_nome       TEXT,
            municipio        TEXT,
            uf               TEXT,
            valor            REAL,
            data             TEXT,
            controle         TEXT
        )
        """
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS _checkpoint (pagina INTEGER PRIMARY KEY)"
    )
    con.commit()


def build(dias: int, out_path: str, resume: bool) -> None:
    hoje = date.today()
    ini = (hoje - timedelta(days=min(dias, 365))).strftime("%Y%m%d")
    fim = hoje.strftime("%Y%m%d")

    if os.path.exists(out_path) and not resume:
        os.remove(out_path)
    con = sqlite3.connect(out_path)
    _criar_db(con)

    paginas_feitas = {
        r[0] for r in con.execute("SELECT pagina FROM _checkpoint").fetchall()
    }
    if paginas_feitas:
        print(f"[resume] {len(paginas_feitas)} páginas já inseridas; retomando.")

    print(f"[1/3] Descobrindo total de páginas (janela {ini}..{fim})")
    body1 = _fetch_pagina(_page_url(ini, fim, 1))
    if not body1:
        print("ERRO: falha ao buscar a página 1.")
        sys.exit(1)
    total_paginas = int(body1.get("totalPaginas") or 1)
    total_reg = int(body1.get("totalRegistros") or 0)
    print(f"      {total_reg:,} contratos em {total_paginas:,} páginas (tam={TAM_PAGINA})")

    pendentes = [p for p in range(1, total_paginas + 1) if p not in paginas_feitas]
    print(f"[2/3] Baixando {len(pendentes):,} páginas pendentes (sequencial)", flush=True)

    total_rows = con.execute("SELECT COUNT(*) FROM contratos").fetchone()[0]
    t0 = time.time()
    feitas = 0

    # Crawl SEQUENCIAL: o PNCP throttla a sessão após bursts concorrentes
    # sustentados. Sequencial mantém um ritmo estável sem ser bloqueado.
    # Páginas que falharem entram em `restantes` e são tentadas de novo numa
    # nova rodada (com cooldown) — nada é pulado/perdido.
    restantes = list(pendentes)
    rodada = 0
    while restantes:
        rodada += 1
        falhas: List[int] = []
        for p in restantes:
            body = body1 if p == 1 else _fetch_pagina(_page_url(ini, fim, p))
            if body is None:
                falhas.append(p)
                # Throttle provável: pausa curta para a janela de limite reabrir.
                time.sleep(2.0)
                continue
            rows = _rows_da_pagina(body)
            con.executemany(
                "INSERT INTO contratos VALUES (?,?,?,?,?,?,?,?,?)", rows
            )
            con.execute("INSERT OR IGNORE INTO _checkpoint VALUES (?)", (p,))
            total_rows += len(rows)
            feitas += 1
            if feitas % 50 == 0:
                con.commit()
                taxa = feitas / max(time.time() - t0, 1)
                falta = (len(pendentes) - feitas) / max(taxa, 0.01)
                print(f"      {feitas:,}/{len(pendentes):,} páginas | "
                      f"{total_rows:,} contratos | {taxa:.1f} pág/s | "
                      f"~{falta/60:.0f} min restantes", flush=True)
        con.commit()
        if falhas:
            print(f"      rodada {rodada}: {len(falhas)} páginas falharam; "
                  f"aguardando 30s e tentando de novo...", flush=True)
            time.sleep(30)
        restantes = falhas
        if rodada > 20:
            print(f"      desistindo de {len(restantes)} páginas após 20 rodadas.", flush=True)
            break

    print("[3/3] Deduplicando e criando índice por fornecedor")
    # Remove duplicatas por numeroControlePNCP (mantém 1)
    con.execute(
        """
        DELETE FROM contratos WHERE rowid NOT IN (
            SELECT MIN(rowid) FROM contratos GROUP BY controle
        ) AND controle <> ''
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_forn ON contratos(fornecedor_doc)")
    con.execute("CREATE TABLE IF NOT EXISTS meta (chave TEXT PRIMARY KEY, valor TEXT)")
    con.execute("DELETE FROM meta")
    n = con.execute("SELECT COUNT(*) FROM contratos").fetchone()[0]
    con.executemany("INSERT INTO meta VALUES (?,?)", [
        ("dias", str(dias)),
        ("janela", f"{ini}-{fim}"),
        ("total_contratos", str(n)),
        ("gerado_em", datetime.now(timezone.utc).isoformat()),
        ("fonte", PNCP_CONTRATOS),
    ])
    con.execute("DROP TABLE IF EXISTS _checkpoint")
    con.commit()
    con.execute("VACUUM")
    con.close()

    size_mb = os.path.getsize(out_path) / 1e6
    print(f"Pronto: {out_path} ({size_mb:.1f} MB, {n:,} contratos)")


def main() -> None:
    ap = argparse.ArgumentParser(description="Gera índice SQLite de contratos do PNCP por fornecedor.")
    ap.add_argument("--dias", type=int, default=365, help="Janela em dias (máx 365)")
    ap.add_argument("--out", default="pncp_fornecedores.sqlite")
    ap.add_argument("--resume", action="store_true", help="Retoma um crawl interrompido")
    args = ap.parse_args()
    build(args.dias, args.out, args.resume)


if __name__ == "__main__":
    main()
