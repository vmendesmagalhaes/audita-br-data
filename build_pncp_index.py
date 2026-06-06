#!/usr/bin/env python3
"""Constrói um índice SQLite dos contratos do PNCP, indexado por CNPJ do
FORNECEDOR.

Por quê
-------
O PNCP não permite consultar contratos por fornecedor (a API é organizada em
torno do órgão comprador). Mas o endpoint /contratos, por janela de data,
devolve o feed NACIONAL. Este script percorre esse feed e gera um índice
compacto por CNPJ do fornecedor — permitindo responder, no backend:
"em quantas prefeituras/órgãos diferentes esta empresa tem contrato?".
Mesmo padrão do índice de doações do TSE.

O PNCP throttla crawlers sustentados (após ~algumas dezenas de páginas a
sessão é estrangulada). Para contornar, a geração é PARALELIZADA por janelas
de data: cada "shard" baixa um pedaço pequeno do ano (poucas páginas, IP
limpo, sem disparar o throttle); depois os pedaços são unidos (merge).

Uso
---
  # um shard (pedaço i de n) da janela de `dias`:
  python build_pncp_index.py shard --shard 0 --shards 48 --dias 365 --out parte_00.sqlite

  # unir os pedaços num índice final:
  python build_pncp_index.py merge --out pncp_fornecedores.sqlite parte_*.sqlite

  # modo simples (janela inteira de uma vez; só viável sem throttle):
  python build_pncp_index.py full --dias 365 --out pncp_fornecedores.sqlite
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import random
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


def _fetch_pagina(url: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
    """Retry curto e com TEMPO LIMITADO por página. Best-effort: se a página
    não responde rápido, o chamador pula (perde aquela página) em vez de
    travar. Garante que o shard sempre termina em poucos minutos, mesmo sob
    throttle (pior caso ~3 x 20s = 60s/página)."""
    for tentativa in range(max_retries):
        proc = subprocess.run(
            ["curl", "-s", "--max-time", "20", url], capture_output=True
        )
        if proc.returncode == 0 and proc.stdout:
            try:
                return json.loads(proc.stdout)
            except json.JSONDecodeError:
                pass  # provável 429/HTML → retry
        time.sleep((1.0 * (2 ** tentativa)) + random.uniform(0, 1))
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


_COLS = ("fornecedor_doc", "fornecedor_nome", "orgao_cnpj", "orgao_nome",
         "municipio", "uf", "valor", "data", "controle")


def _criar_tabela(con: sqlite3.Connection) -> None:
    con.execute("PRAGMA journal_mode=OFF")
    con.execute("PRAGMA synchronous=OFF")
    con.execute(
        "CREATE TABLE IF NOT EXISTS contratos ("
        "fornecedor_doc TEXT NOT NULL, fornecedor_nome TEXT, orgao_cnpj TEXT, "
        "orgao_nome TEXT, municipio TEXT, uf TEXT, valor REAL, data TEXT, "
        "controle TEXT)"
    )
    con.commit()


def _janela(dias: int) -> Tuple[date, date]:
    hoje = date.today()
    return hoje - timedelta(days=min(dias, 365)), hoje


def _janela_shard(dias: int, shard: int, shards: int) -> Tuple[str, str]:
    """Sub-janela [ini, fim] do shard i de n, dentro dos últimos `dias`."""
    ini_total, fim_total = _janela(dias)
    total_dias = (fim_total - ini_total).days
    passo = total_dias / shards
    s_ini = ini_total + timedelta(days=round(passo * shard))
    s_fim = ini_total + timedelta(days=round(passo * (shard + 1)))
    if shard == shards - 1:
        s_fim = fim_total
    return s_ini.strftime("%Y%m%d"), s_fim.strftime("%Y%m%d")


DEADLINE_SHARD_S = 15 * 60   # tempo máximo por shard (trava contra timeout)


def crawl_janela(ini: str, fim: str, out_path: str) -> None:
    """Baixa os contratos da janela [ini, fim] para um SQLite parcial.
    Best-effort com TEMPO LIMITADO: páginas que não respondem são puladas e,
    se o shard passar de DEADLINE_SHARD_S, finaliza com o que tem. Garante que
    o shard sempre termina (sem timeout/cancelamento). O índice resultante é
    um limite inferior real do nº de órgãos — nunca superestima."""
    t0 = time.time()
    if os.path.exists(out_path):
        os.remove(out_path)
    con = sqlite3.connect(out_path)
    _criar_tabela(con)

    time.sleep(random.uniform(0, 8))  # stagger leve entre shards paralelos
    print(f"[crawl] janela {ini}..{fim}", flush=True)
    body1 = _fetch_pagina(_page_url(ini, fim, 1), max_retries=5)
    if not body1:
        print("ERRO: página 1 falhou.", flush=True)
        sys.exit(1)
    total_paginas = int(body1.get("totalPaginas") or 1)
    print(f"        {int(body1.get('totalRegistros') or 0):,} contratos em "
          f"{total_paginas} páginas", flush=True)

    total_rows = 0

    def inserir(body):
        nonlocal total_rows
        rows = _rows_da_pagina(body)
        con.executemany("INSERT INTO contratos VALUES (?,?,?,?,?,?,?,?,?)", rows)
        total_rows += len(rows)

    inserir(body1)
    puladas: List[int] = []
    for p in range(2, total_paginas + 1):
        if time.time() - t0 > DEADLINE_SHARD_S:
            puladas.extend(range(p, total_paginas + 1))
            break
        body = _fetch_pagina(_page_url(ini, fim, p))
        if body is None:
            puladas.append(p)
        else:
            inserir(body)
    # 2ª chance para as puladas, enquanto houver tempo
    ainda = []
    for p in puladas:
        if time.time() - t0 > DEADLINE_SHARD_S:
            ainda.append(p)
            continue
        body = _fetch_pagina(_page_url(ini, fim, p))
        if body is None:
            ainda.append(p)
        else:
            inserir(body)
    con.commit()
    con.close()
    msg = f"[crawl] pronto: {out_path} ({total_rows:,} contratos"
    if ainda:
        msg += f", {len(ainda)}/{total_paginas} páginas puladas (throttle)"
    print(msg + ")", flush=True)


def _finalizar(con: sqlite3.Connection, dias: int) -> int:
    """Dedup por numeroControlePNCP + índice por fornecedor + meta."""
    con.execute(
        "DELETE FROM contratos WHERE rowid NOT IN "
        "(SELECT MIN(rowid) FROM contratos GROUP BY controle) AND controle <> ''"
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_forn ON contratos(fornecedor_doc)")
    con.execute("CREATE TABLE IF NOT EXISTS meta (chave TEXT PRIMARY KEY, valor TEXT)")
    con.execute("DELETE FROM meta")
    n = con.execute("SELECT COUNT(*) FROM contratos").fetchone()[0]
    ini, fim = _janela(dias)
    con.executemany("INSERT INTO meta VALUES (?,?)", [
        ("dias", str(dias)),
        ("janela", f"{ini:%Y%m%d}-{fim:%Y%m%d}"),
        ("total_contratos", str(n)),
        ("gerado_em", datetime.now(timezone.utc).isoformat()),
        ("fonte", PNCP_CONTRATOS),
    ])
    con.commit()
    return n


def merge(out_path: str, partes: List[str], dias: int, minimo: int = 1) -> None:
    partes = [p for p in partes if os.path.exists(p) and os.path.getsize(p) > 0]
    if len(partes) < minimo:
        print(f"ERRO: só {len(partes)} pedaços válidos (mínimo {minimo}). Aborta.", flush=True)
        sys.exit(1)
    if os.path.exists(out_path):
        os.remove(out_path)
    con = sqlite3.connect(out_path)
    _criar_tabela(con)
    print(f"[merge] {len(partes)} partes", flush=True)
    for parte in partes:
        con.execute("ATTACH DATABASE ? AS p", (parte,))
        con.execute(f"INSERT INTO contratos SELECT {','.join(_COLS)} FROM p.contratos")
        con.commit()
        con.execute("DETACH DATABASE p")
        c = con.execute("SELECT COUNT(*) FROM contratos").fetchone()[0]
        print(f"        + {os.path.basename(parte)} -> {c:,} linhas", flush=True)
    n = _finalizar(con, dias)
    con.execute("VACUUM")
    con.close()
    mb = os.path.getsize(out_path) / 1e6
    print(f"[merge] pronto: {out_path} ({mb:.1f} MB, {n:,} contratos únicos)", flush=True)


def full(dias: int, out_path: str) -> None:
    ini, fim = _janela(dias)
    crawl_janela(ini.strftime("%Y%m%d"), fim.strftime("%Y%m%d"), out_path)
    con = sqlite3.connect(out_path)
    n = _finalizar(con, dias)
    con.execute("VACUUM")
    con.close()
    print(f"[full] {n:,} contratos únicos", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Índice SQLite de contratos do PNCP por fornecedor.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_shard = sub.add_parser("shard", help="Baixa um pedaço (janela de data) do ano")
    p_shard.add_argument("--shard", type=int, required=True)
    p_shard.add_argument("--shards", type=int, required=True)
    p_shard.add_argument("--dias", type=int, default=365)
    p_shard.add_argument("--out", required=True)

    p_merge = sub.add_parser("merge", help="Une pedaços num índice final")
    p_merge.add_argument("--out", required=True)
    p_merge.add_argument("--dias", type=int, default=365)
    p_merge.add_argument("--min", type=int, default=1, dest="minimo")
    p_merge.add_argument("partes", nargs="+")

    p_full = sub.add_parser("full", help="Janela inteira de uma vez")
    p_full.add_argument("--dias", type=int, default=365)
    p_full.add_argument("--out", required=True)

    args = ap.parse_args()
    if args.cmd == "shard":
        ini, fim = _janela_shard(args.dias, args.shard, args.shards)
        crawl_janela(ini, fim, args.out)
    elif args.cmd == "merge":
        partes = []
        for pat in args.partes:
            partes.extend(sorted(glob.glob(pat)) if "*" in pat else [pat])
        merge(args.out, partes, args.dias, minimo=args.minimo)
    elif args.cmd == "full":
        full(args.dias, args.out)


if __name__ == "__main__":
    main()
