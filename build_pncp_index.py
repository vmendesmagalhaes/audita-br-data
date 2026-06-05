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


def crawl_janela(ini: str, fim: str, out_path: str) -> None:
    """Baixa todos os contratos da janela [ini, fim] para um SQLite parcial.
    Sequencial e resiliente: páginas que falham são retentadas em rodadas
    com cooldown — nada é pulado."""
    if os.path.exists(out_path):
        os.remove(out_path)
    con = sqlite3.connect(out_path)
    _criar_tabela(con)

    print(f"[crawl] janela {ini}..{fim}", flush=True)
    body1 = _fetch_pagina(_page_url(ini, fim, 1))
    if not body1:
        print("ERRO: página 1 falhou.", flush=True)
        sys.exit(1)
    total_paginas = int(body1.get("totalPaginas") or 1)
    print(f"        {int(body1.get('totalRegistros') or 0):,} contratos em "
          f"{total_paginas} páginas", flush=True)

    restantes = list(range(1, total_paginas + 1))
    total_rows = 0
    rodada = 0
    while restantes:
        rodada += 1
        falhas: List[int] = []
        for p in restantes:
            body = body1 if p == 1 else _fetch_pagina(_page_url(ini, fim, p))
            if body is None:
                falhas.append(p)
                time.sleep(2.0)
                continue
            rows = _rows_da_pagina(body)
            con.executemany(
                "INSERT INTO contratos VALUES (?,?,?,?,?,?,?,?,?)", rows
            )
            total_rows += len(rows)
        con.commit()
        if falhas:
            print(f"        rodada {rodada}: {len(falhas)} falhas; cooldown 30s",
                  flush=True)
            time.sleep(30)
        restantes = falhas
        if rodada > 15:
            print(f"        desistindo de {len(restantes)} páginas", flush=True)
            break
    con.close()
    print(f"[crawl] pronto: {out_path} ({total_rows:,} contratos)", flush=True)


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


def merge(out_path: str, partes: List[str], dias: int) -> None:
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
        merge(args.out, partes, args.dias)
    elif args.cmd == "full":
        full(args.dias, args.out)


if __name__ == "__main__":
    main()
