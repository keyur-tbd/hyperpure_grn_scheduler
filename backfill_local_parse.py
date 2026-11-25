"""One-off backfill (2026-11-26): re-read every LlamaExtract-loaded Hyperpure GRN from its PDF and replace the rows
where LlamaExtract got it wrong.

Before bdf6c9b (2026-10-21) every GRN went through LlamaExtract, which dropped lines, doubled lines and misread
quantities (27HYD-02539: '51' read as '5'), so claims showed false shorts. For every file still without
raw_data.parsed_by and a GRN date on or after --since:
  * download the PDF from Drive, parse it with hyperpure_pdf, check it against the printed total;
  * same line count, quantity and amount as stored -> left alone;
  * the parse adds up to the printed total and differs -> the stored rows are replaced by the parsed lines
    (new rows inserted first, then the old ids deleted; raw_data.parsed_by = 'local text parser (hyperpure fix
    2026-11-25)', llama_rows_replaced); the old rows go to backfill_backup.jsonl (uploaded as a run artifact);
  * the parse does not add up -> left alone and listed.
No LlamaExtract call is made.

    python backfill_local_parse.py [--since 2026-03-25] [--apply] [--limit N] [--budget-minutes 200]

Env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY; token.json (Drive) in the working directory.
"""
import argparse
import hashlib
import io
import json
import os
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from supabase import create_client

import hyperpure_pdf
from supabase_sink import normalize_supabase_url

TABLE = 'hyperpure_grn'
TAG = 'local text parser (hyperpure fix 2026-11-25)'
COLS = ('id,drive_file_id,source_file,supplier,po_number,vendor_invoice_number,shipping_address,grn_date,grn_qty,'
        'amount,row_hash,raw_data')


def num(v):
    try:
        return float(str(v).replace(',', ''))
    except (TypeError, ValueError):
        return 0.0


def stored_rows(sb, since):
    """Rows still loaded by LlamaExtract, GRN date >= since, paged in id order (unordered paging skips rows)."""
    out, start = [], 0
    while True:
        res = (sb.table(TABLE).select(COLS).is_('raw_data->>parsed_by', 'null').gte('grn_date', since)
               .order('id').range(start, start + 999).execute())
        out += res.data or []
        if len(res.data or []) < 1000:
            return out
        start += 1000


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--since', default='2026-03-25')
    ap.add_argument('--apply', action='store_true')
    ap.add_argument('--limit', type=int)
    ap.add_argument('--budget-minutes', type=float, default=200)
    a = ap.parse_args()
    t0 = time.time()

    sb = create_client(normalize_supabase_url(os.environ['SUPABASE_URL']), os.environ['SUPABASE_SERVICE_ROLE_KEY'])
    creds = Credentials.from_authorized_user_file('token.json')
    if not creds.valid:
        creds.refresh(Request())

    by_file = defaultdict(list)
    for r in stored_rows(sb, a.since):
        by_file[r['drive_file_id']].append(r)
    files = sorted(by_file, key=lambda f: max(str(r['grn_date']) for r in by_file[f]), reverse=True)
    if a.limit:
        files = files[:a.limit]
    print(f'{len(files)} LlamaExtract-loaded GRN file(s) since {a.since}', flush=True)

    def download(fid):
        for attempt in range(4):
            try:
                d = build('drive', 'v3', credentials=creds, cache_discovery=False)
                buf = io.BytesIO()
                dl = MediaIoBaseDownload(buf, d.files().get_media(fileId=fid))
                done = False
                while not done:
                    _, done = dl.next_chunk()
                path = f'/tmp/hp_{fid}.pdf' if os.name != 'nt' else os.path.join(os.environ.get('TEMP', '.'), f'hp_{fid}.pdf')
                with open(path, 'wb') as fh:
                    fh.write(buf.getvalue())
                return path
            except Exception as exc:  # noqa: BLE001 -- retried, then reported
                err = exc
                time.sleep(2 * (attempt + 1))
        print(f'  download failed {fid}: {err}', flush=True)
        return None

    cnt, changed, unreconciled = Counter(), [], []
    backup = open('backfill_backup.jsonl', 'w', encoding='utf-8')
    with ThreadPoolExecutor(8) as pool:
        for i, (fid, path) in enumerate(zip(files, pool.map(download, files)), 1):
            if time.time() - t0 > a.budget_minutes * 60:
                cnt['not_reached_budget'] += len(files) - i + 1
                print('run budget spent; the rest stays for the next run', flush=True)
                break
            if i % 250 == 0:
                print(i, dict(cnt), flush=True)
            rows = by_file[fid]
            if not path:
                cnt['download_failed'] += 1
                continue
            try:
                parsed = hyperpure_pdf.parse(path)
            except Exception as exc:  # noqa: BLE001
                cnt['parse_error'] += 1
                unreconciled.append((rows[0]['source_file'], f'parse error {exc}'))
                continue
            finally:
                try:
                    os.remove(path)
                except OSError:
                    pass
            items, total = parsed['items'], parsed['printed_total']
            issue = hyperpure_pdf.check(items, total)
            s_q = sum(num(r['grn_qty']) for r in rows)
            s_a = sum(num(r['amount']) for r in rows)
            p_q = sum(num(it['grn_qty']) for it in items)
            p_a = hyperpure_pdf.amount_sum(items)
            if len(items) == len(rows) and abs(p_q - s_q) < 0.01 and abs(p_a - s_a) < 1:
                cnt['same'] += 1
                continue
            if issue:
                cnt['differs_unreconciled'] += 1
                unreconciled.append((rows[0]['source_file'], issue))
                continue
            cnt['differs'] += 1
            h, hdr = rows[0], parsed['header']
            base = {'po_number': hdr.get('po_number') or h['po_number'],
                    'vendor_invoice_number': hdr.get('vendor_invoice_number') or h['vendor_invoice_number'],
                    'shipping_address': hdr.get('shipping_address') or h['shipping_address'],
                    'grn_date': hdr.get('grn_date') or h['grn_date'],
                    'supplier': h['supplier'], 'source_file': h['source_file'], 'drive_file_id': fid}
            new = []
            for n, it in enumerate(items):
                row = dict(base, **{k: it.get(k) for k in ('product_no', 'product_name', 'hsn', 'uom', 'gst_rate',
                                                           'qty_ordered', 'qty_delivered', 'grn_qty', 'damaged_qty',
                                                           'price_per_unit', 'amount')})
                row['total_tax_amount'] = it.get('Total Tax Amount')
                row['raw_data'] = dict(row, parsed_by=TAG, llama_rows_replaced=len(rows))
                row['row_hash'] = hashlib.sha256(f"hp_local|{fid}|{n}|{it.get('product_no')}".encode()).hexdigest()
                new.append(row)
            changed.append({'invoice': base['vendor_invoice_number'], 'grn_date': base['grn_date'],
                            'stored': [len(rows), s_q, round(s_a, 2)], 'pdf': [len(items), p_q, round(p_a, 2)]})
            backup.write(json.dumps({'drive_file_id': fid, 'old_rows': rows}, default=str) + '\n')
            if a.apply:
                sb.table(TABLE).upsert(new, on_conflict='row_hash').execute()
                sb.table(TABLE).delete().in_('id', [r['id'] for r in rows]).execute()
    backup.close()

    print('\nresult', dict(cnt), 'applied' if a.apply else 'DRY RUN (nothing written)', flush=True)
    for c in changed:
        print('  replaced' if a.apply else '  would replace', c, flush=True)
    for name, why in unreconciled:
        print('  left as is (PDF does not add up):', name, why, flush=True)
    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        with open(summary, 'a', encoding='utf-8') as fh:
            fh.write(f"### Hyperpure local-parse backfill since {a.since}\n\n`{dict(cnt)}` "
                     f"({'applied' if a.apply else 'dry run'})\n\n")
            for c in changed:
                fh.write(f"- {c['invoice']} {c['grn_date']}: stored {c['stored']} -> PDF {c['pdf']}\n")


if __name__ == '__main__':
    main()
