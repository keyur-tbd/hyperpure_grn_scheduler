"""Local text parser for Hyperpure GOODS RECEIVED NOTE PDFs, used to check LlamaExtract's rows.

LlamaExtract sometimes returns only some of a GRN's lines (27KOL-02971: 2 of 11, 27BLR-09202: 1 of 16) and nothing
noticed. Every Hyperpure GRN prints a grand total ("Total ₹94836.39") that equals the sum of its line amounts, so a
set of lines is complete when its amounts add up to that total. parse() reads the lines straight from the PDF's text
layer, with the same keys the Hyperpure Agent emits; check() compares an extraction with the printed total.

    python hyperpure_pdf.py some_grn.pdf          # print the parsed lines and whether they reconcile
"""
import re
import sys
from typing import Any, Dict, List, Optional

NUM = r'-?\d+(?:\.\d+)?'
# 145629 Whole Wheat Fourgrain 19051000 107 107 107 0 164.5 Count 0% 0 17601.5
# product no, [name fragment], HSN, qty ord, qty del, GRN qty, damaged, price, UoM, GST rate, tax, amount
LINE = re.compile(rf'^(\d{{4,8}})\s+(.*?)\s*(\d{{8}})\s+({NUM})\s+({NUM})\s+({NUM})\s+({NUM})\s+({NUM})\s+(\S+)\s+({NUM}%)\s+({NUM})\s+({NUM})$')
TOTAL = re.compile(r'^Total\s*₹\s*([\d,]+(?:\.\d+)?)\s*$')
# PO no, PO date, supplier bill number (may hold a space: '27BLR -08408'), delivered on
HEADER = re.compile(r'([A-Z0-9]+-PO-\d+)\s+(\d{1,2} [A-Za-z]{3} \d{4})\s+(.+?)\s+(\d{1,2} [A-Za-z]{3} \d{4})')
MONTHS = {m: i for i, m in enumerate(['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'], 1)}
TOLERANCE = 1.0   # rupees; line amounts are printed to 2 decimals, the total too


def _iso(d: str) -> str:
    day, mon, year = d.split()
    return f'{int(year):04d}-{MONTHS[mon[:3].lower()]:02d}-{int(day):02d}'


def _num(s: str) -> str:
    """'107' stays '107', '164.50' -> '164.5' (LlamaExtract's style; the sink casts anyway)"""
    return s.rstrip('0').rstrip('.') if '.' in s else s


def _shipped_to(pdf) -> Optional[str]:
    """The right-hand 'Shipped To' column of page 1, as one line (what the agent returns as shipping_address)."""
    page = pdf.pages[0]
    text = page.crop((page.width / 2, 0, page.width, page.height)).extract_text() or ''
    lines = text.splitlines()
    try:
        start = next(i for i, l in enumerate(lines) if l.strip().startswith('Shipped To'))
    except StopIteration:
        return None
    out = []
    for l in lines[start + 1:]:
        l = l.strip()
        if l.startswith(('State', 'Phone', 'GSTIN', 'GRN Number')):
            break
        l = re.sub(r'^(Shipping|Address\s*:)\s*', '', l).strip()
        if l and (not out or l != out[0]):
            out.append(l)
    # the name line repeats as the first address line ("Zomato ... (CPC-KOL2)" then "Zomato ... (CPC-KOL2), 1, 4, ...")
    if len(out) > 1 and out[1].startswith(out[0]):
        out = out[1:]
    return ' '.join(out) or None


def parse(path: str) -> Dict[str, Any]:
    """{'header': {...}, 'items': [...], 'printed_total': float or None}. Raises nothing for odd layouts; an
    unrecognised PDF just comes back with no items and no total, which check() reports as not reconciled."""
    import pdfplumber
    with pdfplumber.open(path) as pdf:
        text = '\n'.join((p.extract_text() or '') for p in pdf.pages)
        shipping = _shipped_to(pdf) if pdf.pages else None
    header: Dict[str, Any] = {}
    m = HEADER.search(text)
    if m:
        header = {'po_number': m.group(1), 'vendor_invoice_number': m.group(3), 'grn_date': _iso(m.group(4))}
    if shipping:
        header['shipping_address'] = shipping

    items: List[Dict[str, Any]] = []
    printed_total = None
    in_table, pending = False, []          # pending = name fragments seen since the last item line
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith('Product No'):
            in_table, pending = True, []
            continue
        t = TOTAL.match(line)
        if t:
            printed_total = float(t.group(1).replace(',', ''))
            if items and pending:
                items[-1]['_tail'].extend(pending)
            # the GRN's own table ends here; a short delivery appends a VENDOR DEBIT NOTE with its own
            # Product No table and Total, which is not part of the receipt
            break
        if not in_table:
            continue
        m = LINE.match(line)
        if not m:
            if line and not line.startswith(('Damaged', 'Qty.')):
                pending.append(line)
            continue
        # fragments before this line: a new product name starts at its brand prefix ("BH-", "BH - "); anything
        # ahead of that is the previous product's wrapped tail
        lead = pending
        cut = next((i for i, f in enumerate(pending) if re.match(r'^BH\s*-', f)), None)
        if items and cut is not None:
            items[-1]['_tail'].extend(pending[:cut])
            lead = pending[cut:]
        elif items and cut is None:
            items[-1]['_tail'].extend(pending)
            lead = []
        pending = []
        (no, mid, hsn, q_ord, q_del, q_grn, dmg, price, uom, gst, tax, amt) = m.groups()
        items.append({'product_no': no, '_lead': lead + ([mid] if mid else []), '_tail': [], 'hsn': hsn,
                      'qty_ordered': _num(q_ord), 'qty_delivered': _num(q_del), 'grn_qty': _num(q_grn),
                      'damaged_qty': _num(dmg), 'price_per_unit': _num(price), 'uom': uom, 'gst_rate': gst,
                      'Total Tax Amount': _num(tax), 'amount': _num(amt)})
    for it in items:
        it['product_name'] = ' '.join(it.pop('_lead') + it.pop('_tail')).strip()
    return {'header': header, 'items': items, 'printed_total': printed_total}


def amount_sum(items) -> float:
    total = 0.0
    for it in items:
        try:
            total += float(str(it.get('amount') or 0).replace(',', ''))
        except ValueError:
            pass
    return total


def check(items, printed_total: Optional[float]) -> Optional[str]:
    """None when the lines add up to the printed total, else a one-line reason."""
    if printed_total is None:
        return 'no printed total found in the PDF text'
    got = amount_sum(items)
    if abs(got - printed_total) > TOLERANCE:
        return f'{len(items)} line(s) add up to {got:.2f}, the GRN prints {printed_total:.2f}'
    return None


if __name__ == '__main__':
    for p in sys.argv[1:]:
        r = parse(p)
        print(p, r['header'])
        for it in r['items']:
            print('  ', it)
        print('  reconciles' if check(r['items'], r['printed_total']) is None else '  ' + check(r['items'], r['printed_total']))
