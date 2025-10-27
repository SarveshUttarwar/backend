import pandas as pd
import numpy as np
import datetime as dt
import math
import sys
import os
from typing import Dict, List, Optional, Any, Tuple

VERBOSE = True
TRACE_ROWS = True
OWNER_EXACT_MATCH = True
OWNER_CONTAINS = False

def dbg(label, **kv):
    if VERBOSE:
        msg = f"[{label}] " + " | ".join(f"{k}={v}" for k, v in kv.items())
        print(msg)

def nz(v, default=0):
    if v is None:
        return default
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return default
    if isinstance(v, str):
        s = v.strip()
        return default if s == "" else v
    return v

def _normalize_short_year(y: int) -> int:
    return y + 2000 if y < 100 else y

def parse_date_near_range(v: Any, d_start: dt.date, d_end: dt.date) -> Optional[dt.date]:
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            origin = dt.datetime(1899, 12, 30)
            return (origin + dt.timedelta(days=float(v))).date()
        s = str(v).strip()
        if s == "":
            return None
        s = s.replace("-", "/").replace(".", "/")
        parts = s.split("/")
        if len(parts) == 3 and all(p.strip().replace(",", "").isdigit() for p in parts):
            d1 = int(parts[0].replace(",", ""))
            m1 = int(parts[1].replace(",", ""))
            y1 = int(parts[2].replace(",", ""))
            y1 = _normalize_short_year(y1)
            a = b = None
            try:
                a = dt.date(y1, m1, d1)
            except:
                a = None
            try:
                b = dt.date(y1, d1, m1)
            except:
                b = None
            lo = (d_start - pd.offsets.DateOffset(months=2)).date()
            hi = (d_end + pd.offsets.DateOffset(months=2)).date()
            def inr(x): return x is not None and lo <= x <= hi
            if inr(a) and not inr(b):
                return a
            if inr(b):
                return b
            return a or b
        return pd.to_datetime(s, dayfirst=False, errors="coerce").date()
    except Exception as e:
        dbg("DATE_PARSE_ERR", value=v, err=str(e))
        return None

def is_pallet(shiptype: str) -> bool:
    return "PAL" in str(shiptype).upper()

def read_workbook(path_or_bytes) -> Dict[str, pd.DataFrame]:
    try:
        xls = pd.ExcelFile(path_or_bytes)
        need = [
            "Customer Master", "Service Rate Card", "Storage Type Master",
            "Storage Charges", "Inbound Handling", "Inbound Rep & Shrink Wrap",
            "Outbound Handling", "Outbound Rep", "Outbound Shrink & Pal Count",
            "Return Handling", "Scrap Handling",
            "Storage Summary", "Activity Summary", "Charge Summary"
        ]
        dfs = {}
        for sh in need:
            if sh in xls.sheet_names:
                df = xls.parse(sh, header=0, dtype=object, keep_default_na=False)
                df.columns = [str(c).strip() for c in df.columns]
                dfs[sh] = df
                dbg("SHEET_OK", name=sh, rows=len(df), cols=len(df.columns))
            else:
                dfs[sh] = pd.DataFrame()
                dbg("SHEET_MISSING", name=sh)
        return dfs
    except Exception as e:
        print(f"Error reading workbook: {e}")
        raise

def build_rate_map(rate_df: pd.DataFrame, cust: str, service: str) -> Dict[str, float]:
    if rate_df.empty:
        dbg("RATE_EMPTY", cust=cust, service=service)
        return {}
    df = rate_df.copy()
    df["_CUST"] = df.iloc[:, 0].astype(str).str.upper().str.strip()
    df["_SERVICE"] = df.iloc[:, 2].astype(str).str.upper().str.strip() if df.shape[1] > 2 else ""
    df["_CHARGE"] = df.iloc[:, 3].astype(str).str.upper().str.strip() if df.shape[1] > 3 else ""
    df["_RATE"] = pd.to_numeric(df.iloc[:, 4], errors="coerce") if df.shape[1] > 4 else 0.0
    sub = df[(df["_CUST"] == cust.upper().strip()) & (df["_SERVICE"] == service.upper().strip())]
    dbg("RATE_SUBSET", cust=cust, service=service, rows=len(sub))
    out = {}
    for _, r in sub.iterrows():
        out[str(r["_CHARGE"])] = float(nz(r["_RATE"], 0.0))
    return out

def build_rate_map_with_units(rate_df: pd.DataFrame, cust: str, service: str) -> Dict[str, Tuple[float, str]]:
    if rate_df.empty:
        return {}
    df = rate_df.copy()
    df["_CUST"] = df.iloc[:, 0].astype(str).str.upper().str.strip()
    df["_SERVICE"] = df.iloc[:, 2].astype(str).str.upper().str.strip() if df.shape[1] > 2 else ""
    df["_CHARGE"] = df.iloc[:, 3].astype(str).str.upper().str.strip() if df.shape[1] > 3 else ""
    df["_RATE"] = pd.to_numeric(df.iloc[:, 4], errors="coerce") if df.shape[1] > 4 else 0.0
    unit_col = df.iloc[:, 6] if df.shape[1] > 6 else ""
    df["_UNIT"] = unit_col.astype(str).str.upper().str.strip() if isinstance(unit_col, pd.Series) else ""
    sub = df[(df["_CUST"] == cust.upper().strip()) & (df["_SERVICE"] == service.upper().strip())]
    out = {}
    for _, r in sub.iterrows():
        out[str(r["_CHARGE"])] = (float(nz(r["_RATE"], 0.0)), str(nz(r["_UNIT"], "")))
    dbg("RATE_WITH_UNITS", service=service, items=len(out))
    return out

def find_rate(rate_map: Dict[str, float], tokens: List[str]) -> float:
    for k, v in rate_map.items():
        uk = str(k).upper()
        if any(t.upper() in uk for t in tokens):
            return float(nz(v, 0.0))
    return 0.0

def generate_storage_summary(dfs: Dict[str, pd.DataFrame], cust: str, d_start: dt.date, d_end: dt.date) -> pd.DataFrame:
    dbg("STORAGE_BEGIN", cust=cust, d_start=d_start, d_end=d_end)
    ws = dfs["Storage Charges"]
    rc = dfs["Service Rate Card"]
    if ws.empty:
        dbg("STORAGE_NO_SOURCE")
        return pd.DataFrame(columns=["Brand","Ambient","Charge_Ambient","Dry","Charge_Dry","Chiller","Charge_Chiller","Freezer","Charge_Freezer"])
    cols = ws.columns
    def ci(i): return cols[i-1] if (i-1) < len(cols) else None
    COL_OWNER = ci(1)
    COL_BRAND = ci(3)
    COL_COND = ci(4)
    COL_BIN = ci(5)
    COL_VOL = ci(8)
    COL_HU = ci(10)
    COL_DATE = ci(16)
    needed = [COL_OWNER, COL_BRAND, COL_COND, COL_BIN, COL_VOL, COL_HU, COL_DATE]
    if any(v is None for v in needed):
        dbg("STORAGE_MISSING_COLS", cols=needed)
        return pd.DataFrame(columns=["Brand","Ambient","Charge_Ambient","Dry","Charge_Dry","Chiller","Charge_Chiller","Freezer","Charge_Freezer"])
    rmap_u = build_rate_map_with_units(rc, cust, "STORAGE")
    if not rmap_u:
        dbg("STORAGE_NO_RATES", cust=cust)
    vol_or_hu = {}
    seen_hu = set()
    all_brand_cond = set()
    for i, r in ws.iterrows():
        try:
            own = str(nz(r.get(COL_OWNER, ""))).upper().strip()
            ok = (own == cust) if OWNER_EXACT_MATCH else (cust in own)
            if not ok:
                if TRACE_ROWS: dbg("STO_SKIP_OWNER", row=i+2, own=own)
                continue
            d = parse_date_near_range(r.get(COL_DATE, ""), d_start, d_end)
            if d is None or d < d_start or d > d_end:
                if TRACE_ROWS: dbg("STO_SKIP_DATE", row=i+2, date=r.get(COL_DATE, ""))
                continue
            brand = str(nz(r.get(COL_BRAND, ""))).strip() or "(blank)"
            cond = str(nz(r.get(COL_COND, ""))).upper().strip()
            vol = float(pd.to_numeric(str(nz(r.get(COL_VOL, 0))).replace(",", ""), errors="coerce") or 0.0)
            binv = str(nz(r.get(COL_BIN, ""))).strip()
            hu = str(nz(r.get(COL_HU, ""))).strip() or f"BIN_{binv}"
            key = f"{brand}|{cond}"
            all_brand_cond.add(key)
            rate_unit = rmap_u.get(cond)
            if rate_unit is None:
                for ck, rv in rmap_u.items():
                    if cond in ck:
                        rate_unit = rv
                        break
            unit = rate_unit[1] if rate_unit else ""
            if unit == "M3":
                vol_or_hu[key] = vol_or_hu.get(key, 0.0) + vol
            else:
                k_hu = f"{key}|{hu}"
                if k_hu not in seen_hu:
                    seen_hu.add(k_hu)
                    vol_or_hu[key] = vol_or_hu.get(key, 0.0) + 1.0
        except Exception as e:
            dbg("STO_ROW_ERR", row=i+2, err=str(e))
    brand_map = {}
    for key in all_brand_cond:
        qty = vol_or_hu.get(key, 0.0)
        brand, cond = key.split("|", 1)
        if brand not in brand_map:
            brand_map[brand] = {}
        if cond in rmap_u:
            rate = float(nz(rmap_u[cond][0], 0.0))
        else:
            rate = 0.0
            for ck, (rv, uu) in rmap_u.items():
                if cond in ck:
                    rate = float(nz(rv, 0.0))
                    break
        amt = round(qty * rate, 2)
        brand_map[brand][cond] = {"v": qty, "c": amt}
    rows_out = []
    gt = {"Ambient_v":0,"Ambient_c":0,"Dry_v":0,"Dry_c":0,"Chiller_v":0,"Chiller_c":0,"Freezer_v":0,"Freezer_c":0}
    for brand, conds in brand_map.items():
        rec = {"Brand": brand, "Ambient":0,"Charge_Ambient":0.0,"Dry":0,"Charge_Dry":0.0,"Chiller":0,"Charge_Chiller":0.0,"Freezer":0,"Charge_Freezer":0.0}
        for cond, lv, lc in [("AMBIENT","Ambient","Charge_Ambient"),("DRY","Dry","Charge_Dry"),("CHILLER","Chiller","Charge_Chiller"),("FREEZER","Freezer","Charge_Freezer")]:
            if cond in conds:
                q = float(conds[cond]["v"]); a = float(conds[cond]["c"])
                rec[lv] = q; rec[lc] = a
                if cond == "AMBIENT":
                    gt["Ambient_v"] += q; gt["Ambient_c"] += a
                elif cond == "DRY":
                    gt["Dry_v"] += q; gt["Dry_c"] += a
                elif cond == "CHILLER":
                    gt["Chiller_v"] += q; gt["Chiller_c"] += a
                elif cond == "FREEZER":
                    gt["Freezer_v"] += q; gt["Freezer_c"] += a
        rows_out.append(rec)
    df = pd.DataFrame(rows_out)
    if df.empty:
        df = pd.DataFrame(columns=["Brand","Ambient","Charge_Ambient","Dry","Charge_Dry","Chiller","Charge_Chiller","Freezer","Charge_Freezer"])
    gt_row = {
        "Brand":"Grand Total",
        "Ambient":round(gt["Ambient_v"],2),"Charge_Ambient":round(gt["Ambient_c"],2),
        "Dry":round(gt["Dry_v"],2),"Charge_Dry":round(gt["Dry_c"],2),
        "Chiller":round(gt["Chiller_v"],2),"Charge_Chiller":round(gt["Chiller_c"],2),
        "Freezer":round(gt["Freezer_v"],2),"Charge_Freezer":round(gt["Freezer_c"],2)
    }
    df = pd.concat([df, pd.DataFrame([gt_row])], ignore_index=True)
    dbg("STORAGE_DONE", rows=len(df))
    return df

def generate_activity_inbound(dfs, cust, d_start, d_end):
    dbg("INB_BEGIN", cust=cust)
    ws = dfs["Inbound Handling"]
    rc = dfs["Service Rate Card"]
    rs = dfs["Inbound Rep & Shrink Wrap"]
    out_cols = ["Brand","InLoose","Charge_Loose","InPallet","Charge_Pallet","InRepShr","Charge_RepShr","RowTotal"]
    if ws.empty and rs.empty:
        dbg("INB_NO_SOURCE")
        return pd.DataFrame(columns=out_cols), pd.DataFrame(columns=["Phase","Row","Where","Error/Note","Value"])
    cols = ws.columns
    def ci(i): return cols[i-1] if (i-1) < len(cols) else None
    OWN = ci(1); DT = ci(2); SHP = ci(4); BR = ci(5); DOC = ci(8); HU = ci(15)
    rmap = build_rate_map(rc, cust, "INBOUND HANDLING")
    looseR = find_rate(rmap, ["LOOSE"])
    palletR = find_rate(rmap, ["PALLET"," PAL"])
    repR = find_rate(rmap, ["REP","SHRINK"])
    d_loose = {}; d_pal = {}; rep_map = {}; brands=set(); seen=set(); doc2brand={}
    if not ws.empty:
        for i, r in ws.iterrows():
            owner = str(nz(r.get(OWN, ""))).upper().strip()
            ok = (owner == cust) if OWNER_EXACT_MATCH else (cust in owner)
            if not ok:
                if TRACE_ROWS: dbg("INB_SKIP_OWNER", row=i+2, own=owner)
                continue
            d = parse_date_near_range(r.get(DT, ""), d_start, d_end)
            if d is None or d < d_start or d > d_end:
                if TRACE_ROWS: dbg("INB_SKIP_DATE", row=i+2, date=r.get(DT, ""))
                continue
            brand = str(nz(r.get(BR, ""))).strip() or "(blank)"
            ship = str(nz(r.get(SHP, ""))).strip()
            doc = str(nz(r.get(DOC, ""))).strip()
            if doc and (doc not in doc2brand):
                doc2brand[doc] = brand
            hu = str(nz(r.get(HU, ""))).strip() or f"ROW{i+2}"
            k = f"{brand}|{is_pallet(ship)}|{hu}"
            if k in seen:
                continue
            seen.add(k)
            brands.add(brand)
            if is_pallet(ship):
                d_pal[brand] = d_pal.get(brand, 0) + 1
            else:
                d_loose[brand] = d_loose.get(brand, 0) + 1
    if not rs.empty:
        cols2 = rs.columns
        def c2(i): return cols2[i-1] if (i-1) < len(cols2) else None
        OWN2=c2(1); DT2=c2(2); REP=c2(4); SHR=c2(5); DOC2=c2(7)
        for i, r in rs.iterrows():
            owner = str(nz(r.get(OWN2, ""))).upper().strip()
            ok = (owner == cust) if OWNER_EXACT_MATCH else (cust in owner)
            if not ok:
                continue
            d = parse_date_near_range(r.get(DT2, ""), d_start, d_end)
            if d is None or d < d_start or d > d_end:
                continue
            cnt = int(pd.to_numeric(nz(r.get(REP, 0)), errors="coerce") or 0) + int(pd.to_numeric(nz(r.get(SHR, 0)), errors="coerce") or 0)
            doc = str(nz(r.get(DOC2, ""))).strip()
            b2 = doc2brand.get(doc, "(blank)")
            rep_map[b2] = rep_map.get(b2, 0) + cnt
            brands.add(b2)
    rows=[]
    for b in sorted(brands):
        loo = int(d_loose.get(b, 0))
        pal = int(d_pal.get(b, 0))
        rep = int(rep_map.get(b, 0))
        aL = round(loo * looseR, 2)
        aP = round(pal * palletR, 2)
        aR = round(rep * repR, 2)
        rows.append({"Brand": b, "InLoose": loo, "Charge_Loose": aL, "InPallet": pal, "Charge_Pallet": aP, "InRepShr": rep, "Charge_RepShr": aR, "RowTotal": round(aL + aP + aR, 2)})
    df = pd.DataFrame(rows)
    if df.empty: df = pd.DataFrame(columns=out_cols)
    gt = {
        "Brand":"Grand Total",
        "InLoose": df.get("InLoose", pd.Series(dtype=float)).sum(),
        "Charge_Loose": df.get("Charge_Loose", pd.Series(dtype=float)).sum(),
        "InPallet": df.get("InPallet", pd.Series(dtype=float)).sum(),
        "Charge_Pallet": df.get("Charge_Pallet", pd.Series(dtype=float)).sum(),
        "InRepShr": df.get("InRepShr", pd.Series(dtype=float)).sum(),
        "Charge_RepShr": df.get("Charge_RepShr", pd.Series(dtype=float)).sum(),
        "RowTotal": df.get("RowTotal", pd.Series(dtype=float)).sum()
    }
    df = pd.concat([df, pd.DataFrame([gt])], ignore_index=True)
    diag = pd.DataFrame([{"Phase":"INBOUND","Row":0,"Where":"Activity Summary","Error/Note":"Brands in output","Value": len(df)-1 if not df.empty else 0}])
    dbg("INB_DONE", brands=len(df)-1)
    return df, diag

def generate_activity_outbound(dfs, cust, d_start, d_end):
    dbg("OUT_BEGIN", cust=cust)
    ws = dfs["Outbound Handling"]
    rc = dfs["Service Rate Card"]
    cols_out = ["Brand","OutEach","Charge_Each","OutPack","Charge_Pack","OutCarton","Charge_Carton","OutPallet","Charge_Pallet","RowTotal"]
    if ws.empty:
        dbg("OUT_NO_SOURCE")
        return pd.DataFrame(columns=cols_out)
    cols = ws.columns
    def ci(i): return cols[i-1] if (i-1) < len(cols) else None
    OWN=ci(1); DT=ci(2); BR=ci(4); EACH=ci(9); PACK=ci(11); CART=ci(13); PAL=ci(15)
    rmap = build_rate_map(rc, cust, "OUTBOUND HANDLING")
    eachR = find_rate(rmap, ["EACH"])
    packR = find_rate(rmap, ["PACK"])
    cartR = find_rate(rmap, ["CARTON","CAR","BDL"])
    palR = find_rate(rmap, ["PALLET","PAL"])
    d_each={}; d_pack={}; d_cart={}; d_pal={}; brands=set()
    for i, r in ws.iterrows():
        owner = str(nz(r.get(OWN, ""))).upper().strip()
        ok = (owner == cust) if OWNER_EXACT_MATCH else (cust in owner)
        if not ok:
            continue
        d = parse_date_near_range(r.get(DT, ""), d_start, d_end)
        if d is None or d < d_start or d > d_end:
            continue
        b = str(nz(r.get(BR, ""))).strip()
        brands.add(b)
        def add(dct, col):
            q = float(pd.to_numeric(nz(r.get(col, 0)), errors="coerce") or 0.0)
            if q != 0: dct[b] = dct.get(b, 0.0) + q
        add(d_each, EACH); add(d_pack, PACK); add(d_cart, CART); add(d_pal, PAL)
    rows=[]
    for b in sorted(brands):
        e=float(d_each.get(b,0.0)); p=float(d_pack.get(b,0.0)); c1=float(d_cart.get(b,0.0)); pal=float(d_pal.get(b,0.0))
        aE=round(e*eachR,2); aP=round(p*packR,2); aC=round(c1*cartR,2); aPal=round(pal*palR,2)
        rows.append({"Brand":(b or "(blank)"),"OutEach":e,"Charge_Each":aE,"OutPack":p,"Charge_Pack":aP,"OutCarton":c1,"Charge_Carton":aC,"OutPallet":pal,"Charge_Pallet":aPal,"RowTotal":round(aE+aP+aC+aPal,2)})
    df=pd.DataFrame(rows)
    if df.empty: df=pd.DataFrame(columns=cols_out)
    gt={
        "Brand":"Grand Total",
        "OutEach": df.get("OutEach", pd.Series(dtype=float)).sum(),
        "Charge_Each": df.get("Charge_Each", pd.Series(dtype=float)).sum(),
        "OutPack": df.get("OutPack", pd.Series(dtype=float)).sum(),
        "Charge_Pack": df.get("Charge_Pack", pd.Series(dtype=float)).sum(),
        "OutCarton": df.get("OutCarton", pd.Series(dtype=float)).sum(),
        "Charge_Carton": df.get("Charge_Carton", pd.Series(dtype=float)).sum(),
        "OutPallet": df.get("OutPallet", pd.Series(dtype=float)).sum(),
        "Charge_Pallet": df.get("Charge_Pallet", pd.Series(dtype=float)).sum(),
        "RowTotal": df.get("RowTotal", pd.Series(dtype=float)).sum()
    }
    df = pd.concat([df, pd.DataFrame([gt])], ignore_index=True)
    dbg("OUT_DONE", brands=len(df)-1)
    return df

def generate_activity_outbound_rep(dfs, cust, d_start, d_end):
    dbg("OUT_REP_BEGIN", cust=cust)
    ws = dfs["Outbound Rep"]
    rc = dfs["Service Rate Card"]
    if ws.empty:
        dbg("OUT_REP_NO_SOURCE")
        return pd.DataFrame(columns=["Brand","OutRepallet_Pal","Charge_OutRepallet","RowTotal"])
    rmap = build_rate_map(rc, cust, "OUTBOUND HANDLING")
    repR = 0.0
    for k, v in rmap.items():
        uk = str(k).upper()
        if ("REPALL" in uk) or ("REPALLET" in uk) or (uk.strip() == "REPALLETIZATION"):
            repR = float(nz(v, 0.0)); break
    cols = ws.columns
    def pick(header_text, default_i=None):
        for c in cols:
            if header_text.lower() in str(c).strip().lower():
                return c
        return cols[default_i-1] if default_i and (default_i-1) < len(cols) else cols[-1]
    OWNER = pick("Owner", 1)
    CDATE = pick("Confirmation Date", 2)
    BRAND = pick("Brand Description", 3)
    SRC_HU = pick("Source Handling Unit", 24 if len(cols) >= 24 else len(cols))
    brand_sets = {}
    brands=set()
    for i, r in ws.iterrows():
        own = str(nz(r.get(OWNER, ""))).upper().strip()
        ok = (own == cust) if OWNER_EXACT_MATCH else (cust in own)
        if not ok:
            continue
        d = parse_date_near_range(r.get(CDATE, ""), d_start, d_end)
        if d is None or d < d_start or d > d_end:
            continue
        b = str(nz(r.get(BRAND, ""))).strip() or "Initial"
        hu = str(nz(r.get(SRC_HU, ""))).strip() or f"ROW{i+2}"
        brands.add(b)
        brand_sets.setdefault(b, set()).add(hu)
    rows=[]
    for b in sorted(brands):
        cnt = len(brand_sets.get(b, set()))
        amt = round(cnt * repR, 2)
        rows.append({"Brand": b, "OutRepallet_Pal": cnt, "Charge_OutRepallet": amt, "RowTotal": amt})
    df = pd.DataFrame(rows)
    if df.empty: df = pd.DataFrame(columns=["Brand","OutRepallet_Pal","Charge_OutRepallet","RowTotal"])
    gt = {
        "Brand":"Grand Total",
        "OutRepallet_Pal": df.get("OutRepallet_Pal", pd.Series(dtype=float)).sum(),
        "Charge_OutRepallet": df.get("Charge_OutRepallet", pd.Series(dtype=float)).sum(),
        "RowTotal": df.get("RowTotal", pd.Series(dtype=float)).sum()
    }
    df = pd.concat([df, pd.DataFrame([gt])], ignore_index=True)
    dbg("OUT_REP_DONE", brands=len(df)-1)
    return df

def generate_activity_outbound_shrink_pal(dfs, cust, d_start, d_end):
    dbg("OUT_SHR_BEGIN", cust=cust)
    ws = dfs["Outbound Shrink & Pal Count"]
    rc = dfs["Service Rate Card"]
    if ws.empty:
        dbg("OUT_SHR_NO_SOURCE")
        return pd.DataFrame(columns=["Brand","OutShrinkPal_Pal","Charge_OutShrinkPal","RowTotal"])
    rmap = build_rate_map(rc, cust, "OUTBOUND HANDLING")
    rate = 0.0
    for k, v in rmap.items():
        uk = str(k).upper()
        if uk == "SHRINK WRAP/PALLET OUT" or ("SHRINK" in uk and "PALLET" in uk and "OUT" in uk):
            rate = float(nz(v, 0.0)); break
    cols = ws.columns
    def pick(header):
        for c in cols:
            if header.lower() in str(c).strip().lower(): return c
        return None
    OWNER = pick("Owner") or cols[0]
    CDATE = pick("Created On") or pick("Date") or cols[0]
    BRAND = pick("Brand Description")
    CNT_SHR = pick("Count - Shrink Wrap")
    CNT_PAL = pick("Count - Pallet Outbound")
    brands=set(); count_map={}
    for i, r in ws.iterrows():
        own = str(nz(r.get(OWNER, ""))).upper().strip()
        ok = (own == cust) if OWNER_EXACT_MATCH else (cust in own)
        if not ok: continue
        d = parse_date_near_range(r.get(CDATE, ""), d_start, d_end)
        if d is None or d < d_start or d > d_end: continue
        b = str(nz(r.get(BRAND, ""))).strip() if BRAND else ""
        vshr = int(pd.to_numeric(nz(r.get(CNT_SHR, 0)), errors="coerce") or 0)
        vpal = int(pd.to_numeric(nz(r.get(CNT_PAL, 0)), errors="coerce") or 0)
        brands.add(b)
        count_map[b] = count_map.get(b, 0) + vshr + vpal
    rows=[]
    for b in sorted(brands):
        cnt = int(count_map.get(b, 0))
        amt = round(cnt * rate, 2)
        rows.append({"Brand": b, "OutShrinkPal_Pal": cnt, "Charge_OutShrinkPal": amt, "RowTotal": amt})
    df = pd.DataFrame(rows)
    if df.empty: df = pd.DataFrame(columns=["Brand","OutShrinkPal_Pal","Charge_OutShrinkPal","RowTotal"])
    gt = {
        "Brand":"Grand Total",
        "OutShrinkPal_Pal": df.get("OutShrinkPal_Pal", pd.Series(dtype=float)).sum(),
        "Charge_OutShrinkPal": df.get("Charge_OutShrinkPal", pd.Series(dtype=float)).sum(),
        "RowTotal": df.get("RowTotal", pd.Series(dtype=float)).sum()
    }
    df = pd.concat([df, pd.DataFrame([gt])], ignore_index=True)
    dbg("OUT_SHR_DONE", brands=len(df)-1)
    return df

def generate_activity_return(dfs, cust, d_start, d_end):
    dbg("RET_BEGIN", cust=cust)
    ws = dfs["Return Handling"]
    rc = dfs["Service Rate Card"]
    cols_out = ["Brand","RetEach","Charge_Each","RetPack","Charge_Pack","RetCarton","Charge_Carton","RetPallet","Charge_Pallet","RowTotal"]
    if ws.empty:
        dbg("RET_NO_SOURCE")
        return pd.DataFrame(columns=cols_out)
    cols = ws.columns
    def pick(header):
        for c in cols:
            if header.lower() in str(c).strip().lower(): return c
        return None
    OWNER = pick("Owner") or cols[0]
    CDATE = pick("Confirmation Date") or pick("Date") or cols[0]
    BRAND = pick("Brand Description")
    QTY = pick("Act Dest Qty Alt UoM") or cols[-2]
    UOM = pick("Alt. Unit of Measure") or cols[-1]
    rmap = build_rate_map(rc, cust, "RETURN HANDLING")
    eachR = packR = carR = palR = 0.0
    for k, v in rmap.items():
        uk = str(k).upper()
        if uk == "EACH":
            eachR = float(nz(v, 0.0))
        elif ("PACK" in uk and "FACTORY" in uk and "SEALED" in uk):
            packR = float(nz(v, 0.0))
        elif (("CARTON" in uk or "BUNDLE" in uk) and "FACTORY" in uk and "SEALED" in uk):
            carR = float(nz(v, 0.0))
        elif uk == "PALLET":
            palR = float(nz(v, 0.0))
    d_each={}; d_pack={}; d_car={}; d_pal={}; brands=set()
    for i, r in ws.iterrows():
        own = str(nz(r.get(OWNER, ""))).upper().strip()
        ok = (cust in own) if not OWNER_EXACT_MATCH else (own == cust or cust in own)
        if not ok: continue
        d = parse_date_near_range(r.get(CDATE, ""), d_start, d_end)
        if d is None or d < d_start or d > d_end: continue
        b = str(nz(r.get(BRAND, ""))).strip() if BRAND else "Initial"
        u = str(nz(r.get(UOM, ""))).upper().strip()
        q = float(pd.to_numeric(nz(r.get(QTY, 0)), errors="coerce") or 0.0)
        brands.add(b)
        if u in ["EA","KG","BAG","GM"]:
            d_each[b] = d_each.get(b, 0.0) + q
        elif u == "PAC":
            d_pack[b] = d_pack.get(b, 0.0) + q
        elif u in ["CAR","BDL"]:
            d_car[b] = d_car.get(b, 0.0) + q
        elif u == "PAL":
            d_pal[b] = d_pal.get(b, 0.0) + q
    rows=[]
    for b in sorted(brands):
        qE=d_each.get(b,0.0); qP=d_pack.get(b,0.0); qC=d_car.get(b,0.0); qL=d_pal.get(b,0.0)
        aE=round(qE*eachR,2); aP=round(qP*packR,2); aC=round(qC*carR,2); aL=round(qL*palR,2)
        rows.append({"Brand": b, "RetEach": qE, "Charge_Each": aE, "RetPack": qP, "Charge_Pack": aP, "RetCarton": qC, "Charge_Carton": aC, "RetPallet": qL, "Charge_Pallet": aL, "RowTotal": round(aE+aP+aC+aL, 2)})
    df=pd.DataFrame(rows)
    if df.empty: df=pd.DataFrame(columns=cols_out)
    gt = {
        "Brand":"Grand Total",
        "RetEach": df.get("RetEach", pd.Series(dtype=float)).sum(),
        "Charge_Each": df.get("Charge_Each", pd.Series(dtype=float)).sum(),
        "RetPack": df.get("RetPack", pd.Series(dtype=float)).sum(),
        "Charge_Pack": df.get("Charge_Pack", pd.Series(dtype=float)).sum(),
        "RetCarton": df.get("RetCarton", pd.Series(dtype=float)).sum(),
        "Charge_Carton": df.get("Charge_Carton", pd.Series(dtype=float)).sum(),
        "RetPallet": df.get("RetPallet", pd.Series(dtype=float)).sum(),
        "Charge_Pallet": df.get("Charge_Pallet", pd.Series(dtype=float)).sum(),
        "RowTotal": df.get("RowTotal", pd.Series(dtype=float)).sum()
    }
    df = pd.concat([df, pd.DataFrame([gt])], ignore_index=True)
    dbg("RET_DONE", brands=len(df)-1)
    return df

def generate_activity_scrap(dfs, cust, d_start, d_end):
    dbg("SCRAP_BEGIN", cust=cust)
    ws = dfs["Scrap Handling"]
    rc = dfs["Service Rate Card"]
    cols_out = ["Brand","ScrapNormal_TON","Charge_Normal","ScrapMunicipality_TON","Charge_Municipality","RowTotal"]
    if ws.empty:
        dbg("SCRAP_NO_SOURCE")
        return pd.DataFrame(columns=cols_out)
    cols = ws.columns
    def pick(header):
        for c in cols:
            if header.lower() in str(c).strip().lower(): return c
        return None
    OWNER = pick("Owner") or cols[0]
    CDATE = pick("Confirmation Date") or pick("Date") or cols[0]
    BRAND = pick("Brand Description")
    REASON = pick("Movement Reason")
    WT = pick("Loading Weight")
    UOM = pick("Weight Unit")
    rmap = build_rate_map(rc, cust, "SCRAP HANDLING")
    normalR = 0.0; muniR = 0.0
    for k, v in rmap.items():
        uk = str(k).upper().strip()
        if uk == "NORMAL":
            normalR = float(nz(v, 0.0))
        elif uk in ("MUNICIPALITY","MUNICIPAL"):
            muniR = float(nz(v, 0.0))
    d_norm={}; d_muni={}; brands=set()
    for i, r in ws.iterrows():
        own = str(nz(r.get(OWNER, ""))).upper().strip()
        ok = (cust in own) if not OWNER_EXACT_MATCH else (own == cust or cust in own)
        if not ok: continue
        d = parse_date_near_range(r.get(CDATE, ""), d_start, d_end)
        if d is None or d < d_start or d > d_end: continue
        b = str(nz(r.get(BRAND, ""))).strip() if BRAND else "Initial"
        rsn = str(nz(r.get(REASON, ""))).upper().strip()
        wRaw = float(pd.to_numeric(str(nz(r.get(WT, 0))).replace(",", ""), errors="coerce") or 0.0)
        u = str(nz(r.get(UOM, ""))).upper().strip() if UOM else ""
        if u == "KG":
            wTon = wRaw/1000.0
        elif u in ("TON","T",""):
            wTon = wRaw
        else:
            wTon = wRaw
        brands.add(b)
        if rsn == "SCNM":
            d_norm[b] = d_norm.get(b, 0.0) + wTon
        elif rsn == "SCMU":
            d_muni[b] = d_muni.get(b, 0.0) + wTon
    rows=[]
    for b in sorted(brands):
        qN=d_norm.get(b,0.0); qM=d_muni.get(b,0.0)
        aN=round(qN*normalR,2); aM=round(qM*muniR,2)
        rows.append({"Brand": b, "ScrapNormal_TON": qN, "Charge_Normal": aN, "ScrapMunicipality_TON": qM, "Charge_Municipality": aM, "RowTotal": round(aN + aM, 2)})
    df=pd.DataFrame(rows)
    if df.empty: df=pd.DataFrame(columns=cols_out)
    gt = {
        "Brand":"Grand Total",
        "ScrapNormal_TON": df.get("ScrapNormal_TON", pd.Series(dtype=float)).sum(),
        "Charge_Normal": df.get("Charge_Normal", pd.Series(dtype=float)).sum(),
        "ScrapMunicipality_TON": df.get("ScrapMunicipality_TON", pd.Series(dtype=float)).sum(),
        "Charge_Municipality": df.get("Charge_Municipality", pd.Series(dtype=float)).sum(),
        "RowTotal": df.get("RowTotal", pd.Series(dtype=float)).sum()
    }
    df = pd.concat([df, pd.DataFrame([gt])], ignore_index=True)
    dbg("SCRAP_DONE", brands=len(df)-1)
    return df

def assemble_activity_summary(df_in, df_out, df_rep, df_shr, df_ret, df_scr):
    dbg("ACT_ASSEMBLE_BEGIN")
    brands=set()
    for d in [df_in, df_out, df_rep, df_shr, df_ret, df_scr]:
        if not d.empty:
            brands.update(d.loc[d["Brand"]!="Grand Total","Brand"].astype(str).tolist())
    rows=[]
    for b in sorted(brands):
        rec = {
            "Brand": b,
            "InLoose":0,"Charge_Loose":0.0,"InPallet":0,"Charge_Pallet":0.0,"InRepShr":0,"Charge_RepShr":0.0,
            "OutEach":0.0,"Charge_Each":0.0,"OutPack":0.0,"Charge_Pack":0.0,"OutCarton":0.0,"Charge_Carton":0.0,"OutPallet":0.0,"Charge_Pallet_Out":0.0,
            "OutRepallet_Pal":0,"Charge_OutRepallet":0.0,
            "OutShrinkPal_Pal":0,"Charge_OutShrinkPal":0.0,
            "RetEach":0.0,"Charge_Each_Return":0.0,"RetPack":0.0,"Charge_Pack_Return":0.0,"RetCarton":0.0,"Charge_Carton_Return":0.0,"RetPallet":0.0,"Charge_Pallet_Return":0.0,
            "ScrapNormal_TON":0.0,"Charge_ScrapNormal":0.0,"ScrapMunicipality_TON":0.0,"Charge_ScrapMunicipality":0.0,
            "RowTotal":0.0
        }
        def merge(src, m):
            if src.empty: return
            r = src.loc[src["Brand"]==b]
            if r.empty: return
            r = r.iloc[0]
            for sk, dk in m.items():
                value = r.get(sk, 0.0)
                rec[dk] = float(nz(value, 0.0)) if isinstance(value,(int,float)) else 0.0
        merge(df_in, {"InLoose":"InLoose","Charge_Loose":"Charge_Loose","InPallet":"InPallet","Charge_Pallet":"Charge_Pallet","InRepShr":"InRepShr","Charge_RepShr":"Charge_RepShr"})
        merge(df_out, {"OutEach":"OutEach","Charge_Each":"Charge_Each","OutPack":"OutPack","Charge_Pack":"Charge_Pack","OutCarton":"OutCarton","Charge_Carton":"Charge_Carton","OutPallet":"OutPallet","Charge_Pallet":"Charge_Pallet_Out"})
        merge(df_rep, {"OutRepallet_Pal":"OutRepallet_Pal","Charge_OutRepallet":"Charge_OutRepallet"})
        merge(df_shr, {"OutShrinkPal_Pal":"OutShrinkPal_Pal","Charge_OutShrinkPal":"Charge_OutShrinkPal"})
        merge(df_ret, {"RetEach":"RetEach","Charge_Each":"Charge_Each_Return","RetPack":"RetPack","Charge_Pack":"Charge_Pack_Return","RetCarton":"RetCarton","Charge_Carton":"Charge_Carton_Return","RetPallet":"RetPallet","Charge_Pallet":"Charge_Pallet_Return"})
        merge(df_scr, {"ScrapNormal_TON":"ScrapNormal_TON","Charge_Normal":"Charge_ScrapNormal","ScrapMunicipality_TON":"ScrapMunicipality_TON","Charge_Municipality":"Charge_ScrapMunicipality"})
        rec["RowTotal"] = round(sum(float(nz(rec[k],0.0)) for k in rec if "Charge_" in k), 2)
        rows.append(rec)
    df=pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=list(rows[0].keys()) if rows else [])
    gt={}
    for c in df.columns:
        if c != "Brand":
            df[c]=pd.to_numeric(df[c],errors="coerce").fillna(0.0)
            gt[c]=df[c].sum()
    gt["Brand"]="Grand Total"
    gt_row={k: round(float(v),2) for k,v in gt.items() if k!="Brand"}
    gt_row["Brand"]="Grand Total"
    df=pd.concat([df, pd.DataFrame([gt_row])], ignore_index=True)
    dbg("ACT_ASSEMBLE_DONE", rows=len(df))
    return df

def get_fixed_charge_amount(service_rate_df: pd.DataFrame, cust_code: str) -> float:
    if service_rate_df.empty:
        return 0.0
    df = service_rate_df.copy()
    df["_CUST"] = df.iloc[:, 0].astype(str).str.upper().str.strip()
    df["_SERVICE"] = df.iloc[:, 2].astype(str).str.upper().str.strip() if df.shape[1] > 2 else ""
    df["_CHARGE"] = df.iloc[:, 3].astype(str).str.upper().str.strip() if df.shape[1] > 3 else ""
    df["_RATE"] = pd.to_numeric(df.iloc[:, 4], errors="coerce") if df.shape[1] > 4 else 0.0
    df = df[df["_CUST"] == cust_code.upper().strip()]
    df = df[df.apply(lambda r: ("FIXED" in r["_SERVICE"]) or ("INVENTORY" in r["_CHARGE"] and "MANAGEMENT" in r["_CHARGE"]), axis=1)]
    amt = float(df["_RATE"].iloc[0]) if not df.empty else 0.0
    dbg("FIXED_CHARGE", amount=amt)
    return amt

def generate_charge_summary(dfs: Dict[str, pd.DataFrame], cust: str, storage_df: pd.DataFrame, activity_df: pd.DataFrame) -> pd.DataFrame:
    dbg("CHARGE_BEGIN", cust=cust)
    rc = dfs["Service Rate Card"]
    amtFixed = get_fixed_charge_amount(rc, cust)
    gtS = storage_df.iloc[-1] if not storage_df.empty else pd.Series()
    amtStorage = sum(float(nz(gtS.get(x, 0.0)) or 0.0) for x in ["Charge_Ambient","Charge_Dry","Charge_Chiller","Charge_Freezer"])
    gtA = activity_df.iloc[-1] if not activity_df.empty else pd.Series()
    val = lambda col: float(nz(gtA.get(col, 0.0)) or 0.0)
    lines=[]
    def add(svc, ctype, amt):
        lines.append({"Service Type": svc, "Charge Type": ctype, "Charges": round(amt, 2) if round(amt, 2) > 0 else " - "})
    add("Fixed Charge", "Inventory Management", amtFixed)
    add("Storage", "Storage charges", amtStorage)
    add("Inbound Handling", "Inbound Loose", val("Charge_Loose"))
    add("Inbound Handling", "Inbound Pallet", val("Charge_Pallet"))
    add("Inbound Handling", "Inbound Repalletization/Shrink Wrap", val("Charge_RepShr"))
    add("Outbound Handling", "Outbound Each", val("Charge_Each"))
    add("Outbound Handling", "Outbound Pack", val("Charge_Pack"))
    add("Outbound Handling", "Outbound Carton", val("Charge_Carton"))
    add("Outbound Handling", "Outbound Pallet", val("Charge_Pallet_Out"))
    add("Outbound Handling", "Outbound Repalletization", val("Charge_OutRepallet"))
    add("Outbound Handling", "Outbound Shrink Wrap/Pallet Out", val("Charge_OutShrinkPal"))
    add("Return Handling", "Return Each", val("Charge_Each_Return"))
    add("Return Handling", "Return Pack - Factory Sealed", val("Charge_Pack_Return"))
    add("Return Handling", "Return Carton - Factory Sealed", val("Charge_Carton_Return"))
    add("Return Handling", "GRV - Pallet", val("Charge_Pallet_Return"))
    add("Scrap Handling", "Scrap Normal", val("Charge_ScrapNormal"))
    add("Scrap Handling", "Scrap Municipality", val("Charge_ScrapMunicipality"))
    add("Labelling (VAS)", "Re-Labelling / Promo Packing", 0.0)
    df = pd.DataFrame(lines)
    total = round(sum([x for x in df["Charges"] if isinstance(x, (int, float, np.floating))]), 2)
    df = pd.concat([df, pd.DataFrame([{"Service Type":"", "Charge Type":"Total Charges", "Charges": total}])], ignore_index=True)
    dbg("CHARGE_DONE", lines=len(df))
    return df

def run_engine(wb_path: str, cust: str, d_start: dt.date, d_end: dt.date) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    dfs = read_workbook(wb_path)
    storage_df = generate_storage_summary(dfs, cust, d_start, d_end)
    inbound_df, diag_df = generate_activity_inbound(dfs, cust, d_start, d_end)
    outbound_df = generate_activity_outbound(dfs, cust, d_start, d_end)
    outbound_rep_df = generate_activity_outbound_rep(dfs, cust, d_start, d_end)
    outbound_shr_df = generate_activity_outbound_shrink_pal(dfs, cust, d_start, d_end)
    return_df = generate_activity_return(dfs, cust, d_start, d_end)
    scrap_df = generate_activity_scrap(dfs, cust, d_start, d_end)
    activity_df = assemble_activity_summary(inbound_df, outbound_df, outbound_rep_df, outbound_shr_df, return_df, scrap_df)
    charge_df = generate_charge_summary(dfs, cust, storage_df, activity_df)
    return storage_df, activity_df, charge_df, diag_df

def looks_like_excel_path(p: str) -> bool:
    if not isinstance(p, str):
        return False
    pl = p.lower()
    return (pl.endswith(".xlsx") or pl.endswith(".xls")) and os.path.sep in p

def safe_argv(idx: int, default_val: str) -> str:
    try:
        val = sys.argv[idx]
        if val.startswith("-"):
            return default_val
        if idx == 1 and (not looks_like_excel_path(val)):
            return default_val
        return val
    except Exception:
        return default_val

def pick_customer_from_master(wb_path: str) -> Optional[str]:
    try:
        xls = pd.ExcelFile(wb_path)
        if "Customer Master" not in xls.sheet_names:
            print("Note: 'Customer Master' sheet not found; enter customer code manually.")
            return None
        df = xls.parse("Customer Master", dtype=object, keep_default_na=False)
        if df.empty:
            print("Note: 'Customer Master' is empty; enter customer code manually.")
            return None
        codes = df.iloc[:,0].astype(str).str.strip().replace("", np.nan).dropna().unique().tolist()
        codes = [c for c in codes if c and c.lower() != "customer"]
        codes_sorted = sorted(set(codes))
        print("\nSelect Customer:")
        for i, c in enumerate(codes_sorted[:200], start=1):
            print(f"{i}. {c}")
        if len(codes_sorted) > 200:
            print("... list truncated to 200 entries")
        choice = input("Enter number or type a customer code: ").strip()
        if choice.isdigit():
            n = int(choice)
            if 1 <= n <= len(codes_sorted[:200]):
                return str(codes_sorted[n-1]).upper()
        if choice:
            return choice.upper()
        return None
    except Exception as e:
        print(f"Customer picker error: {e}")
        return None

def get_billing_frequency(wb_path: str, cust_code: str) -> int:
    try:
        xls = pd.ExcelFile(wb_path)
        if "Customer Master" not in xls.sheet_names:
            print("Note: 'Customer Master' sheet not found; using default billing frequency of 30 days.")
            return 30
        df = xls.parse("Customer Master", dtype=object, keep_default_na=False)
        if df.empty:
            print("Note: 'Customer Master' is empty; using default billing frequency of 30 days.")
            return 30
        
        # Find the "Billing Frequency(In Days)" column (case-insensitive)
        cols = df.columns.str.lower()
        freq_col = None
        for col in cols:
            if "billing frequency" in col and "days" in col:
                freq_col = df.columns[cols.get_loc(col)]
                break
        if freq_col is None:
            print("Note: 'Billing Frequency(In Days)' column not found; using default billing frequency of 30 days.")
            return 30
        
        # Match customer code (case-insensitive)
        df["_CUST"] = df.iloc[:, 0].astype(str).str.upper().str.strip()
        cust_row = df[df["_CUST"] == cust_code.upper().strip()]
        if cust_row.empty:
            print(f"Note: Customer {cust_code} not found in 'Customer Master'; using default billing frequency of 30 days.")
            return 30
        
        # Get billing frequency
        freq = cust_row[freq_col].iloc[0]
        freq_val = pd.to_numeric(freq, errors="coerce")
        if pd.isna(freq_val) or freq_val <= 0:
            print(f"Note: Invalid or missing billing frequency for {cust_code}; using default of 30 days.")
            return 30
        
        freq_val = int(freq_val)
        print(f"Billing frequency for {cust_code}: {freq_val} days")
        return freq_val
    except Exception as e:
        print(f"Error retrieving billing frequency: {e}; using default billing frequency of 30 days.")
        return 30

def prompt_start_date(default_val: str) -> dt.date:
    s = input(f"Enter Start Date (dd/mm/yyyy or yyyy-mm-dd) [{default_val}]: ").strip()
    if not s:
        s = default_val
    try:
        if "/" in s:
            d, m, y = s.split("/")
            return dt.date(int(y), int(m), int(d))
        return dt.datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        print("Invalid date, using default.")
        if "/" in default_val:
            d, m, y = default_val.split("/")
            return dt.date(int(y), int(m), int(d))
        return dt.datetime.strptime(default_val, "%Y-%m-%d").date()

if __name__ == "__main__":
    try:
        # Use absolute path from user input
        wb_path = r"C:\Users\User\Downloads\reactapp\backend\demo_data_billingsystem_latest_1 (2).xlsx"
        if not os.path.exists(wb_path):
            print(f"Error: Excel file not found at {wb_path}. Please check the path and try again.")
            sys.exit(1)
        
        print(f"\nWorkbook: {wb_path}")
        env_cust = os.environ.get("BILLING_CUST", "").strip().upper()
        cust_code = env_cust if env_cust else None
        if not cust_code:
            picked = pick_customer_from_master(wb_path)
            if picked:
                cust_code = picked
        if not cust_code:
            cust_code = input("Enter Customer Code: ").strip().upper() or "C2201"
        
        env_start = os.environ.get("BILLING_START", "").strip()
        default_start = env_start if env_start else "2025-07-01"
        start_date = prompt_start_date(default_start)
        
        # Get billing frequency and calculate end_date
        billing_freq = get_billing_frequency(wb_path, cust_code)
        end_date = start_date + dt.timedelta(days=billing_freq - 1)  # Subtract 1 to include start_date in period
        
        dbg("MAIN_INPUTS", file=wb_path, cust=cust_code, start=start_date, end=end_date, billing_freq=billing_freq)
        
        storage_df, activity_df, charge_df, diag_df = run_engine(wb_path, cust_code, start_date, end_date)
        
        # Print summaries to console
        print("\n=== Storage Summary ===")
        print(storage_df.to_string(index=False))
        print("\n=== Activity Summary ===")
        print(activity_df.to_string(index=False))
        print("\n=== Charge Summary ===")
        print(charge_df.to_string(index=False))
        print("\n=== Diagnostics (Inbound) ===")
        print(diag_df.to_string(index=False))
        
        # Save summaries to Excel
        output_dir = os.path.dirname(wb_path)
        output_path = os.path.join(output_dir, f"billing_summary_output_{cust_code}_{start_date.strftime('%Y%m%d')}.xlsx")
        
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                storage_df.to_excel(writer, sheet_name="Storage Summary", index=False)
                activity_df.to_excel(writer, sheet_name="Activity Summary", index=False)
                charge_df.to_excel(writer, sheet_name="Charge Summary", index=False)
                diag_df.to_excel(writer, sheet_name="Diagnostics", index=False)
            print(f"\nResults saved to: {output_path}")
        except Exception as e:
            print(f"Error saving Excel file: {e}")
        
    except Exception as e:
        print(f"Runtime Error: {e}")
        sys.exit(1)