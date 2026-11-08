"""gf_zbieraj.py: pobiera oferty z Google Flights i zapisuje je do bazy (tabela oferty, zrodlo='google').

Co sprawdza (w tej kolejności):
 1. Prośby z wyszukiwań w aplikacji (tabela gf_kolejka): dokładnie te oferty, które pokazało wyszukiwanie w danym
    trybie (daty, weekend, przedłużony weekend, okno), najnowsze prośby pierwsze.
 2. Gdy zostaje czas: najtańszą ofertę każdej trasy z bazy (tylko lotniska z HARVEST_SKAD / GF_SKAD).
Aplikacja czyta je z bazy razem z ofertami Travelpayouts; przy tej samej ofercie Google ma pierwszeństwo.

Uruchomienie (u siebie; PowerShell):
    $env:DATABASE_URL="..."
    python gf_zbieraj.py --sucho                    # pokaż, co by sprawdzono
    python gf_zbieraj.py --max 10 --pokaz           # próba z widocznym oknem
    python gf_zbieraj.py --ciagle --budzet-min 600   # działa w tle i na bieżąco obsługuje prośby z aplikacji
Opcje: --max (zapytań na przebieg, domyślnie 40), --pauza (średnia przerwa w s, domyślnie 10),
       --budzet-min (domyślnie 20), --skad WAW,KRK, --swiezosc-h (domyślnie 24), --horyzont-dni (domyślnie 270),
       --ciagle (po opróżnieniu kolejki czeka 30 s i sprawdza ponownie, aż do końca budżetu), --sucho, --pokaz.
Przy blokadzie Google skrypt kończy i przez GF_BLOKADA_H godzin (domyślnie 6) nie próbuje ponownie."""
import argparse
import os
import random
import sys
import time

import psycopg

import harvest
from gf import Blokada, Przegladarka, najtansze

TABELE = [
    "CREATE TABLE IF NOT EXISTS ceny_google (skad text NOT NULL, cel text NOT NULL, wylot date NOT NULL, "
    "powrot date NOT NULL, cena int, cena_bezposrednio int, przesiadki int, tekst text, "
    "pobrano timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (skad, cel, wylot, powrot))",
    "CREATE TABLE IF NOT EXISTS gf_kolejka (skad text NOT NULL, cel text NOT NULL, lotnisko text, "
    "wylot date NOT NULL, powrot date NOT NULL, prosba timestamptz NOT NULL DEFAULT now(), "
    "PRIMARY KEY (skad, cel, wylot, powrot))",
    "CREATE TABLE IF NOT EXISTS gf_stan (klucz text PRIMARY KEY, do_kiedy timestamptz NOT NULL)",
    "ALTER TABLE oferty ADD COLUMN IF NOT EXISTS zrodlo text NOT NULL DEFAULT 'tp'",
    "ALTER TABLE oferty ADD COLUMN IF NOT EXISTS pobrano timestamptz NOT NULL DEFAULT now()",
]


def zapisz_w_ofertach(conn, skad, cel, lot, wy, po, karty):
    """Zapisuje oferty z Google do tabeli oferty (zrodlo='google'), skąd czyta je aplikacja: najtańszą (z jej
    liczbą przesiadek) i, jeśli jest inna, najtańszą bezpośrednią. Poprzednie wiersze Google dla tej trasy i
    terminów są zastępowane. Wiersze Travelpayouts ("tp") zostają nietknięte."""
    lotnisko = lot or cel
    conn.execute("DELETE FROM oferty WHERE zrodlo='google' AND skad=%s AND cel=%s AND cel_lotnisko=%s "
                 "AND wylot=%s AND powrot=%s", (skad, cel, lotnisko, wy, po))
    if not karty:
        return
    najt = min(karty, key=lambda k: k["cena"])
    wiersze = [(najt["cena"], najt["przesiadki"], najt["tekst"])]
    bezp = [k for k in karty if k["przesiadki"] == 0]
    if bezp and najt["przesiadki"] != 0:
        b = min(bezp, key=lambda k: k["cena"])
        wiersze.append((b["cena"], 0, b["tekst"]))
    for cena, prz, tekst in wiersze:
        conn.execute(
            "INSERT INTO oferty (skad, cel, cel_lotnisko, miesiac, wylot, powrot, cena, przesiadki, przesiadki_powrot, "
            "linia, zrodlo, pobrano) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'google', now())",
            (skad, cel, lotnisko, wy.strftime("%Y-%m"), wy, po, cena, prz, prz, ""))


def z_kolejki(conn, maks, swiezosc_h):
    """Prośby z aplikacji, najnowsze pierwsze, bez ofert sprawdzonych niedawno."""
    conn.execute("DELETE FROM gf_kolejka WHERE prosba < now() - interval '24 hours' OR wylot < current_date + 2")
    conn.execute("DELETE FROM gf_kolejka k WHERE EXISTS (SELECT 1 FROM ceny_google g WHERE g.skad=k.skad AND g.cel=k.cel "
                 "AND g.wylot=k.wylot AND g.powrot=k.powrot AND g.pobrano > now() - make_interval(hours => %s::int))",
                 (swiezosc_h,))
    return conn.execute(
        "SELECT k.skad, k.cel, k.lotnisko, k.wylot, k.powrot, "
        "COALESCE((SELECT MIN(o.cena) FROM oferty o WHERE o.skad=k.skad AND o.cel=k.cel "
        "AND o.wylot=k.wylot AND o.powrot=k.powrot), 0) FROM gf_kolejka k ORDER BY k.prosba DESC LIMIT %s::int",
        (maks,)).fetchall()


def najtansze_z_bazy(conn, skady, maks, swiezosc_h, horyzont):
    """Dla każdej trasy najtańsza oferta z bazy, bez sprawdzonych niedawno; najtańsze pierwsze."""
    wiersze = conn.execute(
        "SELECT DISTINCT ON (o.skad, o.cel) o.skad, o.cel, o.cel_lotnisko, o.wylot, o.powrot, o.cena "
        "FROM oferty o WHERE o.skad = ANY(%s) AND o.wylot >= current_date + 7 "
        "AND o.wylot <= current_date + %s::int ORDER BY o.skad, o.cel, o.cena", (list(skady), horyzont)).fetchall()
    swieze = set(conn.execute(
        "SELECT skad, cel, wylot, powrot FROM ceny_google "
        "WHERE pobrano > now() - make_interval(hours => %s::int)", (swiezosc_h,)).fetchall())
    cele = [w for w in wiersze if (w[0], w[1], w[3], w[4]) not in swieze]
    cele.sort(key=lambda w: w[5])
    return cele[:maks], len(wiersze), len(wiersze) - len(cele)


def sprawdz_i_zapisz(br, baza, zad, pauza, koniec):
    """Jedno zapytanie do Google i zapis. Zwraca "ok", "brak" albo "blokada"."""
    skad, cel, lot, wy, po, cena = zad
    try:
        karty = br.karty(skad, lot or cel, wy.isoformat(), po.isoformat())
    except Blokada:
        godz = int(os.environ.get("GF_BLOKADA_H") or 6)
        with psycopg.connect(baza) as conn:
            conn.execute("INSERT INTO gf_stan (klucz, do_kiedy) VALUES ('blokada', now() + make_interval(hours => %s::int)) "
                         "ON CONFLICT (klucz) DO UPDATE SET do_kiedy = EXCLUDED.do_kiedy", (godz,))
        print(f"Google pokazał blokadę. Kończę i wstrzymuję się na {godz} h (nic nie obchodzę).")
        return "blokada"
    except Exception as e:
        print(f"  {skad}->{cel}: błąd odczytu ({type(e).__name__}: {str(e)[:80]})")
        return "brak"
    n = najtansze(karty)
    with psycopg.connect(baza) as conn:
        conn.execute(
            "INSERT INTO ceny_google (skad, cel, wylot, powrot, cena, cena_bezposrednio, przesiadki, tekst, pobrano) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s, now()) ON CONFLICT (skad, cel, wylot, powrot) DO UPDATE SET "
            "cena=EXCLUDED.cena, cena_bezposrednio=EXCLUDED.cena_bezposrednio, przesiadki=EXCLUDED.przesiadki, "
            "tekst=EXCLUDED.tekst, pobrano=now()",
            (skad, cel, wy, po, n[0] if n else None, n[1] if n else None, n[2] if n else None,
             n[3][:200] if n else None))
        zapisz_w_ofertach(conn, skad, cel, lot, wy, po, karty)
        conn.execute("DELETE FROM gf_kolejka WHERE skad=%s AND cel=%s AND wylot=%s AND powrot=%s", (skad, cel, wy, po))
    if n:
        roz = f", {n[0] - cena:+d} wobec bazy ({cena})" if cena else ""
        print(f"  {skad}->{cel} {wy}..{po}: Google {n[0]} PLN{roz}")
        wynik = "ok"
    else:
        print(f"  {skad}->{cel} {wy}..{po}: brak wyników w Google")
        wynik = "brak"
    time.sleep(max(0, pauza * random.uniform(0.6, 1.5)))
    return wynik


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=40)
    ap.add_argument("--pauza", type=float, default=10)
    ap.add_argument("--budzet-min", type=float, default=20)
    ap.add_argument("--skad", default="")
    ap.add_argument("--swiezosc-h", type=int, default=int(os.environ.get("GF_SWIEZOSC_H") or 24))
    ap.add_argument("--horyzont-dni", type=int, default=270)
    ap.add_argument("--ciagle", action="store_true")
    ap.add_argument("--pokaz", action="store_true")
    ap.add_argument("--sucho", action="store_true", help="tylko pokaż, co by sprawdzono")
    a = ap.parse_args()
    baza = os.environ.get("DATABASE_URL", "")
    if not baza:
        sys.exit("Brak DATABASE_URL.")
    surowe = a.skad or os.environ.get("GF_SKAD") or os.environ.get("HARVEST_SKAD") or "WAW"
    skady = harvest.normalizuj_skady([x.strip().upper() for x in surowe.split(",") if x.strip()],
                                     harvest.mapa_miast(harvest.nowa_sesja()))
    with psycopg.connect(baza) as conn:
        for sql in TABELE:
            conn.execute(sql)
        conn.execute("DELETE FROM ceny_google WHERE wylot < current_date")
        conn.execute("DELETE FROM oferty WHERE zrodlo='google' AND (wylot < current_date OR pobrano < now() - interval '7 days')")

    def blokada_aktywna():
        with psycopg.connect(baza) as conn:
            return conn.execute("SELECT do_kiedy FROM gf_stan WHERE klucz='blokada' AND do_kiedy > now()").fetchone()

    b = blokada_aktywna()
    if b:
        print(f"Wstrzymane po blokadzie Google do {b[0]:%Y-%m-%d %H:%M} UTC. Kończę.")
        return

    def kolejny_pakiet():
        with psycopg.connect(baza) as conn:
            kol = z_kolejki(conn, a.max, a.swiezosc_h)
            reszta = max(0, a.max - len(kol))
            tanie, razem, pominiete = ([], 0, 0)
            if reszta:
                tanie, razem, pominiete = najtansze_z_bazy(conn, skady, reszta + len(kol), a.swiezosc_h, a.horyzont_dni)
                juz = {(k[0], k[1], k[3], k[4]) for k in kol}
                tanie = [t for t in tanie if (t[0], t[1], t[3], t[4]) not in juz][:reszta]
        return kol, tanie, razem, pominiete

    koniec = time.time() + a.budzet_min * 60
    if a.sucho:
        kol, tanie, razem, pominiete = kolejny_pakiet()
        print(f"Z kolejki z aplikacji: {len(kol)}; najtańsze z bazy: {len(tanie)} (tras {razem}, świeżych {pominiete})")
        for zr, lista in (("kolejka", kol), ("baza", tanie)):
            for skad, cel, lot, wy, po, cena in lista:
                print(f"  [{zr}] {skad}->{cel} ({lot}) {wy}..{po} w bazie: {cena} PLN")
        return
    zapisane = brak = 0
    with Przegladarka(a.pokaz) as br:
        while time.time() < koniec - 30:
            kol, tanie, razem, pominiete = kolejny_pakiet()
            zadania = kol + tanie
            if not zadania:
                if not a.ciagle:
                    break
                time.sleep(30)
                continue
            print(f"Do sprawdzenia: {len(kol)} z kolejki, {len(tanie)} najtańszych z bazy")
            for zad in zadania:
                if time.time() > koniec - 30:
                    print("Budżet czasu wyczerpany, resztę sprawdzę w następnym przebiegu.")
                    break
                w = sprawdz_i_zapisz(br, baza, zad, a.pauza, koniec)
                if w == "blokada":
                    print(f"Gotowe: zapisano {zapisane}, bez wyników {brak}.")
                    return
                if w == "ok":
                    zapisane += 1
                else:
                    brak += 1
            if not a.ciagle:
                break
    print(f"Gotowe: zapisano {zapisane}, bez wyników {brak}.")


if __name__ == "__main__":
    main()
