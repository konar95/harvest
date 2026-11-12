"""Harvester: pobiera oferty lotów (Travelpayouts) i klimat (Open-Meteo) do bazy Postgres.

python harvest.py            - zbiera ceny (uruchamia GitHub Actions)
python harvest.py --klimat   - uzupełnia klimat lotnisk (uruchamiaj lokalnie; można przerwać i wznowić)

Przy zbieraniu cen zapisuje też historię cen (tabela historia_cen, do trendów) i oferty specjalne (tabela okazje).
Zmienne opcjonalne: HISTORIA_DNI (domyślnie 120), HARVEST_OKAZJE=0 (wyłącza oferty specjalne), TP_MARKET (np. pl).
"""
import csv
import io
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import psycopg
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

URL_CEN = "https://api.travelpayouts.com/aviasales/v3/prices_for_dates"
WALUTA = "pln"
MIESIECY_NAPRZOD = 8              # ile miesięcy do przodu zbieramy
WATKI = 4                         # ile zapytań o ceny równolegle
BUDZET_MIN = float(os.environ.get("HARVEST_BUDZET_MIN") or 25)   # po tylu minutach kończymy sami (limit zadania w GitHub Actions)
MARGINES_LOTNISKA = 120                # nie zaczynamy nowego lotniska, gdy do końca budżetu zostało mniej sekund
MARGINES_PACZKI = 45                   # nie zaczynamy nowej paczki zapytań, gdy do końca budżetu zostało mniej sekund
PACZKA = int(os.environ.get("HARVEST_PACZKA") or 150)             # tyle zapytań zapisujemy do bazy naraz
BAZA_LOTNISK = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
# kierunki zawsze zbierane (kody lotnisk), niezależnie od tego, czy baza cen je poleca
DODATKOWE = [
    # północ, kierunki zorzy polarnej i Laponii (dotychczasowe)
    "TOS", "RVN", "KRN", "KEF", "ALF", "KTT", "IVL", "LYR",
    # popularne połączenia z Polski: Szwecja, Norwegia, Finlandia (bez lotów krajowych)
    "ARN", "GOT", "OSL", "TRF", "BGO", "SVG", "TRD", "AES", "BOO", "HEL",
]


URL_OKAZJI = "https://api.travelpayouts.com/aviasales/v3/get_special_offers"
HISTORIA_DNI = int(os.environ.get("HISTORIA_DNI") or 120)             # tyle dni trzymamy historię cen
OKAZJE_WLACZONE = (os.environ.get("HARVEST_OKAZJE") or "1") != "0"   # 0 = nie zbieraj ofert specjalnych
TP_MARKET = (os.environ.get("TP_MARKET") or "").strip()               # opcjonalny rynek danych dla ofert specjalnych, np. pl
TABELE_CEN = [
    # najniższa cena dnia dla pary skąd-dokąd i miesiąca wylotu; wiersz powstaje tylko, gdy cena się zmieniła
    "CREATE TABLE IF NOT EXISTS historia_cen (skad text NOT NULL, cel text NOT NULL, miesiac text NOT NULL, "
    "dzien date NOT NULL DEFAULT current_date, cena int NOT NULL, PRIMARY KEY (skad, cel, miesiac, dzien))",
    "CREATE TABLE IF NOT EXISTS okazje (skad text NOT NULL, cel text NOT NULL, cel_lotnisko text, cel_nazwa text, "
    "wylot date, powrot date, cena int NOT NULL, linia text, tytul text, link text, "
    "pobrano timestamptz NOT NULL DEFAULT now())",
    "CREATE INDEX IF NOT EXISTS okazje_idx ON okazje (skad, cena)",
]
# Zapis historii: jeden wiersz na dzień i tylko wtedy, gdy cena różni się od ostatnio zapisanej (oszczędność miejsca).
SQL_HISTORIA = (
    "INSERT INTO historia_cen (skad, cel, miesiac, dzien, cena) "
    "SELECT n.skad, n.cel, n.miesiac, current_date, n.cena FROM ("
    "  SELECT skad, cel, miesiac, min(cena) AS cena FROM oferty "
    "  WHERE skad = %s AND cel = ANY(%s) AND wylot >= current_date GROUP BY skad, cel, miesiac) n "
    "LEFT JOIN LATERAL (SELECT h.cena, h.dzien FROM historia_cen h WHERE h.skad = n.skad AND h.cel = n.cel "
    "  AND h.miesiac = n.miesiac ORDER BY h.dzien DESC LIMIT 1) p ON true "
    "WHERE p.cena IS DISTINCT FROM n.cena OR p.dzien = current_date "
    "ON CONFLICT (skad, cel, miesiac, dzien) DO UPDATE SET cena = LEAST(historia_cen.cena, EXCLUDED.cena)")


class BladTokenu(Exception):
    pass


def _data(tekst):
    try:
        return date.fromisoformat(str(tekst)[:10])
    except (TypeError, ValueError):
        return None


def zbierz_okazje(sesja, token, baza_url, skady, miasta):
    """Oferty specjalne Travelpayouts (nietypowo niskie ceny) dla każdego lotniska wylotu: jedno zapytanie na lotnisko.
    Błąd jednego lotniska nie przerywa pozostałych; stare okazje danego lotniska zastępujemy nowymi."""
    razem = 0
    for skad in skady:
        params = {"origin": skad, "currency": WALUTA, "token": token}
        if TP_MARKET:
            params["market"] = TP_MARKET
        try:
            r = zapytaj(sesja, URL_OKAZJI, params, proby=2, pauza=15)
        except Exception as e:
            print(f"[okazje] {skad}: {type(e).__name__}, pomijam")
            continue
        if r.status_code in (401, 403):
            raise BladTokenu("Token Travelpayouts odrzucony")
        if r.status_code != 200:
            print(f"[okazje] {skad}: HTTP {r.status_code}, pomijam")
            continue
        try:
            dane = r.json().get("data") or []
        except ValueError:
            dane = []
        wiersze = []
        for o in dane:
            cel, cena, wylot = o.get("destination"), o.get("price"), _data(o.get("departure_at"))
            if not cel or cena is None or wylot is None or wylot < date.today():
                continue
            link = str(o.get("link") or "")
            wiersze.append((skad, miasta.get(cel) or cel, o.get("destination_airport"), o.get("destination_name"),
                            wylot, _data(o.get("return_at")), int(round(cena)), o.get("airline"),
                            str(o.get("title") or "")[:200], link[:500]))
        with psycopg.connect(baza_url) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM okazje WHERE skad = %s", (skad,))
                cur.executemany(
                    "INSERT INTO okazje (skad, cel, cel_lotnisko, cel_nazwa, wylot, powrot, cena, linia, tytul, link) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", wiersze)
        razem += len(wiersze)
        time.sleep(0.3)
    print(f"[okazje] zapisano {razem} ofert specjalnych dla {len(skady)} lotnisk")


def nowa_sesja():
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=Retry(
        total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False)))     # po wyczerpaniu ponowień zwraca odpowiedź zamiast rzucać wyjątek
    return s


def zapytaj(sesja, url, params, proby=2, pauza=15):
    """GET z dodatkową, dłuższą pauzą i ponowieniem, gdy serwer nadal odpowiada 429."""
    r = None
    for i in range(proby):
        r = sesja.get(url, params=params, timeout=60)
        if r.status_code != 429:
            return r
        time.sleep(pauza * (i + 1))
    return r


def miesiace_naprzod(n):
    d, wynik = date.today().replace(day=1), []
    for _ in range(n):
        wynik.append(d.strftime("%Y-%m"))
        d = date(d.year + (d.month == 12), d.month % 12 + 1, 1)
    return wynik


def mapa_miast(sesja):
    """Kod lotniska -> kod miasta (jak w aplikacji)."""
    try:
        r = sesja.get("https://api.travelpayouts.com/data/airports.json", timeout=60)
        r.raise_for_status()
        return {a["code"]: a.get("city_code") for a in r.json() if a.get("code")}
    except Exception:
        return {}


def wczytaj_lotniska(sesja):
    r = sesja.get(BAZA_LOTNISK, timeout=60)
    r.raise_for_status()
    wynik = {}
    for w in csv.reader(io.StringIO(r.text)):
        if len(w) > 7 and len(w[4]) == 3:
            try:
                wynik[w[4]] = (float(w[6]), float(w[7]))
            except ValueError:
                pass
    wynik.setdefault("BER", (52.3667, 13.5033))   # Berlin Brandenburg (otwarty w 2020 r., brak w starych danych OpenFlights)
    return wynik


def odkryj(sesja, token, skad, mies):
    """Dla każdego miesiąca: kierunki, dla których baza cen zna oferty {miesiąc: {kod miasta: kod lotniska}}."""
    wynik = {}
    for m in mies:
        r = zapytaj(sesja, URL_CEN, {
            "origin": skad, "departure_at": m, "direct": "false", "unique": "true",
            "sorting": "price", "currency": WALUTA, "limit": 1000, "token": token},
            proby=3, pauza=30)
        if r.status_code in (401, 403):
            raise BladTokenu("Token Travelpayouts odrzucony")
        kody = {}
        if r.status_code == 200:
            for o in r.json().get("data") or []:
                if o.get("destination"):
                    kody.setdefault(o["destination"], o.get("destination_airport"))
        else:
            print(f"[{skad}] odkrywanie {m}: HTTP {r.status_code}, pomijam miesiąc")
        print(f"[{skad}] {m}: znanych kierunków {len(kody)}")
        wynik[m] = kody
        time.sleep(0.5)
    return wynik


def pobierz(sesja, token, skad, cel, m):
    r = zapytaj(sesja, URL_CEN, {
        "origin": skad, "destination": cel, "departure_at": m, "one_way": "false",
        "unique": "false", "sorting": "price", "direct": "false", "currency": WALUTA,
        "limit": 1000, "token": token}, proby=2, pauza=15)
    if r.status_code in (401, 403):
        raise BladTokenu("Token Travelpayouts odrzucony")
    r.raise_for_status()
    wiersze = []
    for o in r.json().get("data") or []:
        if o.get("price") is None:
            continue
        try:
            wylot = date.fromisoformat(str(o["departure_at"])[:10])
            powrot = date.fromisoformat(str(o["return_at"])[:10])
        except (KeyError, ValueError):
            continue
        wiersze.append((skad, cel, o.get("destination_airport"), m, wylot, powrot,
                        int(round(o["price"])), o.get("transfers"), o.get("return_transfers"),
                        o.get("airline")))
    time.sleep(0.1)
    return wiersze


def wykonaj(sesja, token, zadania, watki, funkcja=None):
    """Zwraca ({zadanie: lista ofert}, [nieudane zadania])."""
    funkcja = funkcja or pobierz
    wyniki, nieudane = {}, []
    with ThreadPoolExecutor(max_workers=watki) as ex:
        przyszle = {ex.submit(funkcja, sesja, token, *z): z for z in zadania}
        for f in as_completed(przyszle):
            z = przyszle[f]
            try:
                wyniki[z] = f.result()
            except BladTokenu as e:
                sys.exit(str(e))
            except Exception:
                nieudane.append(z)
    return wyniki, nieudane


def agreguj(d):
    """Średnie klimatyczne dla każdego dnia roku (klucz: miesiąc*100 + dzień)."""
    agreg = {}
    for dzien, tx, p in zip(d["time"], d["temperature_2m_max"], d["precipitation_sum"]):
        md = int(dzien[5:7]) * 100 + int(dzien[8:10])
        a = agreg.setdefault(md, ([], []))
        if tx is not None:
            a[0].append(tx)
        if p is not None:
            a[1].append(p)
    return [(md, sum(tx) / len(tx) if tx else None, None,
             sum(1 for x in p if x > 1) / len(p) if p else None)
            for md, (tx, p) in agreg.items()]


def uzupelnij_klimat(sesja, baza_url):
    lotniska = wczytaj_lotniska(sesja)
    with psycopg.connect(baza_url) as conn:
        popularne = [r[0] for r in conn.execute(
            "SELECT cel_lotnisko FROM oferty WHERE cel_lotnisko IS NOT NULL "
            "GROUP BY cel_lotnisko ORDER BY COUNT(*) DESC").fetchall()]
        ma = {r[0] for r in conn.execute("SELECT DISTINCT lotnisko FROM klimat").fetchall()}
    kolejka = [k for k in DODATKOWE + popularne if k in lotniska and k not in ma]
    kolejka = list(dict.fromkeys(kolejka))          # bez duplikatów, z zachowaniem kolejności
    print(f"Do uzupełnienia: {len(kolejka)} lotnisk (najpopularniejsze pierwsze).")
    rok = date.today().year
    zrobione = 0
    with psycopg.connect(baza_url, autocommit=True) as conn:
        for kod in kolejka:
            lat, lon = lotniska[kod]
            dane = None
            for proba in range(6):
                r = sesja.get("https://archive-api.open-meteo.com/v1/archive", params={
                    "latitude": lat, "longitude": lon, "start_date": f"{rok - 3}-01-01",
                    "end_date": f"{rok - 1}-12-31", "timezone": "auto",
                    "daily": "temperature_2m_max,precipitation_sum"}, timeout=60)
                if r.status_code == 429:
                    pauza = 60 * (proba + 1)
                    print(f"Limit Open-Meteo, czekam {pauza} s...")
                    time.sleep(pauza)
                    continue
                r.raise_for_status()
                dane = r.json()["daily"]
                break
            if dane is None:
                print("Limit Open-Meteo nie ustępuje. Uruchom skrypt ponownie później "
                      "(postęp jest zapisany, kolejne uruchomienie kontynuuje).")
                return
            conn.cursor().executemany(
                "INSERT INTO klimat (lotnisko, md, tmax, tmin, deszcz) VALUES (%s,%s,%s,%s,%s) "
                "ON CONFLICT (lotnisko, md) DO UPDATE SET tmax=EXCLUDED.tmax, "
                "tmin=EXCLUDED.tmin, deszcz=EXCLUDED.deszcz",
                [(kod,) + w for w in agreguj(dane)])
            zrobione += 1
            print(f"[{zrobione}/{len(kolejka)}] {kod}")
            time.sleep(2)
    print("Klimat uzupełniony dla wszystkich lotnisk z kolejki.")


def normalizuj_skady(skady, miasta):
    """Zamienia kody lotnisk na kody miast (LHR, LGW, STN -> LON) i usuwa duplikaty.
    Aplikacja i wyszukiwarka łączą ceny po kodzie miasta, a zapytanie o miasto obejmuje wszystkie jego lotniska."""
    wynik = []
    for kod in skady:
        miasto = miasta.get(kod) or kod
        if miasto != kod:
            print(f"Uwaga: {kod} to lotnisko miasta {miasto}, zbieram pod kodem miasta {miasto}.")
        if miasto not in wynik:
            wynik.append(miasto)
    return wynik


# ---------------- LOTY JEDNOKIERUNKOWE (tryb: wylot z jednego lotniska, powrót na inne) ----------------
ODSWIEZ_JEDN_H = float(os.environ.get("HARVEST_JEDN_ODSWIEZ_H") or 48)    # po tylu godzinach ponawiamy zebrany odcinek
UDZIAL_JEDN = float(os.environ.get("HARVEST_JEDN_UDZIAL") or 0.3)         # jaka część budżetu czasu idzie na loty jednokierunkowe
MAX_PAR_JEDN = int(os.environ.get("OPENJAW_MAX_PAR") or 12)               # maks. liczba par zgłoszonych przez aplikację
MIESIECY_JEDN = int(os.environ.get("HARVEST_JEDN_MIESIECY") or 6)         # na ile miesięcy do przodu zbieramy loty jednokierunkowe
TABELE_JEDN = [
    "CREATE TABLE IF NOT EXISTS oferty_jedn (skad text NOT NULL, cel text NOT NULL, skad_lot text, cel_lot text, "
    "miesiac text NOT NULL, dzien date NOT NULL, cena int NOT NULL, przesiadki int, linia text, "
    "pobrano timestamptz NOT NULL DEFAULT now())",
    "CREATE INDEX IF NOT EXISTS oferty_jedn_idx ON oferty_jedn (skad, cel, miesiac)",
    "CREATE TABLE IF NOT EXISTS harvest_jedn (skad text NOT NULL, cel text NOT NULL, miesiac text NOT NULL, "
    "pobrano timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (skad, cel, miesiac))",
    "CREATE TABLE IF NOT EXISTS openjaw_pary (skad text NOT NULL, powrot text NOT NULL, "
    "prosba timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (skad, powrot))",
]


def pobierz_jedn(sesja, token, skad, cel, m):
    """Najtańsze loty w jedną stronę skad -> cel w danym miesiącu. Zwraca wiersze do tabeli oferty_jedn."""
    r = zapytaj(sesja, URL_CEN, {
        "origin": skad, "destination": cel, "departure_at": m, "one_way": "true",
        "unique": "false", "sorting": "price", "direct": "false", "currency": WALUTA,
        "limit": 1000, "token": token}, proby=2, pauza=15)
    if r.status_code in (401, 403):
        raise BladTokenu("Token Travelpayouts odrzucony")
    r.raise_for_status()
    najlepsze = {}
    for o in r.json().get("data") or []:
        if o.get("price") is None:
            continue
        try:
            dzien = date.fromisoformat(str(o["departure_at"])[:10])
        except (KeyError, ValueError):
            continue
        klucz = (dzien, o.get("origin_airport"), o.get("destination_airport"), o.get("transfers") or 0)
        cena = int(round(o["price"]))
        if klucz not in najlepsze or cena < najlepsze[klucz][0]:
            najlepsze[klucz] = (cena, o.get("airline"))
    time.sleep(0.1)
    return [(skad, cel, k[1], k[2], m, k[0], c, k[3], linia) for k, (c, linia) in najlepsze.items()]


def pary_open_jaw(conn, miasta, skady):
    """([wszystkie pary], [pary priorytetowe]) jako (skad, powrot) w kodach miast.
    Wszystkie pary: każde miasto z HARVEST_SKAD jako wylot i jako powrót (n * (n-1) par). Priorytetowe: z zapytań
    zgłoszonych przez aplikację (tabela openjaw_pary) oraz z opcjonalnej zmiennej HARVEST_OPENJAW ("WAW>KTW,...")."""
    priorytet = []
    for a, b in conn.execute("SELECT skad, powrot FROM openjaw_pary ORDER BY prosba DESC LIMIT %s",
                             (MAX_PAR_JEDN,)).fetchall():
        priorytet.append((a, b))
    for p in (os.environ.get("HARVEST_OPENJAW") or "").split(","):
        if ">" in p:
            a, b = (x.strip().upper() for x in p.split(">", 1))
            if len(a) == 3 and len(b) == 3:
                priorytet.append((miasta.get(a) or a, miasta.get(b) or b))
    wszystkie = [(a, b) for a in skady for b in skady if a != b]
    priorytet = [p for p in dict.fromkeys(priorytet) if p[0] != p[1]]
    return list(dict.fromkeys(priorytet + wszystkie)), priorytet


def zbierz_jednokierunkowe(sesja, token, baza_url, miasta, mies, koniec, skady=()):
    """Zbiera odcinki skad->cel i cel->powrot dla wszystkich par miast z HARVEST_SKAD oraz par zgłoszonych przez
    aplikację. Odcinki nie zależą od pary (skad->cel liczy się raz), więc koszt rośnie liniowo z liczbą lotnisk.
    Kolejność: odcinki par z aplikacji, potem nigdy niepobrane, potem najstarsze. Wznawialne; kończy po `koniec`."""
    mies = list(mies)[:MIESIECY_JEDN]
    with psycopg.connect(baza_url) as conn:
        for sql in TABELE_JEDN:
            conn.execute(sql)
        pary, priorytet = pary_open_jaw(conn, miasta, list(skady))
        if not pary:
            print("[jednokierunkowe] brak lotnisk w HARVEST_SKAD (potrzeba co najmniej dwóch) i brak zgłoszonych par.")
            return
        odcinki, odcinki_prio = set(), set()
        cele_z = {}
        for a in {p[0] for p in pary}:
            cele_z[a] = [r[0] for r in conn.execute(
                "SELECT DISTINCT cel FROM oferty WHERE skad=%s AND zrodlo='tp'", (a,)).fetchall()]
        for a, b in pary:
            for x in cele_z.get(a, []):
                if x in (a, b):
                    continue
                for m in mies:
                    odcinki.add((a, x, m))
                    odcinki.add((x, b, m))
                    if (a, b) in priorytet:
                        odcinki_prio.update({(a, x, m), (x, b, m)})
        pobrano = {(s, c, m): t.timestamp() for s, c, m, t in conn.execute(
            "SELECT skad, cel, miesiac, pobrano FROM harvest_jedn").fetchall()}
    prog = time.time() - ODSWIEZ_JEDN_H * 3600
    do_zrobienia = sorted((o for o in odcinki if pobrano.get(o, 0) < prog),
                          key=lambda o: (o not in odcinki_prio, pobrano.get(o, 0), o))
    print(f"[jednokierunkowe] pary: {len(pary)} (z aplikacji lub HARVEST_OPENJAW: {len(priorytet)}), "
          f"miesięcy: {len(mies)}, odcinków: {len(odcinki)}, do pobrania: {len(do_zrobienia)}")
    zrobione = bledy = 0
    for i in range(0, len(do_zrobienia), PACZKA):
        if time.time() > koniec - MARGINES_PACZKI:
            print("[jednokierunkowe] budżet czasu wyczerpany, resztę pobiorę w następnym uruchomieniu.")
            break
        paczka = do_zrobienia[i:i + PACZKA]
        wyniki, nieudane = wykonaj(sesja, token, paczka, WATKI, pobierz_jedn)
        bledy += len(nieudane)
        with psycopg.connect(baza_url) as conn:
            with conn.cursor() as cur:
                cur.executemany("DELETE FROM oferty_jedn WHERE skad=%s AND cel=%s AND miesiac=%s", list(wyniki))
                cur.executemany(
                    "INSERT INTO oferty_jedn (skad, cel, skad_lot, cel_lot, miesiac, dzien, cena, przesiadki, linia) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", [w for l in wyniki.values() for w in l])
                cur.executemany(
                    "INSERT INTO harvest_jedn (skad, cel, miesiac, pobrano) VALUES (%s,%s,%s, now()) "
                    "ON CONFLICT (skad, cel, miesiac) DO UPDATE SET pobrano = now()", list(wyniki))
                cur.execute("DELETE FROM oferty_jedn WHERE dzien < current_date")
        zrobione += len(wyniki)
        print(f"[jednokierunkowe] zapisano {zrobione}/{len(do_zrobienia)} (błędów: {bledy})")


def main():
    baza_url = os.environ.get("DATABASE_URL", "")
    if not baza_url:
        sys.exit("Brak DATABASE_URL.")
    sesja = nowa_sesja()
    if "--klimat" in sys.argv:
        uzupelnij_klimat(sesja, baza_url)
        return

    token = os.environ.get("TRAVELPAYOUTS_TOKEN", "")
    if not token:
        sys.exit("Brak TRAVELPAYOUTS_TOKEN.")
    skady = [s.strip().upper() for s in (os.environ.get("HARVEST_SKAD") or "WAW").split(",")
             if s.strip()]
    miasta = mapa_miast(sesja)
    skady = normalizuj_skady(skady, miasta)
    mies = miesiace_naprzod(MIESIECY_NAPRZOD)
    dodatkowe = {miasta.get(x) or x for x in DODATKOWE}

    # Budżet czasu: GitHub Actions przerywa zadanie po limicie, więc kończymy sami i zapisujemy postęp.
    # Kolejne uruchomienie wznawia pracę dokładnie tam, gdzie skończyło to poprzednie.
    koniec_budzetu = time.time() + BUDZET_MIN * 60
    with psycopg.connect(baza_url) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS harvest_pary (skad text NOT NULL, cel text NOT NULL, "
                     "miesiac text NOT NULL, PRIMARY KEY (skad, cel, miesiac))")
        # kolumny dla drugiego źródła cen (Google Flights, patrz gf_zbieraj.py); harvest rusza tylko wiersze "tp"
        conn.execute("ALTER TABLE oferty ADD COLUMN IF NOT EXISTS zrodlo text NOT NULL DEFAULT 'tp'")
        conn.execute("ALTER TABLE oferty ADD COLUMN IF NOT EXISTS pobrano timestamptz NOT NULL DEFAULT now()")
        for sql in TABELE_CEN:                      # historia cen i oferty specjalne
            conn.execute(sql)
        conn.execute("DELETE FROM historia_cen WHERE dzien < current_date - %s::int "
                     "OR miesiac < to_char(current_date, 'YYYY-MM')", (HISTORIA_DNI,))
        log = dict(conn.execute("SELECT skad, koniec FROM harvest_log").fetchall())
    # najpierw lotniska nigdy niezebrane, potem te z najstarszymi danymi (rozpoczęte, a niedokończone, są wśród najstarszych)
    skady.sort(key=lambda x: (log.get(x) is not None, log[x].timestamp() if log.get(x) else 0))
    print(f"Kolejność lotnisk ({len(skady)}): {', '.join(skady)}. Budżet czasu: {BUDZET_MIN:g} min.")
    try:        # loty jednokierunkowe (tryb z powrotem na inne lotnisko): własny, ograniczony udział czasu
        zbierz_jednokierunkowe(sesja, token, baza_url, miasta, mies,
                               min(koniec_budzetu, time.time() + UDZIAL_JEDN * BUDZET_MIN * 60), skady)
    except SystemExit:
        raise
    except Exception as e:
        print(f"[jednokierunkowe] pominięto z powodu błędu: {type(e).__name__}: {e}")

    if OKAZJE_WLACZONE:
        try:        # oferty specjalne: jedno zapytanie na lotnisko, nie obciąża budżetu czasu
            zbierz_okazje(sesja, token, baza_url, skady, miasta)
        except BladTokenu as e:
            sys.exit(str(e))
        except Exception as e:
            print(f"[okazje] pominięto z powodu błędu: {type(e).__name__}: {e}")

    for skad in skady:
        if time.time() > koniec_budzetu - MARGINES_LOTNISKA:
            print(f"Budżet czasu na wyczerpaniu, pozostałe lotniska zbiorę w następnym uruchomieniu "
                  f"(od {skad}).")
            break
        t0 = time.time()
        try:
            odkryte = odkryj(sesja, token, skad, mies)
        except BladTokenu as e:
            sys.exit(str(e))
        cele = set(dodatkowe)
        zadania = {(skad, cel, m) for cel in dodatkowe for m in mies}
        for m, kody in odkryte.items():             # pytamy tylko o pary kierunek-miesiąc, które baza zna
            cele |= set(kody)
            zadania |= {(skad, cel, m) for cel in kody}
        zadania = sorted(zadania)
        with psycopg.connect(baza_url) as conn:
            gotowe = set(conn.execute("SELECT skad, cel, miesiac FROM harvest_pary WHERE skad=%s",
                                      (skad,)).fetchall())
        do_zrobienia = [z for z in zadania if z not in gotowe]
        print(f"[{skad}] kierunków: {len(cele)}, zapytań: {len(zadania)}, "
              f"do zrobienia: {len(do_zrobienia)} (wznawiam: {len(gotowe)} gotowych)")

        bledy, przerwano = 0, False
        for i in range(0, len(do_zrobienia), PACZKA):
            if time.time() > koniec_budzetu - MARGINES_PACZKI:
                przerwano = True
                break
            paczka = do_zrobienia[i:i + PACZKA]
            wyniki, nieudane = wykonaj(sesja, token, paczka, WATKI)
            if nieudane:
                time.sleep(15)
                wyniki2, nieudane = wykonaj(sesja, token, nieudane, 2)
                wyniki.update(wyniki2)
            bledy += len(nieudane)
            wiersze = [w for lista in wyniki.values() for w in lista]
            # połączenie z bazą otwieramy dopiero teraz, żeby nie trzymać jej "obudzonej" podczas pobierania
            with psycopg.connect(baza_url) as conn:
                with conn.cursor() as cur:
                    cur.executemany("DELETE FROM oferty WHERE skad=%s AND cel=%s AND miesiac=%s AND zrodlo = 'tp'",
                                    list(wyniki))
                    cur.executemany(
                        "INSERT INTO oferty (skad, cel, cel_lotnisko, miesiac, wylot, powrot, cena, "
                        "przesiadki, przesiadki_powrot, linia) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        wiersze)
                    cur.executemany("INSERT INTO harvest_pary (skad, cel, miesiac) VALUES (%s,%s,%s) "
                                    "ON CONFLICT DO NOTHING", list(wyniki))
                    cur.execute("DELETE FROM oferty WHERE wylot < current_date")
                    # historia cen: najniższa dzisiejsza cena dla kierunków z tej paczki (wszystkie źródła cen)
                    if wyniki:
                        cur.execute(SQL_HISTORIA, (skad, sorted({z[1] for z in wyniki})))
            print(f"[{skad}] zapisano {min(i + PACZKA, len(do_zrobienia))}/{len(do_zrobienia)} "
                  f"(błędów: {bledy}, {time.time() - t0:.0f} s)")

        if przerwano:
            print(f"[{skad}] przerwano z powodu budżetu czasu; postęp zapisany, wznowię od tego miejsca "
                  f"w następnym uruchomieniu.")
            break
        # lotnisko ukończone: kończymy cykl i czyścimy znaczniki postępu
        with psycopg.connect(baza_url) as conn:
            with conn.cursor() as cur:
                if bledy <= 0.2 * max(1, len(zadania)):
                    cur.execute("SELECT count(*) FROM oferty WHERE skad=%s", (skad,))
                    ofert = cur.fetchone()[0]
                    cur.execute(
                        "INSERT INTO harvest_log (skad, horyzont, koniec, ofert, kierunkow) "
                        "VALUES (%s,%s,now(),%s,%s) ON CONFLICT (skad) DO UPDATE SET "
                        "horyzont=EXCLUDED.horyzont, koniec=EXCLUDED.koniec, "
                        "ofert=EXCLUDED.ofert, kierunkow=EXCLUDED.kierunkow",
                        (skad, mies[-1], ofert, len(cele)))
                cur.execute("DELETE FROM harvest_pary WHERE skad=%s", (skad,))
        print(f"[{skad}] gotowe, błędów: {bledy}, czas: {time.time() - t0:.0f} s")


if __name__ == "__main__":
    main()
