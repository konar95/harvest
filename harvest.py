"""Harvester: pobiera oferty lotów (Travelpayouts) i klimat (Open-Meteo) do bazy Postgres.

python harvest.py            - zbiera ceny (uruchamia GitHub Actions)
python harvest.py --klimat   - uzupełnia klimat lotnisk (uruchamiaj lokalnie; można przerwać i wznowić)
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
DODATKOWE = ["TOS", "RVN", "KRN", "KEF", "ALF", "KTT", "IVL", "LYR"]


class BladTokenu(Exception):
    pass


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


def wykonaj(sesja, token, zadania, watki):
    """Zwraca ({zadanie: lista ofert}, [nieudane zadania])."""
    wyniki, nieudane = {}, []
    with ThreadPoolExecutor(max_workers=watki) as ex:
        przyszle = {ex.submit(pobierz, sesja, token, *z): z for z in zadania}
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
        log = dict(conn.execute("SELECT skad, koniec FROM harvest_log").fetchall())
    # najpierw lotniska nigdy niezebrane, potem te z najstarszymi danymi (rozpoczęte, a niedokończone, są wśród najstarszych)
    skady.sort(key=lambda x: (log.get(x) is not None, log[x].timestamp() if log.get(x) else 0))
    print(f"Kolejność lotnisk ({len(skady)}): {', '.join(skady)}. Budżet czasu: {BUDZET_MIN:g} min.")

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
                    cur.executemany("DELETE FROM oferty WHERE skad=%s AND cel=%s AND miesiac=%s",
                                    list(wyniki))
                    cur.executemany(
                        "INSERT INTO oferty (skad, cel, cel_lotnisko, miesiac, wylot, powrot, cena, "
                        "przesiadki, przesiadki_powrot, linia) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        wiersze)
                    cur.executemany("INSERT INTO harvest_pary (skad, cel, miesiac) VALUES (%s,%s,%s) "
                                    "ON CONFLICT DO NOTHING", list(wyniki))
                    cur.execute("DELETE FROM oferty WHERE wylot < current_date")
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
