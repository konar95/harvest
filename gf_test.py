"""Test: czy da się odczytać ceny z Google Flights (Playwright). Uruchamiaj u siebie, pojedynczymi zapytaniami.
  pip install playwright
  python -m playwright install chromium
  python gf_test.py --skad WAW --cel FCO --od 2027-05-10 --do 2027-05-17
Opcje: --pokaz (widoczne okno przeglądarki), --dump (zapisz zrzut i HTML także przy sukcesie).
Skrypt nie obchodzi CAPTCHA ani blokad. Gdy Google je pokaże, kończy z komunikatem."""
import argparse
import re
import sys
import time
from urllib.parse import quote

CENA_RE = [
    re.compile(r"From\s+([\d.,\s\u00a0\u202f]+?)\s+Polish zlot", re.I),   # aria-label: "From 543 Polish zlotys round trip total"
    re.compile(r"(\d[\d.,\u00a0\u202f ]*?)\s?(?:PLN|zł)\b", re.I),          # "543 zł" / "1 234 PLN"
    re.compile(r"(?:PLN|zł)\s?(\d[\d.,\u00a0\u202f]*)", re.I),               # "PLN 543" / "zł543" / "PLN 1,234.50"
]


def cena_z_tekstu(tekst):
    """Pierwsza cena w PLN znaleziona w tekście karty wyniku (liczba całkowita) albo None."""
    for wz in CENA_RE:
        m = wz.search(tekst)
        if m:
            liczba = re.sub(r"[\s\u00a0\u202f]", "", m.group(1)).strip(".,")
            if re.search(r"[.,]\d{2}$", liczba):          # grosze: odcinamy
                liczba = liczba[:-3]
            cyfry = re.sub(r"\D", "", liczba)
            if cyfry:
                return int(cyfry)
    return None


def parsuj_karte(tekst):
    t = " ".join(tekst.split())
    low = t.lower()
    przesiadki = 0 if ("nonstop" in low or "bez przesiadek" in low) else None
    m = re.search(r"(\d)\s+stops?\b|(\d)\s+przesiad", low)
    if m:
        przesiadki = int(m.group(1) or m.group(2))
    return {"cena": cena_z_tekstu(t), "przesiadki": przesiadki, "tekst": t[:220]}


def adres(skad, cel, od, do):
    q = f"Flights to {cel} from {skad} on {od} through {do}"
    return f"https://www.google.com/travel/flights?q={quote(q)}&curr=PLN&hl=en&gl=PL"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skad", required=True)
    ap.add_argument("--cel", required=True)
    ap.add_argument("--od", required=True)
    ap.add_argument("--do", required=True)
    ap.add_argument("--pokaz", action="store_true")
    ap.add_argument("--dump", action="store_true")
    ap.add_argument("--url", help="(do testów) własny adres zamiast Google")
    a = ap.parse_args()
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        przegladarka = p.chromium.launch(headless=not a.pokaz)
        ctx = przegladarka.new_context(locale="en-US", timezone_id="Europe/Warsaw",
                                       viewport={"width": 1280, "height": 1600})
        strona = ctx.new_page()
        strona.goto(a.url or adres(a.skad, a.cel, a.od, a.do), wait_until="domcontentloaded", timeout=45000)
        # strona zgody na pliki cookie (UE): zwykłe kliknięcie "Reject all"
        try:
            przycisk = strona.get_by_role("button", name=re.compile(r"reject all|odrzuć wszystko", re.I)).first
            przycisk.wait_for(timeout=6000)
            przycisk.click()
            strona.wait_for_load_state("domcontentloaded")
        except Exception:
            pass
        if "sorry" in strona.url or strona.locator("text=unusual traffic").count():
            print("Google pokazał blokadę (CAPTCHA / nietypowy ruch). Kończę, nic nie obchodzę.")
            przegladarka.close()
            sys.exit(2)
        try:
            strona.wait_for_selector("li", timeout=25000)
        except Exception:
            pass
        time.sleep(4)                                    # wyniki doładowują się po chwili
        karty = []
        for el in strona.locator("li").all():
            try:
                tekst = (el.get_attribute("aria-label") or "") + " " + el.inner_text(timeout=2000)
            except Exception:
                continue
            k = parsuj_karte(tekst)
            if k["cena"]:
                karty.append(k)
        if a.dump or not karty:
            strona.screenshot(path="gf_debug.png", full_page=True)
            open("gf_debug.html", "w", encoding="utf-8").write(strona.content())
            print("Zapisano gf_debug.png i gf_debug.html")
        przegladarka.close()

    unikalne = {}
    for k in karty:
        unikalne.setdefault((k["cena"], k["tekst"][:60]), k)
    wynik = sorted(unikalne.values(), key=lambda k: k["cena"])
    print(f"Znaleziono wyników z ceną: {len(wynik)}")
    for k in wynik[:10]:
        print(f"  {k['cena']} PLN | przesiadki: {k['przesiadki']} | {k['tekst'][:150]}")
    if not wynik:
        print("Brak wyników: wyślij mi gf_debug.png i gf_debug.html (bez danych osobistych), poprawię odczyt.")


if __name__ == "__main__":
    main()
