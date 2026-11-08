"""gf.py: odczyt cen z Google Flights przez Playwright (przeglądarka bez interfejsu).
Jedno zapytanie = jedna trasa i daty. Nie obchodzi CAPTCHA ani blokad: przy blokadzie zgłasza Blokada."""
import time

from gf_test import adres, parsuj_karte


class Blokada(Exception):
    """Google pokazał CAPTCHA albo komunikat o nietypowym ruchu."""


class Przegladarka:
    def __init__(self, pokaz=False):
        self.pokaz = pokaz

    def __enter__(self):
        from playwright.sync_api import sync_playwright
        self.pw = sync_playwright().start()
        self.b = self.pw.chromium.launch(headless=not self.pokaz)
        self.ctx = self.b.new_context(locale="en-US", timezone_id="Europe/Warsaw",
                                      viewport={"width": 1280, "height": 1600})
        self.strona = self.ctx.new_page()
        return self

    def __exit__(self, *a):
        try:
            self.b.close()
        finally:
            self.pw.stop()

    def karty(self, skad, cel, od, do, url=None):
        """Lista kart wyników {"cena", "przesiadki", "tekst"} dla trasy i dat (tam i z powrotem)."""
        import re
        s = self.strona
        s.goto(url or adres(skad, cel, od, do), wait_until="domcontentloaded", timeout=45000)
        try:                                                 # baner zgody (UE): zwykłe "Reject all"
            p = s.get_by_role("button", name=re.compile(r"reject all|odrzuć wszystko", re.I)).first
            p.wait_for(timeout=4000)
            p.click()
            s.wait_for_load_state("domcontentloaded")
        except Exception:
            pass
        tresc = ""
        try:
            tresc = s.inner_text("body", timeout=3000).lower()
        except Exception:
            pass
        if "sorry" in s.url or "unusual traffic" in tresc or "captcha" in tresc:
            raise Blokada("Google pokazał blokadę")
        try:
            s.wait_for_selector("li", timeout=25000)
        except Exception:
            pass
        time.sleep(4)                                        # wyniki doładowują się po chwili
        wynik = []
        for el in s.locator("li").all():
            try:
                tekst = (el.get_attribute("aria-label") or "") + " " + el.inner_text(timeout=2000)
            except Exception:
                continue
            k = parsuj_karte(tekst)
            if k["cena"] and k["cena"] >= 50:                # odsiewamy śmieci (np. fragmenty menu)
                wynik.append(k)
        return wynik


def najtansze(karty):
    """(najtańsza cena, najtańsza bezpośrednia albo None, przesiadki najtańszej, tekst najtańszej)."""
    if not karty:
        return None
    n = min(karty, key=lambda k: k["cena"])
    bezp = [k["cena"] for k in karty if k["przesiadki"] == 0]
    return n["cena"], (min(bezp) if bezp else None), n["przesiadki"], n["tekst"]
