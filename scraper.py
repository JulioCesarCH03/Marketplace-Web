import re
import time
import requests
import pandas as pd
from typing import Optional
from bs4 import BeautifulSoup
import logging
import uuid
from datetime import datetime

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

COMMON_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
             "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")

# -------------------- Helpers --------------------
def get_page_content(url, headers=None, timeout=15):
    """
    Obtiene el contenido HTML de una URL usando requests.
    """
    if headers is None:
        headers = {"User-Agent": COMMON_UA}
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        logger.warning(f"Error al obtener {url}: {e}")
        return None

def slugify_zone(zona: str) -> str:
    if not zona:
        return ""
    s = zona.lower().strip()
    trans = str.maketrans("áéíóúñü", "aeiounu")
    s = s.translate(trans)
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"[^a-z0-9\-]", "", s)
    return s

def parse_precio_con_moneda(precio_str):
    if not precio_str:
        return (None, None)
    s = str(precio_str)
    moneda = None
    if "S/" in s or s.strip().startswith("S/"):
        moneda = "S"
    elif "$" in s:
        moneda = "USD"
    nums = re.sub(r"[^\d]", "", s)
    return (moneda, int(nums)) if nums else (moneda, None)

def _extract_int_from_text(s):
    if s is None:
        return None
    text = str(s).strip()
    text = re.sub(r'\s+', ' ', text)
    m = re.search(r'(\d+)', text)
    return int(m.group(1)) if m else None

# -------------------- Nestoria --------------------
EXCEPCIONES = ["miraflores", "tarapoto", "la molina", "magdalena", "lambayeque", "ventanilla", "la victoria"]

def build_zona_slug_nestoria(zona_input: str) -> str:
    if not zona_input or not zona_input.strip():
        return "lima"
    z = zona_input.strip().lower().replace(" ", "-")
    if z not in [e.lower() for e in EXCEPCIONES]:
        return z
    else:
        return "lima_" + z

def scrape_nestoria(zona: str = "", dormitorios: str = "0", banos: str = "0",
                    price_min: Optional[int] = None, price_max: Optional[int] = None,
                    palabras_clave: str = "", max_results_per_zone: int = 200):
    zona_slug = build_zona_slug_nestoria(zona)
    base_url = f"https://www.nestoria.pe/{zona_slug}/inmuebles/alquiler"
    if dormitorios and dormitorios != "0":
        base_url += f"/dormitorios-{dormitorios}"
    params = []
    if banos and banos != "0":
        params.append(f"bathrooms={banos}")
    if price_min and str(price_min) != "0":
        params.append(f"price_min={price_min}")
    if price_max and str(price_max) != "0":
        params.append(f"price_max={price_max}")
    if params:
        base_url += "?" + "&".join(params)
    logger.info(f"URL de Nestoria: {base_url}")

    soup = get_page_content(base_url)
    if not soup:
        return pd.DataFrame()

    results = []
    items = soup.select("li.rating__new") or soup.select("ul#main__listing_res > li")
    if not items:
        items = [li for li in soup.find_all("li") if li.select_one(".result__details__price")]
    if not items:
        items = soup.find_all(["li", "div", "article"], class_=lambda x: x and any(cls in x for cls in ["listing", "result", "property", "item"]))

    seen_links = set()
    for i, li in enumerate(items):
        try:
            a_tag = li.select_one("a.results__link") or li.select_one("a[href]")
            if not a_tag:
                continue
            link = a_tag.get("data-href") or a_tag.get("href") or ""
            if link and link.startswith("/"):
                link = "https://www.nestoria.pe" + link
            if not link or link in seen_links:
                continue

            title_elem = li.select_one(".listing__title__text") or li.select_one(".listing__title") or a_tag
            title = title_elem.get_text(" ", strip=True) if title_elem else a_tag.get_text(" ", strip=True)[:140]

            price_elem = li.select_one(".result__details__price span") or li.select_one(".result__details__price") or li.select_one(".price")
            price_text = price_elem.get_text(" ", strip=True) if price_elem else ""

            moneda, precio_val = parse_precio_con_moneda(price_text)
            if price_max is not None and moneda == "S" and precio_val is not None and precio_val > price_max:
                continue
            if price_min is not None and moneda == "S" and precio_val is not None and precio_val < price_min:
                continue
            if moneda == "USD" and (price_max is not None or price_min is not None):
                continue

            desc_elem = li.select_one(".listing__description") or li.select_one(".result__summary") or None
            desc = desc_elem.get_text(" ", strip=True) if desc_elem else li.get_text(" ", strip=True)[:800]

            text_content = li.get_text(" ", strip=True).lower()
            dormitorios_text = ""
            dorm_match = re.search(r'(\d+)\s*dormitori', text_content, flags=re.I)
            if dorm_match:
                dormitorios_text = dorm_match.group(1)
            banos_text = ""
            banos_match = re.search(r'(\d+)\s*bañ', text_content, flags=re.I)
            if banos_match:
                banos_text = banos_match.group(1)
            m2_text = ""
            m2_match = re.search(r'(\d{1,4})\s*(m²|m2)', text_content, flags=re.I)
            if m2_match:
                m2_text = m2_match.group(1)

            img_url = ""
            img_tag = li.select_one("img")
            if img_tag:
                img_url = img_tag.get("src") or img_tag.get("data-src") or ""
                if img_url and img_url.startswith("//"):
                    img_url = "https:" + img_url
                img_url = img_url.strip()

            results.append({
                "titulo": title,
                "precio": price_text,
                "m2": m2_text,
                "dormitorios": dormitorios_text,
                "baños": banos_text,
                "descripcion": desc,
                "link": link,
                "imagen_url": img_url
            })
            seen_links.add(link)
        except Exception as e:
            logger.warning(f"Error procesando anuncio en Nestoria: {e}")
            continue

    logger.info(f"Procesados {len(results)} anuncios válidos de Nestoria")
    return pd.DataFrame(results)

# -------------------- Infocasas --------------------
def scrape_infocasas(zona: str = "", dormitorios: str = "0", banos: str = "0",
                     price_min: Optional[int] = None, price_max: Optional[int] = None,
                     palabras_clave: str = "", max_scrolls: int = 8):
    ZONA_MAPEO_INFOCASAS = {
        "ancón": "ancon", "ate": "ate", "barranco": "barranco", "breña": "breña",
        "carabayllo": "carabayllo", "chaclacayo": "chaclacayo", "chorrillos": "chorrillos",
        "cieneguilla": "cieneguilla", "comas": "comas", "el agustino": "el-agustino",
        "independencia": "independencia", "jesús maría": "jesus-maria", "la molina": "la-molina",
        "la victoria": "la-victoria", "lima": "lima-cercado", "lince": "lince",
        "los olivos": "los-olivos", "lurigancho": "lurigancho", "lurín": "lurin",
        "magdalena del mar": "magdalena-del-mar", "miraflores": "miraflores",
        "pachacámac": "pachacamac", "pucusana": "pucusana", "pueblo libre": "pueblo-libre",
        "puente piedra": "puente-piedra", "punta hermosa": "punta-hermosa",
        "punta negra": "punta-negra", "rímac": "rimac", "san bartolo": "san-bartolo",
        "san borja": "san-borja", "san isidro": "san-isidro",
        "san juan de lurigancho": "san-juan-de-lurigancho",
        "san juan de miraflores": "san-juan-de-miraflores", "san luis": "san-luis",
        "san martín de porres": "san-martin-de-porres", "san miguel": "san-miguel",
        "santa anita": "santa-anita", "santa maría del mar": "santa-maria-del-mar",
        "santa rosa": "santa-rosa", "santiago de surco": "santiago-de-surco",
        "surquillo": "surquillo", "villa el salvador": "villa-el-salvador",
        "villa maría del triunfo": "villa-maria-del-triunfo"
    }

    if zona and zona.strip():
        zona_lower = zona.strip().lower()
        zone_slug = ZONA_MAPEO_INFOCASAS.get(zona_lower, slugify_zone(zona))
        base = f"https://www.infocasas.com.pe/alquiler/casas-y-departamentos/lima/{zone_slug}"
    else:
        base = "https://www.infocasas.com.pe/alquiler/casas-y-departamentos"

    if dormitorios and dormitorios != "0" and banos and banos != "0" and price_min is not None and price_max is not None:
        base += f"/{dormitorios}-dormitorio/{banos}-bano/desde-{price_min}/hasta-{price_max}?&IDmoneda=6"
    elif dormitorios and dormitorios != "0" and banos and banos != "0":
        base += f"/{dormitorios}-dormitorio/{banos}-bano"
    elif dormitorios and dormitorios != "0":
        base += f"/{dormitorios}-dormitorio"
    elif banos and banos != "0":
        base += f"/{banos}-bano"

    if palabras_clave and palabras_clave.strip():
        if "?" in base:
            base += f"&searchstring={requests.utils.quote(palabras_clave.strip())}"
        else:
            base += f"?searchstring={requests.utils.quote(palabras_clave.strip())}"

    logger.info(f"URL de InfoCasas: {base}")
    soup = get_page_content(base)
    if not soup:
        return pd.DataFrame()

    results = []
    nodes = soup.select("div.listingCard") or soup.select("article")
    for n in nodes:
        try:
            a = n.select_one("a[href]")
            if not a:
                continue
            href = a.get("href") if a else ""
            if href and href.startswith("/"):
                href = "https://www.infocasas.com.pe" + href

            title_elem = n.select_one("h2.lc-title") or n.select_one(".lc-title") or a
            title = title_elem.get_text(" ", strip=True) if title_elem else n.get_text(" ", strip=True)[:250]

            price_elem = n.select_one(".main-price") or n.select_one(".lc-price p") or n.select_one(".property-price-tag p")
            price = price_elem.get_text(" ", strip=True) if price_elem else ""

            typology_items = n.select(".lc-typologyTag__item strong")
            dormitorios_text = ""
            banos_text = ""
            m2_text = ""
            for item in typology_items:
                text = item.get_text().strip()
                if "Dorm" in text:
                    dorm_match = re.search(r'(\d+)', text)
                    if dorm_match:
                        dormitorios_text = dorm_match.group(1)
                elif "Baños" in text or "Baño" in text:
                    banos_match = re.search(r'(\d+)', text)
                    if banos_match:
                        banos_text = banos_match.group(1)
                elif "m²" in text:
                    m2_match = re.search(r'(\d+)', text)
                    if m2_match:
                        m2_text = m2_match.group(1)

            desc_elem = n.select_one(".lc-description") or n.select_one("p")
            desc = desc_elem.get_text(" ", strip=True) if desc_elem else n.get_text(" ", strip=True)[:400]

            img_url = ""
            img_tag = n.select_one(".cardImageGallery .gallery-image img")
            if img_tag:
                img_url = img_tag.get("src") or img_tag.get("data-src") or ""
                if img_url and img_url.startswith("//"):
                    img_url = "https:" + img_url
                img_url = img_url.strip()

            results.append({
                "titulo": title,
                "precio": price,
                "m2": m2_text,
                "dormitorios": dormitorios_text,
                "baños": banos_text,
                "descripcion": desc,
                "link": href or "",
                "imagen_url": img_url
            })
        except Exception as e:
            logger.warning(f"Error procesando anuncio en InfoCasas: {e}")
            continue

    return pd.DataFrame(results)


# -------------------- Properati --------------------
def scrape_properati(zona: str = "", dormitorios: str = "0", banos: str = "0",
                     price_min: Optional[int] = None, price_max: Optional[int] = None,
                     palabras_clave: str = ""):
    ZONA_MAPEO_PROPERATI = {
        "ancón": "ancon", "ate": "ate", "barranco": "barranco", "breña": "brena",
        "carabayllo": "carabayllo", "chaclacayo": "chaclacayo", "chorrillos": "chorrillos",
        "cieneguilla": "cieneguilla", "comas": "comas", "el agustino": "el-agustino",
        "independencia": "independencia", "jesús maría": "jesus-maria", "la molina": "la-molina",
        "la victoria": "la-victoria", "lima": "lima", "lince": "lince",
        "los olivos": "los-olivos", "lurigancho": "lurigancho", "lurín": "lurin",
        "magdalena del mar": "magdalena-del-mar", "miraflores": "miraflores",
        "pachacámac": "pachacamac", "pucusana": "pucusana", "pueblo libre": "pueblo-libre",
        "puente piedra": "puente-piedra", "punta hermosa": "punta-hermosa",
        "punta negra": "punta-negra", "rímac": "rimac", "san bartolo": "san-bartolo",
        "san borja": "san-borja", "san isidro": "san-isidro",
        "san juan de lurigancho": "san-juan-de-lurigancho",
        "san juan de miraflores": "san-juan-de-miraflores", "san luis": "san-luis",
        "san martín de porres": "san-martin-de-porres", "san miguel": "san-miguel",
        "santa anita": "santa-anita", "santa maría del mar": "santa-maria-del-mar",
        "santa rosa": "santa-rosa", "santiago de surco": "santiago-de-surco",
        "surquillo": "surquillo", "villa el salvador": "villa-el-salvador",
        "villa maría del triunfo": "villa-maria-del-triunfo"
    }

    if zona and zona.strip():
        zona_lower = zona.strip().lower()
        zone_slug = ZONA_MAPEO_PROPERATI.get(zona_lower, slugify_zone(zona))
        base = f"https://www.properati.com.pe/s/{zone_slug}/alquiler?propertyType=apartment%2Chouse"
    else:
        base = "https://www.properati.com.pe/s/alquiler?propertyType=apartment%2Chouse"

    params = []
    if dormitorios and dormitorios != "0":
        params.append(f"bedrooms={dormitorios}")
    if banos and banos != "0":
        params.append(f"bathrooms={banos}")
    if price_min is not None:
        params.append(f"minPrice={price_min}")
    if price_max is not None:
        params.append(f"maxPrice={price_max}")

    if palabras_clave and palabras_clave.strip():
        palabras = palabras_clave.lower().split()
        amenities = []
        other_keywords = []
        for p in palabras:
            if p == "piscina":
                amenities.append("swimming_pool")
            elif p == "jardin":
                amenities.append("garden")
            else:
                other_keywords.append(p)
        if amenities:
            base += "&amenities=" + ",".join(amenities)
        if other_keywords:
            base += "&keyword=" + requests.utils.quote(" ".join(other_keywords))

    if params:
        base += "&" + "&".join(params)

    logger.info(f"URL de Properati: {base}")
    try:
        r = requests.get(base, headers={"User-Agent": COMMON_UA}, timeout=15)
        r.raise_for_status()
    except Exception as e:
        logger.error(f"Error en Properati al hacer la petición: {e}")
        return pd.DataFrame()

    soup = BeautifulSoup(r.text, "html.parser")
    cards = soup.select("article") or soup.select("div.posting-card") or soup.select("a[href]")
    results = []
    for c in cards:
        try:
            a = c.select_one("a[href]") or c.select_one("a.title")
            href = a.get("href") if a else ""
            if href and href.startswith("/"):
                href = "https://www.properati.com.pe" + href
            title = a.get_text(" ", strip=True) if a else c.get_text(" ", strip=True)[:140]

            price_elem = c.select_one(".price")
            price = price_elem.get_text(" ", strip=True) if price_elem else ""

            dormitorios_text = ""
            dorm_elem = c.select_one(".properties__bedrooms")
            if dorm_elem:
                dorm_text = dorm_elem.get_text(" ", strip=True)
                dorm_match = re.search(r'(\d+)', dorm_text)
                if dorm_match:
                    dormitorios_text = dorm_match.group(1)

            banos_text = ""
            banos_elem = c.select_one(".properties__bathrooms")
            if banos_elem:
                banos_text_full = banos_elem.get_text(" ", strip=True)
                banos_match = re.search(r'(\d+)', banos_text_full)
                if banos_match:
                    banos_text = banos_match.group(1)

            m2_text = ""
            m2_elem = c.select_one(".properties__area")
            if m2_elem:
                m2_text_full = m2_elem.get_text(" ", strip=True)
                m2_match = re.search(r'(\d+)', m2_text_full)
                if m2_match:
                    m2_text = m2_match.group(1)

            img = ""
            img_tag = c.select_one("img")
            if img_tag:
                img = img_tag.get("src") or img_tag.get("data-src") or ""
                if img and img.startswith("https://img"):
                    img = img.strip()
                elif img and img.startswith("//"):
                    img_full = "https:" + img
                    if img_full.startswith("https://img"):
                        img = img_full.strip()
                    else:
                        img = ""
                else:
                    img = ""

            results.append({
                "titulo": title,
                "precio": price,
                "m2": m2_text,
                "dormitorios": dormitorios_text,
                "baños": banos_text,
                "descripcion": title,
                "link": href or "",
                "imagen_url": img
            })
        except Exception as e:
            logger.warning(f"Error en Properati al procesar un anuncio: {e}")
            continue

    return pd.DataFrame(results)

# -------------------- Doomos --------------------
def scrape_doomos(zona: str = "", dormitorios: str = "0", banos: str = "0",
                  price_min: Optional[int] = None, price_max: Optional[int] = None,
                  palabras_clave: str = ""):
    ZONA_IDS_CORRECTOS = {
        "ancón": "-336912", "ate": "-337679", "breña": "65645345", "carabayllo": "-339907",
        "chaclacayo": "-341190", "chorrillos": "-342811", "cieneguilla": "-343329",
        "comas": "-343903", "el agustino": "-345552", "jesús maría": "348294",
        "la molina": "-351740", "la victoria": "-352442", "lima": "45343445",
        "lince": "-352696", "los olivos": "191126", "lurigancho": "-353648",
        "lurín": "-353652", "magdalena del mar": "326245", "miraflores": "-354864",
        "pachacámac": "-356636", "pucusana": "-359672", "pueblo libre": "-359690",
        "puente piedra": "-359759", "punta hermosa": "-360186", "punta negra": "-360189",
        "rímac": "-361308", "san bartolo": "-362154", "san borja": "-362170",
        "san isidro": "-362425", "san luis": "-362738", "san miguel": "-362804",
        "santiago de surco": "-364705", "surquillo": "-364723"
    }

    base_url = "http://www.doomos.com.pe/search/"
    params = {
        "clase": "1",
        "stipo": "16",
        "pagina": "1",
        "sort": "primeasc"
    }

    if not zona or not zona.strip():
        params["loc_name"] = "Lima (Región de Lima)"
        params["loc_id"] = "-352647"
    else:
        zona_lower = zona.strip().lower()
        loc_id = ZONA_IDS_CORRECTOS.get(zona_lower, "")
        zona_formateada = f"{zona.strip()} (Región de Lima)"
        params["loc_name"] = zona_formateada
        if loc_id:
            params["loc_id"] = loc_id

    if dormitorios and dormitorios != "0":
        params["piezas"] = dormitorios
    if banos and banos != "0":
        params["banos"] = banos
    if price_min is not None:
        params["preciomin"] = str(price_min)
    if price_max is not None:
        params["preciomax"] = str(price_max)
    if palabras_clave and palabras_clave.strip():
        params["keyword"] = palabras_clave.strip()

    url = base_url + "?" + "&".join(f"{k}={requests.utils.quote(str(v))}" for k,v in params.items())
    logger.info(f"URL de Doomos: {url}")

    soup = get_page_content(url)
    if not soup:
        return pd.DataFrame()

    results = []
    cards = soup.select(".content_result")
    if not cards:
        logger.warning("No se encontraron cards en Doomos")
        return pd.DataFrame()

    for card in cards:
        try:
            a_tag = card.select_one(".content_result_titulo a")
            if not a_tag:
                continue
            title = a_tag.get_text(" ", strip=True)
            href = a_tag.get("href") or ""
            if href and href.startswith("/"):
                href = "http://www.doomos.com.pe" + href

            price_elem = card.select_one(".content_result_precio")
            price = price_elem.get_text(" ", strip=True) if price_elem else ""

            desc_elem = card.select_one(".content_result_descripcion")
            desc = desc_elem.get_text(" ", strip=True) if desc_elem else card.get_text(" ", strip=True)[:400]

            text_content = card.get_text(" ", strip=True).lower()
            dormitorios_text = ""
            banos_text = ""
            m2_text = ""
            dorm_match = re.search(r'(\d+)\s*dormitorio', text_content)
            if dorm_match:
                dormitorios_text = dorm_match.group(1)
            banos_match = re.search(r'(\d+)\s*baño', text_content)
            if banos_match:
                banos_text = banos_match.group(1)
            m2_match = re.search(r'(\d+)\s*m2', text_content)
            if m2_match:
                m2_text = m2_match.group(1)

            img_url = ""
            img_tag = card.select_one("img.content_result_image")
            if img_tag:
                img_url = img_tag.get("src") or img_tag.get("data-src") or ""
                if img_url and img_url.startswith("//"):
                    img_url = "https:" + img_url
                img_url = img_url.strip()

            results.append({
                "titulo": title,
                "precio": price,
                "m2": m2_text,
                "dormitorios": dormitorios_text,
                "baños": banos_text,
                "descripcion": desc,
                "link": href,
                "imagen_url": img_url
            })
        except Exception as e:
            logger.warning(f"Error procesando card en Doomos: {e}")
            continue

    return pd.DataFrame(results)

# -------------------- Filtrado y Unificación --------------------
SCRAPERS = [
    ("nestoria", scrape_nestoria),
    ("infocasas", scrape_infocasas),
    ("properati", scrape_properati),
    ("doomos", scrape_doomos),
]

def _parse_price_soles(s):
    moneda, val = parse_precio_con_moneda(str(s))
    return val if moneda == "S" else None

def _filter_df_strict(df, dormitorios_req, banos_req, price_min, price_max):
    if df is None or df.empty:
        return pd.DataFrame()
    dfc = df.copy().reset_index(drop=True)
    dfc["_precio_soles"] = dfc["precio"].apply(_parse_price_soles)
    dfc["_dorm_num"] = dfc["dormitorios"].apply(_extract_int_from_text)
    dfc["_banos_num"] = dfc["baños"].apply(_extract_int_from_text)
    mask = pd.Series(True, index=dfc.index)

    if dormitorios_req and str(dormitorios_req).strip() != "" and str(dormitorios_req) != "0":
        try:
            dorm_req_int = int(dormitorios_req)
            mask &= (dfc["_dorm_num"].notnull()) & (dfc["_dorm_num"] == dorm_req_int)
        except:
            pass

    if banos_req and str(banos_req).strip() != "" and str(banos_req) != "0":
        try:
            banos_req_int = int(banos_req)
            mask &= (dfc["_banos_num"].notnull()) & (dfc["_banos_num"] == banos_req_int)
        except:
            pass

    if (price_min is not None) or (price_max is not None):
        pmin = price_min if price_min is not None else -10**12
        pmax = price_max if price_max is not None else 10**12
        mask &= dfc["_precio_soles"].notnull()
        mask &= (dfc["_precio_soles"] >= int(pmin)) & (dfc["_precio_soles"] <= int(pmax))

    df_filtered = dfc.loc[mask].copy().reset_index(drop=True)
    df_filtered.drop(columns=["_precio_soles","_dorm_num","_banos_num"], errors="ignore", inplace=True)
    return df_filtered

def _filter_by_keywords(df, palabras_clave: str):
    if df is None or df.empty or not palabras_clave or not palabras_clave.strip():
        return df
    palabras = palabras_clave.lower().split()
    dfc = df.copy()
    dfc["texto_completo"] = (
        dfc["titulo"].astype(str) + " " +
        dfc.get("descripcion", pd.Series([""]*len(dfc))).astype(str) + " " +
        dfc.get("m2", pd.Series([""]*len(dfc))).astype(str) + " " +
        dfc.get("dormitorios", pd.Series([""]*len(dfc))).astype(str) + " " +
        dfc.get("baños", pd.Series([""]*len(dfc))).astype(str)
    ).str.lower()
    for p in palabras:
        dfc = dfc[dfc["texto_completo"].str.contains(re.escape(p), na=False, case=False)]
    dfc.drop(columns=["texto_completo"], errors="ignore", inplace=True)
    return dfc

def run_scrapers(zona: str = "", dormitorios: str = "0", banos: str = "0",
                 price_min: Optional[int] = None, price_max: Optional[int] = None,
                 palabras_clave: str = ""):
    frames = []
    logger.info(f"🔎 Buscando: zona='{zona}' | dorms={dormitorios} | baños={banos} | pmin={price_min} | pmax={price_max} | keywords='{palabras_clave}'")
    for name, func in SCRAPERS:
        logger.info(f"-> Ejecutando scraper: {name}")
        try:
            df = func(zona=zona, dormitorios=dormitorios, banos=banos, price_min=price_min, price_max=price_max, palabras_clave=palabras_clave)
        except Exception as e:
            logger.error(f" ❌ Error CRÍTICO ejecutando {name}: {e}")
            df = pd.DataFrame()

        if df is None or not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(columns=["titulo","precio","m2","dormitorios","baños","descripcion","link","imagen_url"])

        required_columns = ["titulo","precio","m2","dormitorios","baños","descripcion","link","imagen_url"]
        for col in required_columns:
            if col not in df.columns:
                df[col] = ""

        df = df.fillna("").astype(object)
        for col in required_columns:
            df[col] = df[col].astype(str).str.strip().replace({None: "", "None": ""})

        df_filtered = _filter_df_strict(df, dormitorios, banos, price_min, price_max)
        logger.info(f"   después filtrado estricto: {len(df_filtered)}")

        if palabras_clave and palabras_clave.strip() and name not in ("urbania", "doomos", "properati"):
            prev = len(df_filtered)
            df_filtered = _filter_by_keywords(df_filtered, palabras_clave)
            logger.info(f"   después filtrar por keywords: {len(df_filtered)}")

        if len(df_filtered) > 0:
            df_filtered = df_filtered.copy()
            df_filtered["fuente"] = name
            df_filtered["scraped_at"] = datetime.now().isoformat()
            df_filtered["id"] = [str(uuid.uuid4()) for _ in range(len(df_filtered))]
            frames.append(df_filtered)

    if not frames:
        logger.warning("⚠️ Ninguna fuente devolvió anuncios tras filtrar.")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined = combined[~combined["link"].str.startswith("#")].reset_index(drop=True)
    combined = combined[combined["link"] != ""].reset_index(drop=True)
    combined = combined.drop_duplicates(subset=["link","titulo"], keep="first").reset_index(drop=True)
    logger.info(f"✅ Total final de propiedades combinadas: {len(combined)}")
    return combined

if __name__ == "__main__":
    print("CONFIG: todos los filtros son opcionales.")
    zona = input("👉 Zona (ej: comas) - vacío para todas: ").strip()
    dormitorios = input("👉 Dormitorios (0 si no filtrar): ").strip() or "0"
    banos = input("👉 Baños (0 si no filtrar): ").strip() or "0"
    pmin = input("👉 Precio mínimo (solo números, 0 si no filtrar): ").strip() or "0"
    pmax = input("👉 Precio máximo (solo números, 0 si no filtrar): ").strip() or "0"
    palabras_clave = input("👉 Palabras clave (opcional): ").strip()
    pmin_val = int(pmin) if pmin and pmin != "0" else None
    pmax_val = int(pmax) if pmax and pmax != "0" else None
    combined = run_scrapers(zona=zona, dormitorios=dormitorios, banos=banos,
                            price_min=pmin_val, price_max=pmax_val,
                            palabras_clave=palabras_clave)
    print("Proceso finalizado. Resultados:", len(combined))