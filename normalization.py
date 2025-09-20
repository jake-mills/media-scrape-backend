from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

TRACKING_PREFIXES = ("utm_",)
TRACKING_KEYS = {"fbclid", "gclid", "yclid", "mc_cid", "mc_eid"}

def normalize_url(url: str) -> str:
    try:
        p = urlparse(url)
        q = parse_qs(p.query, keep_blank_values=False)
        q = {k: v for k, v in q.items()
             if not k.startswith(TRACKING_PREFIXES) and k not in TRACKING_KEYS}
        cleaned = p._replace(
            netloc=p.netloc.lower(),
            fragment="",
            query=urlencode(q, doseq=True)
        )
        path = cleaned.path.replace("//", "/")
        cleaned = cleaned._replace(path=path)
        return urlunparse(cleaned)
    except Exception:
        return url
