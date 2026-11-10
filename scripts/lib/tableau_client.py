"""Shared Tableau client for the assessment scripts. Standard library only.

Configuration comes from environment variables, loaded from a .env file at the
repo root (or the path in TABLEAU_ENV_FILE). Variables already set in the shell
take precedence over the file.

Every script takes --site. Any setting can be overridden per site by suffixing
it with __<SITE KEY>, where the key is the site contentUrl upper-cased with
non-alphanumerics turned into underscores (the Server Default site is DEFAULT):

    TABLEAU_PAT_NAME=assessment            # used for every site...
    TABLEAU_PAT_NAME__FINANCE=finance-pat  # ...except --site finance

Required:
    TABLEAU_SERVER_URL         https://tableau.example.com (no trailing path)
    TABLEAU_PAT_NAME
    TABLEAU_PAT_SECRET
Optional:
    TABLEAU_SITE_CONTENT_URL   site to use when --site is not passed
    TABLEAU_SITES              comma-separated sites for --all (step 00, runner); needed for Site
                               Admin PATs, which cannot list the server's sites
    TABLEAU_DEPLOYMENT         server | cloud (auto-detected from the URL otherwise)
    TABLEAU_API_VERSION        skip REST version negotiation (e.g. 3.24)
    TABLEAU_CA_BUNDLE          CA bundle path for corporate TLS inspection
    TABLEAU_VERIFY_TLS         set to 0 only for a lab server with a self-signed cert

Sign-in safety:
  * Tableau invalidates a PAT after four consecutive failed sign-ins. A failed
    sign-in is never retried here.
  * A PAT can hold only one session at a time; signing in again (to any site)
    ends the previous session. Run sites one after another, never in parallel
    with the same PAT. A session that ends mid-run is re-established once.
"""

import argparse
import http.client
import json
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def log(msg):
    print(msg, file=sys.stderr, flush=True)


# ---------------------------------------------------------------- .env / config
def load_dotenv(path=None):
    """Minimal .env parser: KEY=VALUE, optional 'export ', quotes, # comments."""
    path = Path(path or os.environ.get("TABLEAU_ENV_FILE") or REPO_ROOT / ".env")
    if not path.exists():
        return None
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[7:]
        key, val = line.split("=", 1)
        key, val = key.strip(), val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
            val = val[1:-1]
        elif " #" in val:
            val = val.split(" #", 1)[0].rstrip()
        os.environ.setdefault(key, val)
    return path


_ENV_LOADED = False


def ensure_env():
    global _ENV_LOADED
    if not _ENV_LOADED:
        p = load_dotenv()
        if p:
            log(f"Loaded config from {p}")
        _ENV_LOADED = True


def normalize_site(site):
    """'default' / '' / None -> '' (the Server Default site); otherwise the contentUrl."""
    if site is None:
        site = os.environ.get("TABLEAU_SITE_CONTENT_URL", "")
    return "" if site.strip().lower() in ("", "default") else site.strip()


def configured_sites():
    """Sites listed in TABLEAU_SITES, normalized; None if unset."""
    ensure_env()
    raw = os.environ.get("TABLEAU_SITES")
    if not raw or not raw.strip():
        return None
    items = (s.strip().strip("\"'") for s in raw.split(","))  # tolerate "a","b" quoting
    return list(dict.fromkeys(normalize_site(s) for s in items if s))


def site_key(site):
    return re.sub(r"[^A-Z0-9]", "_", site.upper()) if site else "DEFAULT"


def site_folder(site):
    return site or "default"


def cfg(name, site, default=None):
    """Per-site override NAME__<SITEKEY>, then NAME, then default."""
    ensure_env()
    v = os.environ.get(f"{name}__{site_key(site)}")
    if v is None:
        v = os.environ.get(name)
    return default if v is None or v == "" else v


def add_site_args(ap, need_out=True):
    ap.add_argument("--site", default=None,
                    help="site contentUrl (the segment after /site/ in a URL); 'default' for the Server Default site")
    if need_out:
        ap.add_argument("--out", default=None, help="output folder (default: output/<site>)")
    return ap


def resolve_site_args(args):
    """Return (site_contentUrl, out_path). Creates the output folder."""
    ensure_env()
    site = normalize_site(args.site)
    out = Path(getattr(args, "out", None) or REPO_ROOT / "output" / site_folder(site))
    out.mkdir(parents=True, exist_ok=True)
    return site, out


def deployment(server_url, site=""):
    d = (cfg("TABLEAU_DEPLOYMENT", site) or "").lower()
    if d in ("server", "cloud"):
        return d
    host = urllib.parse.urlparse(server_url).hostname or ""
    return "cloud" if host.endswith("online.tableau.com") else "server"


class TableauError(RuntimeError):
    pass


class TransientError(TableauError):
    """Network-level failure (connection dropped, timeout, DNS)."""


class HTTPStatusError(TableauError):
    def __init__(self, status, msg):
        super().__init__(msg)
        self.status = status


# ---------------------------------------------------------------- client
class TableauClient:
    def __init__(self, site=None):
        ensure_env()
        self.site = normalize_site(site)
        self.server = (cfg("TABLEAU_SERVER_URL", self.site) or "").rstrip("/")
        if not self.server:
            sys.exit("TABLEAU_SERVER_URL is not set (see .env.example)")
        self.deployment = deployment(self.server, self.site)
        ca = cfg("TABLEAU_CA_BUNDLE", self.site)
        if cfg("TABLEAU_VERIFY_TLS", self.site, "1") == "0":
            log("WARNING: TLS verification disabled (TABLEAU_VERIFY_TLS=0)")
            self._ssl = ssl._create_unverified_context()
        else:
            self._ssl = ssl.create_default_context(cafile=ca) if ca else ssl.create_default_context()
        self.version = cfg("TABLEAU_API_VERSION", self.site)
        self.product_version = None
        self.token = self.site_id = self.user_id = None
        self._signed_in_here = False

    def __enter__(self):
        self.sign_in()
        return self

    def __exit__(self, *exc):
        self.sign_out()

    # -- low-level HTTP
    def _http(self, method, url, body=None, accept="application/json", timeout=300):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Accept", accept)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        if self.token:
            req.add_header("X-Tableau-Auth", self.token)
        try:
            with urllib.request.urlopen(req, timeout=timeout, context=self._ssl) as r:
                raw = r.read()
        except urllib.error.HTTPError as e:
            detail = e.read().decode(errors="replace")[:800]
            raise HTTPStatusError(e.code, f"{method} {url} -> HTTP {e.code}: {detail}") from None
        except urllib.error.URLError as e:
            raise TransientError(f"{method} {url} -> {e.reason}") from None
        except (http.client.HTTPException, ConnectionError, TimeoutError) as e:
            raise TransientError(f"{method} {url} -> {type(e).__name__}: {e}") from None
        if accept != "application/json":
            return raw
        return json.loads(raw) if raw else {}

    def request(self, method, path, body=None, retries=3):
        """Authenticated call with one re-auth on expiry and backoff on 429/5xx/network errors."""
        url = path if path.startswith("http") else f"{self.server}{path}"
        reauthed = False
        for attempt in range(retries + 1):
            try:
                return self._http(method, url, body)
            except HTTPStatusError as e:
                if e.status == 401 and self._signed_in_here and not reauthed:
                    log("Session ended (expired, or this PAT signed in elsewhere); signing in again")
                    self._signed_in_here, self.token = False, None
                    self.sign_in()
                    reauthed = True
                    continue
                if e.status in (429, 502, 503, 504) and attempt < retries:
                    delay = min(30, 2 ** (attempt + 1))
                    log(f"HTTP {e.status}; retrying in {delay}s")
                    time.sleep(delay)
                    continue
                raise
            except TransientError as e:
                if attempt < retries:
                    delay = min(30, 2 ** (attempt + 1))
                    log(f"{e}; retrying in {delay}s")
                    time.sleep(delay)
                    continue
                raise

    # -- auth
    def server_info(self):
        info = self._http("GET", f"{self.server}/api/2.4/serverinfo")["serverInfo"]
        pv = info.get("productVersion")
        self.product_version = pv.get("value") if isinstance(pv, dict) else pv
        return info

    def negotiate_version(self):
        if self.version:
            return self.version
        try:
            self.version = self.server_info()["restApiVersion"]
        except Exception as e:  # noqa: BLE001
            log(f"REST version negotiation failed ({e}); using 3.22")
            self.version = "3.22"
        return self.version

    def sign_in(self):
        self.negotiate_version()
        name, secret = cfg("TABLEAU_PAT_NAME", self.site), cfg("TABLEAU_PAT_SECRET", self.site)
        if not (name and secret):
            sys.exit(f"No PAT for site '{site_folder(self.site)}': set TABLEAU_PAT_NAME / TABLEAU_PAT_SECRET "
                     f"(or the __{site_key(self.site)} overrides) in .env")
        body = {"credentials": {
            "personalAccessTokenName": name,
            "personalAccessTokenSecret": secret,
            "site": {"contentUrl": self.site},
        }}
        log(f"Signing in to site '{site_folder(self.site)}' with PAT '{name}' (REST {self.version}); one attempt")
        resp = self._http("POST", f"{self.server}/api/{self.version}/auth/signin", body)
        c = resp["credentials"]
        self.token, self.site_id = c["token"], c["site"]["id"]
        self.user_id = (c.get("user") or {}).get("id")
        self._signed_in_here = True

    def sign_out(self):
        if not self._signed_in_here:
            return
        try:
            self._http("POST", f"{self.server}/api/{self.version}/auth/signout")
        except Exception:  # noqa: BLE001
            pass
        self._signed_in_here = False

    # -- REST helpers
    def rest(self, method, path, body=None):
        return self.request(method, f"/api/{self.version}/sites/{self.site_id}{path}", body)

    def rest_paged(self, path, collection, item, page_size=1000, site_scoped=True):
        """Iterate a paginated REST collection, e.g. ('/datasources', 'datasources', 'datasource')."""
        page = 1
        sep = "&" if "?" in path else "?"
        base = f"/api/{self.version}/sites/{self.site_id}" if site_scoped else f"/api/{self.version}"
        while True:
            resp = self.request("GET", f"{base}{path}{sep}pageSize={page_size}&pageNumber={page}")
            items = (resp.get(collection) or {}).get(item) or []
            if isinstance(items, dict):
                items = [items]
            yield from items
            total = int((resp.get("pagination") or {}).get("totalAvailable") or 0)
            if page * page_size >= total or not items:
                return
            page += 1

    def current_user(self):
        return (self.rest("GET", f"/users/{self.user_id}") or {}).get("user") or {}

    def list_sites(self):
        """All sites on the server. Server Administrators only."""
        return list(self.rest_paged("/sites", "sites", "site", page_size=100, site_scoped=False))

    # -- Metadata API
    def graphql(self, query, variables=None):
        payload = {"query": query}
        if variables is not None:
            payload["variables"] = variables
        return self.request("POST", "/api/metadata/graphql", payload)

    # -- VizQL Data Service (Cloud Admin Insights; Server 2025.1+)
    def vds_query(self, datasource_luid, query):
        body = {"datasource": {"datasourceLuid": datasource_luid}, "query": query}
        resp = self.request("POST", "/api/v1/vizql-data-service/query-datasource", body)
        return resp.get("data") or []

    def admin_insights(self):
        """Map Admin Insights datasource name -> LUID. Tableau Cloud only."""
        filt = urllib.parse.quote("projectName:eq:Admin Insights", safe=":")
        found = {d["name"]: d["id"] for d in self.rest_paged(f"/datasources?filter={filt}",
                                                             "datasources", "datasource")}
        if not found:
            raise TableauError("No Admin Insights datasources visible. Admin Insights is Tableau Cloud only, "
                               "and the PAT owner must be a Site Admin.")
        return found
