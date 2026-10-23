"""
Sentinel-1 Download Pipeline Workflow

A robust, automated pipeline for querying, filtering, and downloading Sentinel-1
SAR products (.SAFE archives) from the Copernicus Data Space Ecosystem (CDSE).

Key Features:
- OData API querying with spatial, temporal, sensor-mode and product-type filtering
  (paginated, so long time ranges are not truncated at 1000 products).
- SAR-specific filters: polarisation, orbit direction, relative orbit, COG exclusion.
- Reprocessing de-duplication (keeps the most recently published product per scene).
- Pass-level ROI coverage filtering (unions consecutive slices of one datatake, so an
  ROI straddling two slices is not wrongly discarded).
- Automated Keycloak authentication with age-based token refresh.
- Sequential or multithreaded downloads with .part files, size checks and resume logs.

Author: Julian Manning
Created: October 2026
Email: julian.manning@outlook.com
LinkedIn: https://www.linkedin.com/in/julian-manning/
"""

# ---------------- Core / Standard Library ----------------
import os
import sys
import time
import re
import html
import zipfile
from datetime import datetime, timedelta
from typing import Optional, Iterable, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from contextlib import contextmanager

# ---------------- Data Handling ----------------
import pandas as pd

# ---------------- Geospatial ----------------
import geopandas as gpd
from shapely.geometry import mapping, shape as shp_shape, box
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform, unary_union
import shapely.wkt as wkt
from pyproj import Transformer

# ---------------- Visualisation / Mapping ----------------
import folium

# ---------------- Networking / IO ----------------
import requests

# ---------------- Utilities ----------------
from tqdm import tqdm

# ---------------- Optional Dependencies ----------------
try:
    import certifi as _certifi
except Exception:
    _certifi = None


# ====================================================================================================
# CONSTANTS
# ====================================================================================================

CDSE_ODATA_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
CDSE_TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

# CDSE access tokens live ~10 minutes; refresh before that.
TOKEN_MAX_AGE_S = 8 * 60

# Sentinel-1 product naming convention, e.g.
# S1A_IW_GRDH_1SDV_20260810T021515_20260810T021540_060521_078A2B_9F1C.SAFE
# S1C_IW_SLC__1SDV_20260810T021514_20260810T021541_003456_006F1E_AB12.SAFE
# S1A_IW_GRDH_1SDV_..._9F1C_COG.SAFE   (Cloud-Optimised GeoTIFF repackaging)
S1_NAME_PATTERN = re.compile(
    r'^(?P<mission>S1[A-D])_'
    r'(?P<mode>IW|EW|WV|S[1-6])_'
    r'(?P<ptype>GRD[HMF]|SLC_|RAW_|OCN_|ETA_)_'
    r'(?P<level>[0-4])(?P<pclass>[SAN])(?P<pol>SH|SV|DH|DV|HH|VV|HV|VH)_'
    r'(?P<start>\d{8}T\d{6})_(?P<stop>\d{8}T\d{6})_'
    r'(?P<abs_orbit>\d{6})_(?P<datatake>[0-9A-F]{6})_(?P<crc>[0-9A-F]{4})'
    r'(?P<cog>_COG)?\.SAFE$',
    re.IGNORECASE
)

# Product-type token as it appears in the file name (always 4 characters).
_PTYPE_TOKENS = {
    "GRDH": "GRDH", "GRDM": "GRDM", "GRDF": "GRDF",
    "SLC": "SLC_", "RAW": "RAW_", "OCN": "OCN_",
}

# Polarisation tokens (name characters 15-16) and their human-readable channels.
POLARISATION_TOKENS = {
    "DV": "VV+VH", "DH": "HH+HV", "SV": "VV", "SH": "HH",
}

# Attributes pulled out of $expand=Attributes into their own columns.
_S1_ATTRIBUTES = [
    "orbitDirection", "relativeOrbitNumber", "orbitNumber", "polarisationChannels",
    "sliceNumber", "totalSlices", "datatakeID", "platformSerialIdentifier",
    "operationalMode", "productType", "timeliness",
]

# ====================================================================================================
# GENERAL UTILITIES (shared with the Sentinel-2 workflow)
# ====================================================================================================

def batch_extract_zips(
    source_folder: str,
    destination_folder: str,
    delete_after_extraction: bool = False
) -> None:
    """
    Automate the batch extraction of compressed ZIP archives to a target directory.

    Iterates through a source folder, extracts every ZIP into the destination
    (Sentinel-1 archives unpack to a native ``*.SAFE`` folder) and optionally
    deletes the source archive after a successful extraction.

    Args:
        source_folder (str): Directory containing the compressed ZIP files.
        destination_folder (str): Directory where extracted contents are stored.
        delete_after_extraction (bool, optional): If True, permanently removes each
            ZIP after successful extraction. Defaults to False.

    Returns:
        None
    """
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    for filename in sorted(os.listdir(source_folder)):
        if not filename.lower().endswith('.zip'):
            continue

        file_path = os.path.join(source_folder, filename)

        try:
            with zipfile.ZipFile(file_path, 'r') as archive:
                archive.extractall(destination_folder)
            print(f'Extracted: {filename}')

            if delete_after_extraction:
                os.remove(file_path)
                print(f'Deleted: {filename}')

        except zipfile.BadZipFile:
            print(f'\x1b[93mInvalid zip file skipped: {filename}\x1b[0m')

# ====================================================================================================

def visualise_roi_on_map(
    roi_gdf: gpd.GeoDataFrame,
    epsg: int = 4326,
    use_dissolve: bool = False,
    zoom_start: int = 8,
    tiles: str = "Esri.WorldImagery",
    rectangle_color: str = "#FF6A00",
    rectangle_weight: int = 2,
    polygon_color: str = "#1f78b4",
    polygon_weight: int = 2,
    polygon_fill_opacity: float = 0.1,
    fit_padding: tuple = (40, 40),
    add_polygon: bool = True,
    save_path: Optional[str] = None,
) -> folium.Map:
    """
    Generate an interactive map visualisation of a Region of Interest (ROI).

    Reprojects the ROI to a geographic CRS, cleans invalid geometries and renders
    both the polygon and its bounding box on a Folium basemap as a spatial sanity
    check before querying the catalogue.

    Args:
        roi_gdf (gpd.GeoDataFrame): Input spatial data containing the ROI.
        epsg (int, optional): Geographic CRS for mapping. Defaults to 4326.
        use_dissolve (bool, optional): Merge all features into one shape. Defaults to False.
        zoom_start (int, optional): Initial zoom level. Defaults to 8.
        tiles (str, optional): Basemap provider. Defaults to "Esri.WorldImagery".
        rectangle_color, rectangle_weight, polygon_color, polygon_weight,
        polygon_fill_opacity, fit_padding, add_polygon: Styling options.
        save_path (str, optional): Save the map as standalone HTML. Defaults to None.

    Returns:
        folium.Map: Interactive map centred on the ROI.

    Raises:
        ValueError: If the GeoDataFrame is None, empty, or lacks a CRS.
    """
    if roi_gdf is None:
        raise ValueError("roi_gdf is None")
    if roi_gdf.empty:
        raise ValueError("roi_gdf is empty")
    if roi_gdf.crs is None:
        raise ValueError("roi_gdf has no CRS defined")

    roi = roi_gdf.to_crs(epsg=epsg)
    roi = roi[~roi.geometry.is_empty]
    roi = roi[roi.geometry.is_valid]
    if roi.empty:
        raise ValueError("ROI geometry is empty or invalid after cleaning")

    geom = roi.dissolve().geometry.iloc[0] if use_dissolve else roi.iloc[0].geometry

    minx, miny, maxx, maxy = geom.bounds
    m = folium.Map(location=[(miny + maxy) / 2.0, (minx + maxx) / 2.0], zoom_start=zoom_start, tiles=tiles)

    bounds_sw, bounds_ne = [miny, minx], [maxy, maxx]
    folium.Rectangle(
        bounds=[bounds_sw, bounds_ne], color=rectangle_color, weight=rectangle_weight,
        fill=False, tooltip="ROI bounds",
    ).add_to(m)

    if add_polygon:
        folium.GeoJson(
            mapping(geom), name="ROI",
            style_function=lambda _: {"color": polygon_color, "weight": polygon_weight,
                                      "fillOpacity": polygon_fill_opacity},
            tooltip="ROI polygon",
        ).add_to(m)

    m.fit_bounds([bounds_sw, bounds_ne], padding=fit_padding)
    if save_path:
        m.save(save_path)
    return m

# ====================================================================================================

def get_roi_wkt_from_vector(file_path, use_bbox: bool = True, simplify_tolerance: float = 0.001) -> str:
    """
    Convert a vector dataset into a WKT geometry (EPSG:4326) for Copernicus API queries.

    Args:
        file_path (str or Path): Path to the vector dataset defining the ROI.
        use_bbox (bool, optional): If True (default), returns the bounding box of all
            features — short, robust URLs. If False, returns the dissolved polygon,
            simplified by ``simplify_tolerance`` degrees to keep the query URL short.
        simplify_tolerance (float, optional): Simplification tolerance in degrees
            when ``use_bbox=False``. Defaults to 0.001 (~100 m).

    Returns:
        str: WKT Polygon string in EPSG:4326.

    Raises:
        ValueError: If the vector file has no defined CRS.
    """
    gdf = gpd.read_file(file_path)

    if gdf.crs is None:
        raise ValueError("The provided spatial file has no defined CRS.")

    if gdf.crs.to_epsg() != 4326:
        original_crs_name = gdf.crs.name
        gdf = gdf.to_crs(epsg=4326)
        print(f"Reprojecting ROI from {original_crs_name} to {gdf.crs.name}...")

    if use_bbox:
        b = gdf.total_bounds
        return box(b[0], b[1], b[2], b[3]).wkt

    geom = _fix_geom(unary_union(gdf.geometry)).simplify(simplify_tolerance, preserve_topology=True)
    if geom.geom_type == "MultiPolygon":
        print("\x1b[93mROI is a MultiPolygon; using its convex hull for the API query.\x1b[0m")
        geom = geom.convex_hull
    return geom.wkt

# ====================================================================================================

def get_keycloak(username: str, password: str) -> str:
    """
    Retrieve an access token from the CDSE Keycloak identity service (ROPC grant).

    Args:
        username (str): CDSE account username (email).
        password (str): CDSE account password.

    Returns:
        str: JWT access token for authenticated API requests.

    Raises:
        Exception: If authentication fails (network, credentials or server error).
    """
    data = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password",
    }
    try:
        r = requests.post(CDSE_TOKEN_URL, data=data, timeout=60)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Keycloak token creation failed: {str(e)}")
    return r.json()["access_token"]

# ====================================================================================================

@contextmanager
def download_timer(process_name="Process"):
    """
    Measure and report the execution time of a code block (context manager).

    Args:
        process_name (str, optional): Name used in the console output. Defaults to "Process".

    Yields:
        None
    """
    start_time = time.time()
    start_time_readable = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time))
    print(f"\n--- Starting {process_name} ---")
    print(f"Start time: {start_time_readable}\n")
    try:
        yield
    finally:
        end_time = time.time()
        end_time_readable = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time))
        elapsed = end_time - start_time
        h, m, s = int(elapsed // 3600), int((elapsed % 3600) // 60), int(elapsed % 60)
        print(f"\n--- {process_name} Complete ---")
        print("\x1b[94m" + f"Start time: {start_time_readable}" + "\x1b[0m")
        print("\x1b[94m" + f"End time:   {end_time_readable}" + "\x1b[0m")
        if h > 0:
            print("\x1b[92m" + f"Elapsed time: {h}h {m}m {s}s" + "\x1b[0m")
        elif m > 0:
            print("\x1b[92m" + f"Elapsed time: {m}m {s}s" + "\x1b[0m")
        else:
            print("\x1b[92m" + f"Elapsed time: {s}s" + "\x1b[0m")

# ====================================================================================================
# GEOMETRY HELPERS
# ====================================================================================================

def _fix_geom(geom: BaseGeometry) -> BaseGeometry:
    """Repair an invalid geometry via ``make_valid`` with a ``buffer(0)`` fallback."""
    if geom is None:
        return None
    try:
        from shapely.validation import make_valid
        g2 = make_valid(geom)
        if g2 is not None and not g2.is_empty:
            return g2
    except Exception:
        pass
    try:
        g3 = geom.buffer(0)
        if g3 is not None and not g3.is_empty:
            return g3
    except Exception:
        pass
    return geom


def _swap_xy_geom(geom):
    """Swap the coordinate order of a geometry from (x, y) to (y, x)."""
    if geom is None:
        return None
    return transform(lambda x, y, z=None: (y, x) if z is None else (y, x, z), geom)


def _utm_epsg_for(lon, lat):
    """Return the UTM EPSG code (326XX / 327XX) for a WGS84 coordinate."""
    zone = int((lon + 180) // 6) + 1
    return 32600 + zone if lat >= 0 else 32700 + zone


def _to_geom(g):
    """Convert WKT strings, GeoJSON dicts or CDSE GeoFootprint structures into Shapely geometry."""
    try:
        if isinstance(g, dict):
            if "Wkt" in g and isinstance(g["Wkt"], str):
                return wkt.loads(g["Wkt"])
            if "type" in g and "coordinates" in g:
                return shp_shape(g)
        if isinstance(g, str) and g.strip():
            return wkt.loads(g)
    except Exception:
        return None
    return None

# ====================================================================================================
# SENTINEL-1 NAME PARSING
# ====================================================================================================

def parse_s1_name(name: str) -> dict:
    """
    Parse a Sentinel-1 product name into its structured components.

    Args:
        name (str): Product name, e.g.
            ``S1A_IW_GRDH_1SDV_20260810T021515_20260810T021540_060521_078A2B_9F1C.SAFE``.

    Returns:
        dict: Keys ``mission, mode, ptype, level, pclass, pol, start, stop, abs_orbit,
        datatake, crc, is_cog``. Values are None if the name does not match.
    """
    keys = ["mission", "mode", "ptype", "level", "pclass", "pol", "start", "stop",
            "abs_orbit", "datatake", "crc", "is_cog"]
    m = S1_NAME_PATTERN.match(str(name))
    if not m:
        return dict.fromkeys(keys)
    d = {k: (v.upper() if isinstance(v, str) else v) for k, v in m.groupdict().items()}
    d["ptype"] = d["ptype"].rstrip("_")
    d["is_cog"] = d.pop("cog") is not None
    d["abs_orbit"] = int(d["abs_orbit"])
    return d

# ====================================================================================================
# CATALOGUE QUERY
# ====================================================================================================

def _normalise_list(value) -> Optional[list]:
    """Return None for empty input, otherwise a list (wrapping scalars)."""
    if value is None:
        return None
    if isinstance(value, (str, int)):
        return [value]
    value = list(value)
    return value or None


def build_s1_query_url(
    ROI: str,
    time_begin: str,
    time_end: str,
    data_collection: str = "SENTINEL-1",
    sensor_mode: str = "IW",
    product_type: str = "GRDH",
    page_size: int = 500,
) -> str:
    """
    Construct the CDSE OData query URL for Sentinel-1 products.

    Server-side filters: collection, spatial intersection, sensing date window and a
    product-name token (e.g. ``_IW_GRDH_``) for sensor mode + product type. No cloud
    filter is applied — SAR products carry no ``cloudCover`` attribute and including
    one returns zero results. Attributes are expanded so orbit direction, relative
    orbit and polarisation can be filtered client-side.

    The end date is inclusive (the whole ``time_end`` day is queried).

    Args:
        ROI (str): WKT geometry in EPSG:4326.
        time_begin (str): Start date (YYYY-MM-DD), inclusive.
        time_end (str): End date (YYYY-MM-DD), inclusive.
        data_collection (str, optional): Defaults to "SENTINEL-1".
        sensor_mode (str, optional): "IW", "EW", "SM" (S1–S6) or "WV". Defaults to "IW".
            Use None to skip the name filter entirely.
        product_type (str, optional): "GRDH", "GRDM", "GRDF", "SLC", "RAW" or "OCN".
            Defaults to "GRDH". Use None to skip the name filter entirely.
        page_size (int, optional): Products per page (CDSE max 1000). Defaults to 500.

    Returns:
        str: Fully encoded query URL.
    """
    t0 = datetime.strptime(time_begin, "%Y-%m-%d")
    t1 = datetime.strptime(time_end, "%Y-%m-%d") + timedelta(days=1)

    filters = [
        f"Collection/Name eq '{data_collection}'",
        f"OData.CSC.Intersects(area=geography'SRID=4326;{ROI}')",
        f"ContentDate/Start ge {t0:%Y-%m-%d}T00:00:00.000Z",
        f"ContentDate/Start lt {t1:%Y-%m-%d}T00:00:00.000Z",
    ]

    if sensor_mode and product_type:
        ptype = product_type.upper()
        if ptype not in _PTYPE_TOKENS:
            raise ValueError(f"Unknown product_type '{product_type}'. Valid: {list(_PTYPE_TOKENS)}")
        filters.append(f"contains(Name,'_{sensor_mode.upper()}_{_PTYPE_TOKENS[ptype]}_')")

    url = (
        f"{CDSE_ODATA_URL}?$filter={' and '.join(filters)}"
        f"&$orderby=ContentDate/Start asc&$expand=Attributes&$count=True&$top={int(page_size)}"
    )
    return html.unescape(url).replace(" ", "%20")


def query_copernicus_s1_catalog(
    ROI: str,
    time_begin: str,
    time_end: str,
    data_collection: str = "SENTINEL-1",
    sensor_mode: str = "IW",
    product_type: str = "GRDH",
    cert_path=None,
    time_out: int = 300,
    page_size: int = 500,
    max_products: int = 20000,
) -> Optional[list]:
    """
    Query the CDSE catalogue for Sentinel-1 products, following pagination.

    Args:
        ROI, time_begin, time_end, data_collection, sensor_mode, product_type, page_size:
            See ``build_s1_query_url``.
        cert_path (str or bool, optional): SSL certificate bundle path.
        time_out (int, optional): HTTP timeout per page (seconds). Defaults to 300.
        max_products (int, optional): Safety cap on total products. Defaults to 20000.

    Returns:
        list[dict] | None: Raw product records, or None if the request failed.
    """
    print("Starting query to Copernicus Data Space (Sentinel-1)...")
    url = build_s1_query_url(ROI, time_begin, time_end, data_collection, sensor_mode, product_type, page_size)
    print(f"Query URL:\n{url}")

    verify = cert_path if cert_path else True
    records, page, total = [], 0, None
    try:
        while url and len(records) < max_products:
            response = requests.get(url, verify=verify, timeout=time_out)
            response.raise_for_status()
            data = response.json()
            if total is None:
                total = data.get("@odata.count")
                if total is not None:
                    print(f"Catalogue reports {total} matching products.")
            batch = data.get("value", [])
            records.extend(batch)
            page += 1

            next_link = data.get("@odata.nextLink")
            if next_link:
                url = next_link
            elif total is not None and len(records) < total and batch:
                # Fallback pagination if the server does not supply a nextLink
                base = re.sub(r"&\$skip=\d+", "", url)
                url = f"{base}&$skip={len(records)}"
            else:
                url = None
        print(f"Retrieved {len(records)} products in {page} page(s).")
        return records
    except requests.exceptions.RequestException as e:
        print(f"\x1b[91mError fetching data: {e}\x1b[0m")
        return None

# ====================================================================================================
# PARSING & FILTERING
# ====================================================================================================

def _flatten_attributes(attrs) -> dict:
    """Turn the CDSE ``Attributes`` list into a {Name: Value} dict."""
    if not isinstance(attrs, list):
        return {}
    return {a.get("Name"): a.get("Value") for a in attrs if isinstance(a, dict)}


def parse_s1_footprints(records: list) -> Optional[gpd.GeoDataFrame]:
    """
    Convert raw catalogue records into a GeoDataFrame of Sentinel-1 footprints.

    Adds parsed name fields (``mission, mode, ptype, pol, start, datatake, is_cog`` …),
    flattened attributes (``orbitDirection, relativeOrbitNumber, polarisationChannels`` …),
    ``size_gb``, ``acq_date`` and ``identifier`` (name without ``.SAFE``).

    Args:
        records (list[dict]): Output of ``query_copernicus_s1_catalog``.

    Returns:
        gpd.GeoDataFrame | None: Footprints in EPSG:4326, or None if empty.
    """
    if not records:
        print("No products found in the response.")
        return None

    p = pd.DataFrame.from_records(records)
    print(f"Raw product count: {p.shape[0]}")

    p["geometry"] = p["GeoFootprint"].apply(_to_geom) if "GeoFootprint" in p else None
    if p["geometry"].isna().all() and "Footprint" in p:
        # Fallback: "geography'SRID=4326;POLYGON ((...))'"
        p["geometry"] = p["Footprint"].astype(str).str.extract(r";(.*)'$", expand=False).apply(_to_geom)

    parsed = pd.DataFrame([parse_s1_name(n) for n in p["Name"]], index=p.index)
    attrs = pd.DataFrame([_flatten_attributes(a) for a in p.get("Attributes", [None] * len(p))], index=p.index)
    for col in _S1_ATTRIBUTES:
        p[col] = attrs[col] if col in attrs else None

    p = pd.concat([p, parsed], axis=1)
    p["identifier"] = p["Name"].str.replace(r"\.SAFE$", "", regex=True)
    p["size_gb"] = pd.to_numeric(p.get("ContentLength"), errors="coerce") / 1e9
    p["acq_date"] = pd.to_datetime(p["start"], format="%Y%m%dT%H%M%S", errors="coerce").dt.date

    gdf = gpd.GeoDataFrame(p.drop(columns=["Attributes"], errors="ignore"), geometry="geometry", crs="EPSG:4326")

    unparsed = gdf["mission"].isna().sum()
    if unparsed:
        print(f"\x1b[93mWarning: {unparsed} product name(s) did not match the Sentinel-1 naming pattern.\x1b[0m")
    return gdf


def filter_s1_products(
    productDF: gpd.GeoDataFrame,
    polarisation: Optional[Union[str, Iterable[str]]] = None,
    orbit_direction: Optional[str] = None,
    relative_orbit: Optional[Union[int, Iterable[int]]] = None,
    missions: Optional[Iterable[str]] = None,
    exclude_cog: bool = True,
) -> gpd.GeoDataFrame:
    """
    Apply Sentinel-1 specific client-side filters.

    Args:
        productDF (gpd.GeoDataFrame): Output of ``parse_s1_footprints``.
        polarisation (str | list[str], optional): Name tokens to keep — "DV" (VV+VH),
            "DH" (HH+HV), "SV" (VV), "SH" (HH). None keeps all.
        orbit_direction (str, optional): "ASCENDING" or "DESCENDING". None keeps both.
        relative_orbit (int | list[int], optional): Relative orbit number(s) (track).
        missions (list[str], optional): e.g. ["S1A", "S1C"]. None keeps all.
        exclude_cog (bool, optional): Drop ``*_COG.SAFE`` duplicates. Defaults to True.

    Returns:
        gpd.GeoDataFrame: Filtered copy.
    """
    df = productDF.copy()
    n0 = len(df)

    def _report(label, before):
        dropped = before - len(df)
        if dropped:
            print(f"  {label}: dropped {dropped}")

    if exclude_cog:
        b = len(df); df = df[~df["is_cog"].fillna(False).astype(bool)]; _report("COG duplicates", b)

    pols = _normalise_list(polarisation)
    if pols:
        pols = [str(x).upper() for x in pols]
        b = len(df); df = df[df["pol"].isin(pols)]; _report(f"polarisation not in {pols}", b)

    if orbit_direction:
        od = orbit_direction.upper()
        b = len(df); df = df[df["orbitDirection"].astype(str).str.upper() == od]; _report(f"orbit direction != {od}", b)

    rel = _normalise_list(relative_orbit)
    if rel:
        rel = [int(r) for r in rel]
        b = len(df)
        df = df[pd.to_numeric(df["relativeOrbitNumber"], errors="coerce").isin(rel)]
        _report(f"relative orbit not in {rel}", b)

    ms = _normalise_list(missions)
    if ms:
        ms = [m.upper() for m in ms]
        b = len(df); df = df[df["mission"].isin(ms)]; _report(f"mission not in {ms}", b)

    print(f"Products after Sentinel-1 filters: {len(df)} (from {n0})")
    return df


def keep_latest_s1_only(productDF: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Retain only the most recently published version of each Sentinel-1 scene.

    Sentinel-1 has no "N" processing-baseline token; reprocessed products share
    mission, mode, product type, polarisation and sensing start/stop times but differ
    in the trailing 4-character CRC. Within each such group the product with the
    latest ``PublicationDate`` is kept (name used as tie-breaker).

    Args:
        productDF (gpd.GeoDataFrame): Products with parsed name fields.

    Returns:
        gpd.GeoDataFrame: De-duplicated copy.
    """
    if productDF.empty:
        return productDF.copy()

    df = productDF.copy()
    df["__scene_key"] = (
        df["mission"].fillna("") + "_" + df["mode"].fillna("") + "_" + df["ptype"].fillna("") + "_" +
        df["level"].fillna("") + df["pclass"].fillna("") + df["pol"].fillna("") + "_" +
        df["start"].fillna("") + "_" + df["stop"].fillna("") + "_" +
        df["is_cog"].astype(str)
    )
    # Unparseable names keep their own key so they are never collapsed together
    df.loc[df["mission"].isna(), "__scene_key"] = df.loc[df["mission"].isna(), "Name"]
    df["__pub"] = pd.to_datetime(df.get("PublicationDate"), errors="coerce", utc=True)

    out = (
        df.sort_values(["__scene_key", "__pub", "Name"], ascending=[True, False, False])
          .groupby("__scene_key", as_index=False).head(1)
          .sort_values("start")
          .drop(columns=["__scene_key", "__pub"])
          .reset_index(drop=True)
    )
    dropped = len(df) - len(out)
    print(f"Reprocessed duplicates removed: {dropped} (kept {len(out)})")
    return out


def apply_pass_coverage_filter(
    productDF: gpd.GeoDataFrame,
    ROI: str,
    min_roi_coverage_frac: float = 0.70,
) -> gpd.GeoDataFrame:
    """
    Keep only satellite passes that jointly cover enough of the ROI.

    Sentinel-1 slices are cut along-track, so an ROI often straddles two consecutive
    slices of the same pass. Slices are grouped by pass (mission + datatake ID — all
    slices of one continuous acquisition share it), their footprints are unioned, and
    ROI coverage is computed for the pass as a whole in a local UTM projection.
    Within a passing group, only slices that actually intersect the ROI are kept.

    Args:
        productDF (gpd.GeoDataFrame): Candidate products.
        ROI (str): WKT geometry in EPSG:4326.
        min_roi_coverage_frac (float, optional): Minimum fraction (0–1) of ROI area the
            pass must cover. Defaults to 0.70.

    Returns:
        gpd.GeoDataFrame: Products belonging to qualifying passes, with ``roi_cover_pct``
        (per slice) and ``pass_cover_pct`` (per pass) columns added.
    """
    if productDF.empty:
        return productDF.copy()

    try:
        roi_geom = _fix_geom(wkt.loads(ROI))
    except Exception as e:
        print(f"\x1b[93mWarning: Failed to parse ROI WKT ({e}); skipping pass coverage filter.\x1b[0m")
        return productDF.copy()

    df = productDF.copy()
    df["geometry"] = df.geometry.apply(_fix_geom)

    # Axis-order safety net (lat/lon vs lon/lat)
    if not df.geometry.intersects(roi_geom).any():
        swapped = _fix_geom(_swap_xy_geom(roi_geom))
        if df.geometry.intersects(swapped).any():
            print("Note: ROI appeared to be lat/long. Swapped to long/lat for coverage filter.")
            roi_geom = swapped

    cx, cy = roi_geom.centroid.x, roi_geom.centroid.y
    proj = Transformer.from_crs("EPSG:4326", f"EPSG:{_utm_epsg_for(cx, cy)}", always_xy=True).transform
    roi_area = transform(proj, roi_geom).area
    if roi_area <= 0:
        print("\x1b[93mWarning: ROI projected area invalid (<=0). Nothing will be downloaded.\x1b[0m")
        return df.iloc[0:0].copy()

    def _frac(geom):
        if geom is None or geom.is_empty:
            return 0.0
        inter = _fix_geom(geom.intersection(roi_geom))
        return 0.0 if inter.is_empty else transform(proj, inter).area / roi_area

    df["roi_cover_pct"] = df.geometry.apply(_frac) * 100.0

    df["__pass"] = df["mission"].fillna("UNK") + "_" + df["datatake"].fillna(df["Name"])
    pass_cover = df.groupby("__pass").geometry.apply(lambda gs: _frac(unary_union(list(gs)))) * 100.0
    df["pass_cover_pct"] = df["__pass"].map(pass_cover)

    thr = float(min_roi_coverage_frac) * 100.0
    print(f"\nPer-pass ROI coverage (threshold {thr:.0f}%):")
    for key, g in df.sort_values("start").groupby("__pass", sort=False):
        flag = "\x1b[92m✓\x1b[0m" if g["pass_cover_pct"].iloc[0] >= thr else "\x1b[91m✗\x1b[0m"
        print(f"  {flag} {g['acq_date'].iloc[0]}  {key}  "
              f"{str(g['orbitDirection'].iloc[0])[:4]:<4} rel.orbit {g['relativeOrbitNumber'].iloc[0]}  "
              f"slices={len(g)}  coverage={g['pass_cover_pct'].iloc[0]:.1f}%")

    kept = df[(df["pass_cover_pct"] >= thr) & (df["roi_cover_pct"] > 0)]
    n_pass_all, n_pass_kept = df["__pass"].nunique(), kept["__pass"].nunique()
    print(f"Passes kept: {n_pass_kept}/{n_pass_all}  |  slices kept: {len(kept)}/{len(df)}")
    return kept.drop(columns=["__pass"]).reset_index(drop=True)

# ====================================================================================================
# REPORTING
# ====================================================================================================

def summarise_s1_products(productDF: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Tabular summary of selected products, with total volume printed.

    Args:
        productDF (gpd.GeoDataFrame): Selected products.

    Returns:
        pd.DataFrame: One row per product (date, mission, direction, track, polarisation,
        size, coverage, name).
    """
    if productDF is None or productDF.empty:
        print("No products to summarise.")
        return pd.DataFrame()

    cols = {
        "acq_date": "Date", "mission": "Mission", "mode": "Mode", "ptype": "Type",
        "orbitDirection": "Direction", "relativeOrbitNumber": "Rel. orbit",
        "pol": "Pol", "size_gb": "Size (GB)", "roi_cover_pct": "ROI cover %",
        "pass_cover_pct": "Pass cover %", "Name": "Name",
    }
    present = [c for c in cols if c in productDF.columns]
    out = pd.DataFrame(productDF[present]).rename(columns=cols).sort_values(["Date", "Name"]).reset_index(drop=True)
    if "Pol" in out:
        out["Pol"] = out["Pol"].map(lambda p: f"{p} ({POLARISATION_TOKENS.get(p, '?')})" if isinstance(p, str) else p)
    for c in ("Size (GB)", "ROI cover %", "Pass cover %"):
        if c in out:
            out[c] = pd.to_numeric(out[c], errors="coerce").round(2)

    total_gb = pd.to_numeric(productDF.get("size_gb"), errors="coerce").sum()
    print(f"\x1b[94m{len(out)} products  |  {out['Date'].nunique()} acquisition dates  |  "
          f"total ≈ {total_gb:.1f} GB\x1b[0m")
    return out


def visualise_s1_footprints(
    productDF: gpd.GeoDataFrame,
    ROI: str,
    tiles: str = "Esri.WorldImagery",
    save_path: Optional[str] = None,
) -> folium.Map:
    """
    Map the selected Sentinel-1 footprints against the ROI.

    Ascending passes are drawn blue, descending orange; tooltips show date, mission,
    relative orbit and product name.

    Args:
        productDF (gpd.GeoDataFrame): Selected products.
        ROI (str): WKT geometry in EPSG:4326.
        tiles (str, optional): Basemap. Defaults to "Esri.WorldImagery".
        save_path (str, optional): Save as standalone HTML.

    Returns:
        folium.Map
    """
    roi_geom = wkt.loads(ROI)
    minx, miny, maxx, maxy = roi_geom.bounds
    m = folium.Map(location=[(miny + maxy) / 2, (minx + maxx) / 2], zoom_start=7, tiles=tiles)

    colours = {"ASCENDING": "#1f78b4", "DESCENDING": "#ff7f00"}
    if productDF is not None and not productDF.empty:
        for _, r in productDF.iterrows():
            if r.geometry is None:
                continue
            c = colours.get(str(r.get("orbitDirection")).upper(), "#666666")
            tip = (f"{r.get('acq_date')} | {r.get('mission')} | {r.get('orbitDirection')} | "
                   f"rel.orbit {r.get('relativeOrbitNumber')}<br>{r['Name']}")
            folium.GeoJson(
                mapping(r.geometry),
                style_function=lambda _, c=c: {"color": c, "weight": 1, "fillOpacity": 0.05},
                tooltip=tip,
            ).add_to(m)
        b = productDF.total_bounds
        m.fit_bounds([[min(b[1], miny), min(b[0], minx)], [max(b[3], maxy), max(b[2], maxx)]], padding=(20, 20))

    folium.GeoJson(
        mapping(roi_geom), name="ROI",
        style_function=lambda _: {"color": "#e31a1c", "weight": 3, "fillOpacity": 0.15},
        tooltip="ROI",
    ).add_to(m)

    if save_path:
        m.save(save_path)
    return m

# ====================================================================================================
# SEARCH (query + parse + filter) — shared by preview, download and resume
# ====================================================================================================

def search_s1_products(
    ROI: str,
    time_begin: str,
    time_end: str,
    data_collection: str = "SENTINEL-1",
    sensor_mode: str = "IW",
    product_type: str = "GRDH",
    polarisation=None,
    orbit_direction: Optional[str] = None,
    relative_orbit=None,
    missions=None,
    exclude_cog: bool = True,
    download_latest_only: bool = True,
    enable_pass_coverage_filter: bool = True,
    min_roi_coverage_frac: float = 0.70,
    cert_path=None,
    time_out: int = 2400,
    **_ignored,
) -> Optional[gpd.GeoDataFrame]:
    """
    Run the full Sentinel-1 selection chain without downloading anything.

    Stages: catalogue query → footprint parsing → S1 filters (COG, polarisation,
    orbit direction, relative orbit, mission) → reprocessing de-duplication →
    pass-level ROI coverage filter.

    Accepts (and ignores) extra keyword arguments so the same ``pipeline_config``
    dictionary used for downloading can be passed straight in.

    Returns:
        gpd.GeoDataFrame | None: Products selected for download (empty if none).
    """
    if cert_path is None and _certifi is not None:
        cert_path = _certifi.where()

    # 1. Catalogue query
    records = query_copernicus_s1_catalog(
        ROI, time_begin, time_end, data_collection, sensor_mode, product_type,
        cert_path=cert_path, time_out=min(int(time_out), 600),
    )
    if records is None:
        return None

    # 2. Footprints → GeoDataFrame
    productDF = parse_s1_footprints(records)
    if productDF is None or productDF.empty:
        return gpd.GeoDataFrame(columns=["Name", "Id", "identifier", "geometry"], geometry="geometry", crs="EPSG:4326")

    # 3. Sentinel-1 specific filters
    print("\nApplying Sentinel-1 filters...")
    productDF = filter_s1_products(productDF, polarisation, orbit_direction, relative_orbit, missions, exclude_cog)

    # 4. Reprocessing de-duplication
    if download_latest_only:
        productDF = keep_latest_s1_only(productDF)

    # 5. Pass-level ROI coverage
    if enable_pass_coverage_filter:
        productDF = apply_pass_coverage_filter(productDF, ROI, min_roi_coverage_frac)
    else:
        print("Skipping pass coverage filter based on user preference.")

    return productDF.reset_index(drop=True)

# ====================================================================================================
# DOWNLOADS
# ====================================================================================================

class _TokenManager:
    """Thread-safe Keycloak token cache that refreshes once older than ``TOKEN_MAX_AGE_S``."""

    def __init__(self, user, password):
        self._user, self._password = user, password
        self._token, self._issued = None, 0.0
        self._lock = Lock()

    def get(self, force: bool = False) -> str:
        with self._lock:
            if force or self._token is None or (time.time() - self._issued) > TOKEN_MAX_AGE_S:
                self._token = get_keycloak(self._user, self._password)
                self._issued = time.time()
            return self._token


def _format_elapsed(seconds: float) -> str:
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    return f"{h:02}:{m:02}:{s:02}"


def _download_single_product(feat: dict, save_folder: str, tokens: _TokenManager, cert_path, time_out) -> tuple:
    """
    Download one product to ``<identifier>.zip`` via a ``.part`` file.

    Returns:
        tuple(str, bool, str): (product Id, success flag, message)
    """
    name, product_id, identifier = feat["Name"], feat["Id"], feat["identifier"]
    expected = feat.get("ContentLength")
    expected = int(expected) if pd.notna(expected) and expected else None
    file_path = os.path.join(save_folder, f"{identifier}.zip")
    part_path = file_path + ".part"
    verify = cert_path if cert_path else True

    # Skip if a complete file already exists
    if os.path.exists(file_path) and (expected is None or os.path.getsize(file_path) == expected):
        return product_id, True, f"Already present, skipped: {name}"

    url = f"{CDSE_ODATA_URL}({product_id})/$value"
    start = time.time()

    for attempt in (1, 2):
        try:
            with requests.Session() as session:
                session.headers.update({"Authorization": f"Bearer {tokens.get(force=(attempt == 2))}"})

                # CDSE answers with a redirect to the download node; follow it manually
                # so the Authorization header is retained.
                resp = session.get(url, allow_redirects=False, verify=verify, timeout=time_out, stream=True)
                hops = 0
                while resp.status_code in (301, 302, 303, 307, 308) and "Location" in resp.headers and hops < 5:
                    resp = session.get(resp.headers["Location"], allow_redirects=False, verify=verify,
                                       timeout=time_out, stream=True)
                    hops += 1

                if resp.status_code == 401 and attempt == 1:
                    continue  # refresh token and retry once
                if resp.status_code != 200:
                    msg = f"HTTP {resp.status_code}"
                    try:
                        msg += f" {resp.json()}"
                    except Exception:
                        pass
                    return product_id, False, f"Failed {name}: {msg}"

                ctype = resp.headers.get("Content-Type", "")
                if not any(t in ctype for t in ("zip", "octet-stream")):
                    return product_id, False, f"Failed {name}: unexpected content type '{ctype}'"

                written, last_tick = 0, time.time()
                with open(part_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
                            written += len(chunk)
                            if time.time() - last_tick >= 10:
                                pct = f" {100 * written / expected:5.1f}%" if expected else ""
                                sys.stdout.write(f"\r  {identifier[:40]}…{pct} {written / 1e9:.2f} GB "
                                                 f"[{_format_elapsed(time.time() - start)}]")
                                sys.stdout.flush()
                                last_tick = time.time()

            if expected is not None and written != expected:
                os.remove(part_path)
                return product_id, False, f"Failed {name}: size mismatch ({written} vs {expected} bytes)"

            os.replace(part_path, file_path)
            return product_id, True, f"Downloaded: {name} ({written / 1e9:.2f} GB) in {_format_elapsed(time.time() - start)}"

        except Exception as e:
            if os.path.exists(part_path):
                try:
                    os.remove(part_path)
                except OSError:
                    pass
            if attempt == 2:
                return product_id, False, f"Error downloading {name}: {e}"

    return product_id, False, f"Failed {name}: authentication retry exhausted"


def execute_s1_downloads(
    products: gpd.GeoDataFrame,
    save_folder: str,
    copernicus_user: str,
    copernicus_password: str,
    max_parallel_downloads: int = 1,
    cert_path=None,
    time_out: int = 2400,
    log_csv_path: Optional[str] = None,
) -> tuple:
    """
    Download products sequentially or in parallel, logging each success immediately.

    Each success is appended to ``log_csv_path`` as soon as it completes, so an
    interrupted run can be resumed without losing progress.

    Returns:
        tuple(list[str], list[str]): (successful product Ids, failed product names)
    """
    tokens = _TokenManager(copernicus_user, copernicus_password)
    print("Authenticating with Copernicus Keycloak...")
    tokens.get()
    print("Token retrieved successfully.")

    downloaded, failed = [], []
    lock = Lock()
    records = products.to_dict("records")

    def _record(result, name):
        pid, ok, msg = result
        with lock:
            print(("\n\x1b[92m" if ok else "\n\x1b[91m") + msg + "\x1b[0m")
            if ok:
                downloaded.append(pid)
                if log_csv_path:
                    with open(log_csv_path, "a") as fh:
                        fh.write(f"{pid}\n")
            else:
                failed.append(name)

    workers = int(max(1, max_parallel_downloads))
    if workers == 1:
        for rec in tqdm(records, total=len(records)):
            _record(_download_single_product(rec, save_folder, tokens, cert_path, time_out), rec["Name"])
    else:
        print(f"\nParallel downloads enabled: {workers} workers")
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(_download_single_product, r, save_folder, tokens, cert_path, time_out): r["Name"]
                       for r in records}
            for fut in tqdm(as_completed(futures), total=len(futures)):
                _record(fut.result(), futures[fut])

    return downloaded, failed

# ====================================================================================================
# END-TO-END PIPELINES
# ====================================================================================================

def _default_log_path(save_folder, time_begin, time_end):
    return os.path.join(save_folder, f"log_{time_begin.replace('-', '_')}_to_{time_end.replace('-', '_')}.csv")


def download_copernicus_s1_data(
    ROI: str,
    time_begin: str,
    time_end: str,
    copernicus_user: str,
    copernicus_password: str,
    save_folder: str,
    data_collection: str = "SENTINEL-1",
    sensor_mode: str = "IW",
    product_type: str = "GRDH",
    polarisation=None,
    orbit_direction: Optional[str] = None,
    relative_orbit=None,
    missions=None,
    exclude_cog: bool = True,
    download_latest_only: bool = True,
    enable_pass_coverage_filter: bool = True,
    min_roi_coverage_frac: float = 0.70,
    max_parallel_downloads: int = 1,
    cert_path=None,
    time_out: int = 2400,
    log_csv_path: Optional[str] = None,
    skip_logged: bool = False,
) -> list:
    """
    End-to-end Sentinel-1 pipeline: search, filter, report and download .SAFE archives.

    Args:
        ROI (str): WKT geometry (EPSG:4326).
        time_begin / time_end (str): Inclusive date window (YYYY-MM-DD).
        copernicus_user / copernicus_password (str): CDSE credentials.
        save_folder (str): Output folder for ``<product>.zip`` files and the log CSV.
        data_collection (str): "SENTINEL-1".
        sensor_mode (str): "IW" (default), "EW", "SM" (S1–S6), "WV".
        product_type (str): "GRDH" (default), "GRDM", "GRDF", "SLC", "RAW", "OCN".
        polarisation (str | list): "DV", "DH", "SV", "SH" or None (all).
        orbit_direction (str): "ASCENDING", "DESCENDING" or None (both).
        relative_orbit (int | list): Track number(s) or None (all).
        missions (list): e.g. ["S1A", "S1C"] or None (all).
        exclude_cog (bool): Drop ``_COG.SAFE`` duplicates. Defaults to True.
        download_latest_only (bool): Keep latest reprocessing per scene. Defaults to True.
        enable_pass_coverage_filter (bool): Apply pass-level ROI coverage threshold.
        min_roi_coverage_frac (float): Pass coverage threshold (0–1). Defaults to 0.70.
        max_parallel_downloads (int): Worker threads (CDSE allows a few concurrent
            downloads per account; 2–4 is sensible). Defaults to 1.
        cert_path (str, optional): SSL bundle (defaults to certifi).
        time_out (int): HTTP timeout (seconds). Defaults to 2400.
        log_csv_path (str, optional): Log of successful product Ids. Defaults to
            ``<save_folder>/log_<begin>_to_<end>.csv``.
        skip_logged (bool): If True, products already in the log are skipped (resume).

    Returns:
        list[str]: Product Ids downloaded in this run.
    """
    if cert_path is None and _certifi is not None:
        cert_path = _certifi.where()

    os.makedirs(save_folder, exist_ok=True)
    log_csv_path = log_csv_path or _default_log_path(save_folder, time_begin, time_end)

    products = search_s1_products(
        ROI=ROI, time_begin=time_begin, time_end=time_end, data_collection=data_collection,
        sensor_mode=sensor_mode, product_type=product_type, polarisation=polarisation,
        orbit_direction=orbit_direction, relative_orbit=relative_orbit, missions=missions,
        exclude_cog=exclude_cog, download_latest_only=download_latest_only,
        enable_pass_coverage_filter=enable_pass_coverage_filter,
        min_roi_coverage_frac=min_roi_coverage_frac, cert_path=cert_path, time_out=time_out,
    )
    if products is None:
        print("\x1b[91mCatalogue query failed; nothing downloaded.\x1b[0m")
        return []

    if skip_logged:
        if os.path.exists(log_csv_path) and os.path.getsize(log_csv_path) > 0:
            logged = set(pd.read_csv(log_csv_path, header=None)[0].astype(str))
            before = len(products)
            products = products[~products["Id"].astype(str).isin(logged)]
            print(f"\x1b[92mSkipped {before - len(products)} products already in log: {log_csv_path}\x1b[0m")
        else:
            print(f"\x1b[93mLog file not found/empty at {log_csv_path}. Proceeding with all products.\x1b[0m")

    if products.empty:
        print("No products to download.")
        return []

    print(f"\nDownloading {len(products)} products:")
    summarise_s1_products(products)
    for name in products["Name"]:
        print("\x1b[92m" + name + "\x1b[0m")

    downloaded, failed = execute_s1_downloads(
        products, save_folder, copernicus_user, copernicus_password,
        max_parallel_downloads, cert_path, time_out, log_csv_path=log_csv_path,
    )

    print(f"\nLog file: {log_csv_path}")
    if not downloaded:
        print('\x1b[93mNo data was successfully downloaded.\x1b[0m')
    if failed:
        print("\n\x1b[91mFailed downloads:\x1b[0m")
        for name in failed:
            print(name)
    return downloaded


def resume_copernicus_s1_data_download(log_csv_path: Optional[str] = None, **pipeline_config) -> list:
    """
    Resume an interrupted Sentinel-1 download, skipping product Ids already in the log.

    Takes the same ``pipeline_config`` as ``download_copernicus_s1_data``. New
    successes are appended to the existing log (never overwritten).

    Returns:
        list[str]: Product Ids downloaded in this run.
    """
    return download_copernicus_s1_data(**pipeline_config, log_csv_path=log_csv_path, skip_logged=True)
