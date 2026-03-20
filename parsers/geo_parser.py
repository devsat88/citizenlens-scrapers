"""
GeoJSON parser. Extracts features, maps properties, and derives lat/lng from geometry.
Supports Point, LineString, and Polygon (centroid for non-point types).
"""
import logging
from typing import Any

logger = logging.getLogger(__name__)


def _centroid_linestring(coords: list[list[float]]) -> tuple[float, float]:
    """Average of all coordinate pairs."""
    lats = [c[1] for c in coords]
    lngs = [c[0] for c in coords]
    return sum(lats) / len(lats), sum(lngs) / len(lngs)


def _centroid_polygon(coords: list[list[list[float]]]) -> tuple[float, float]:
    """Centroid of the outer ring (first ring)."""
    outer = coords[0]
    lats = [c[1] for c in outer]
    lngs = [c[0] for c in outer]
    return sum(lats) / len(lats), sum(lngs) / len(lngs)


def _extract_latlong(geometry: dict[str, Any]) -> tuple[float | None, float | None]:
    """Return (latitude, longitude) from a GeoJSON geometry object."""
    if not geometry:
        return None, None

    geo_type = geometry.get("type", "")
    coords = geometry.get("coordinates")

    if not coords:
        return None, None

    if geo_type == "Point":
        # coords: [lng, lat]
        return float(coords[1]), float(coords[0])

    if geo_type == "LineString":
        lat, lng = _centroid_linestring(coords)
        return lat, lng

    if geo_type in ("Polygon", "MultiPolygon"):
        ring = coords[0] if geo_type == "Polygon" else coords[0][0]
        lat, lng = _centroid_polygon([ring])
        return lat, lng

    if geo_type == "MultiLineString":
        # Flatten all lines and average
        all_coords = [c for line in coords for c in line]
        lat, lng = _centroid_linestring(all_coords)
        return lat, lng

    logger.warning("geo_parser: unsupported geometry type '%s'", geo_type)
    return None, None


def parse(geojson: dict, config: dict) -> list[dict]:
    """
    Extract features from a GeoJSON FeatureCollection.

    config keys:
      columns (dict) : {property_name: field_name} — maps feature properties to output fields
    """
    col_map: dict[str, str] = config.get("columns", {})
    features: list[dict] = geojson.get("features", [])

    results: list[dict] = []
    for feature in features:
        if not isinstance(feature, dict):
            continue

        properties: dict = feature.get("properties") or {}
        geometry: dict = feature.get("geometry") or {}

        record: dict = {}

        # Map properties
        if col_map:
            for src, dst in col_map.items():
                value = properties.get(src)
                if value is not None:
                    record[dst] = value
        else:
            record.update(properties)

        # Always extract coordinates
        lat, lng = _extract_latlong(geometry)
        if lat is not None:
            record["latitude"] = lat
        if lng is not None:
            record["longitude"] = lng

        if record:
            results.append(record)

    return results
