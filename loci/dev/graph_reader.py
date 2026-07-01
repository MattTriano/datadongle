"""
Reader for the routing graph binary format (v2) plus small exploration helpers.

This is the inverse of loci.exports.graph_format: it parses the bytes
written by `write_graph` back into a `networkx.MultiDiGraph` shaped like
the one `RoutingGraphExporter._build_graph` produces (same node keys,
same edge attribute names), so notebook analysis matches the exporter's
view of the world.

The format spec lives in services/routing/docs/graph-format.md. If
FORMAT_VERSION bumps there, this module needs a corresponding update.

Notebook usage:

    from graph_reader import load_graph, summarize, low_stress_subgraph, edges_dataframe

    G = load_graph("routing_graph.bin.gz")
    summarize(G)

    df = edges_dataframe(G)
    df["stress_per_meter"].describe()

    low = low_stress_subgraph(G, max_stress_per_meter=2.0)
    import networkx as nx
    islands = sorted(nx.strongly_connected_components(low), key=len, reverse=True)

Conventions:
  - Node keys are OSM node ids (ints), with attributes x (lon), y (lat),
    and is_intersection — matching the exporter.
  - Edge attributes: segment_id, name, highway, infra_type, forward,
    length_m, stress_cost, physical_cost, intersection_cost, crash_cost.
  - The format encodes missing values as NaN (floats) or a sentinel
    index (strings); the loader maps both to None for ergonomic
    filtering in pandas/networkx.
"""

from __future__ import annotations

import gzip
import math
import statistics
import struct
from pathlib import Path

import networkx as nx

MAGIC = b"LOCI"
SUPPORTED_FORMAT_VERSION = 2
NULL_STR_IDX = 0xFFFFFFFF
EDGE_FLAG_FORWARD = 0b0000_0001

# Record layouts, mirroring the writer. "x" bytes are padding.
# Node: u64 osm_id, f64 lon, f64 lat, u8 is_intersection, 7 pad = 32 bytes.
_NODE = struct.Struct("<QddB7x")
# Edge: u32 target, u32 segment_id_str, u32 name_str, u32 highway_str,
# u32 infra_type_str, u8 flags, 3 pad, f32 x5 (length, stress, physical,
# intersection, crash) = 44 bytes.
_EDGE = struct.Struct("<IIIIIB3xfffff")

_U32 = struct.Struct("<I")
_HEADER = struct.Struct("<4sHHd")  # magic, version, flags, heuristic_floor


def load_graph(path: str | Path) -> nx.MultiDiGraph:
    """Load a routing graph binary file into a MultiDiGraph.

    Accepts both gzipped (.bin.gz, as deployed to S3) and raw .bin files,
    sniffed by the gzip magic bytes rather than the file extension.
    """
    path = Path(path)
    with open(path, "rb") as f:
        head = f.read(2)
    opener = gzip.open if head == b"\x1f\x8b" else open
    with opener(path, "rb") as f:
        buf = f.read()

    pos = 0

    # --- Header ---
    magic, version, _flags, heuristic_floor = _HEADER.unpack_from(buf, pos)
    pos += _HEADER.size
    if magic != MAGIC:
        raise ValueError(f"{path}: bad magic {magic!r}, expected {MAGIC!r}")
    if version != SUPPORTED_FORMAT_VERSION:
        raise ValueError(
            f"{path}: format version {version}, this reader supports "
            f"{SUPPORTED_FORMAT_VERSION}"
        )

    # --- String table ---
    (n_strings,) = _U32.unpack_from(buf, pos)
    pos += 4
    strings: list[str] = []
    for _ in range(n_strings):
        (slen,) = _U32.unpack_from(buf, pos)
        pos += 4
        strings.append(buf[pos : pos + slen].decode("utf-8"))
        pos += slen

    def _str(idx: int) -> str | None:
        return None if idx == NULL_STR_IDX else strings[idx]

    # --- Node table ---
    (n_nodes,) = _U32.unpack_from(buf, pos)
    pos += 4
    G = nx.MultiDiGraph()
    osm_ids: list[int] = []
    for _ in range(n_nodes):
        osm_id, lon, lat, is_intersection = _NODE.unpack_from(buf, pos)
        pos += _NODE.size
        osm_ids.append(osm_id)
        G.add_node(osm_id, x=lon, y=lat, is_intersection=bool(is_intersection))

    # --- Edge table ---
    # Edge records don't carry their source node; it's implied by the
    # CSR offsets table that follows. Parse records first, attach after.
    (n_edges,) = _U32.unpack_from(buf, pos)
    pos += 4
    edge_records = []
    for _ in range(n_edges):
        edge_records.append(_EDGE.unpack_from(buf, pos))
        pos += _EDGE.size

    # --- CSR offsets ---
    # csr_offsets[i] .. csr_offsets[i+1] is the slice of edge_records
    # whose source is node index i.
    (n_offsets,) = _U32.unpack_from(buf, pos)
    pos += 4
    if n_offsets != n_nodes + 1:
        raise ValueError(
            f"{path}: CSR offset count {n_offsets} != node count + 1 ({n_nodes + 1})"
        )
    csr_offsets = struct.unpack_from(f"<{n_offsets}I", buf, pos)
    pos += 4 * n_offsets

    for source_idx in range(n_nodes):
        for j in range(csr_offsets[source_idx], csr_offsets[source_idx + 1]):
            (
                target_idx,
                seg_idx,
                name_idx,
                highway_idx,
                infra_idx,
                flags,
                length_m,
                stress_cost,
                physical_cost,
                intersection_cost,
                crash_cost,
            ) = edge_records[j]
            G.add_edge(
                osm_ids[source_idx],
                osm_ids[target_idx],
                segment_id=strings[seg_idx],
                name=_str(name_idx),
                highway=_str(highway_idx),
                infra_type=_str(infra_idx),
                forward=bool(flags & EDGE_FLAG_FORWARD),
                length_m=_none_if_nan(length_m),
                stress_cost=_none_if_nan(stress_cost),
                physical_cost=_none_if_nan(physical_cost),
                intersection_cost=_none_if_nan(intersection_cost),
                crash_cost=_none_if_nan(crash_cost),
            )

    # --- Segment geometry table ---
    (n_geoms,) = _U32.unpack_from(buf, pos)
    pos += 4
    segment_geometry: dict[str, tuple] = {}
    for _ in range(n_geoms):
        seg_idx, n_coords = struct.unpack_from("<II", buf, pos)
        pos += 8
        flat = struct.unpack_from(f"<{2 * n_coords}d", buf, pos)
        pos += 16 * n_coords
        segment_geometry[strings[seg_idx]] = tuple(zip(flat[0::2], flat[1::2]))

    if pos != len(buf):
        raise ValueError(
            f"{path}: {len(buf) - pos} unexpected trailing bytes after geometry table"
        )

    G.graph["heuristic_floor"] = heuristic_floor
    G.graph["format_version"] = version
    G.graph["segment_geometry"] = segment_geometry
    return G


def _none_if_nan(value: float) -> float | None:
    return None if math.isnan(value) else value


# ----------------------------------------------------------------------
# Exploration helpers
# ----------------------------------------------------------------------


def summarize(G: nx.MultiDiGraph) -> None:
    """Print a quick orientation summary of a loaded graph."""
    print(f"Nodes: {G.number_of_nodes():,}")
    print(f"Edges: {G.number_of_edges():,}")
    print(f"Heuristic floor: {G.graph.get('heuristic_floor'):.4f} cost/meter")
    print(f"Segment geometries: {len(G.graph.get('segment_geometry', {})):,}")

    intersections = sum(1 for _, d in G.nodes(data=True) if d.get("is_intersection"))
    print(f"Intersection nodes: {intersections:,}")

    weak = sorted(nx.weakly_connected_components(G), key=len, reverse=True)
    strong = sorted(nx.strongly_connected_components(G), key=len, reverse=True)
    print(f"Weakly connected components: {len(weak)} (largest: {len(weak[0]):,} nodes)")
    print(f"Strongly connected components: {len(strong)} (largest: {len(strong[0]):,} nodes)")

    spm = [
        d["stress_cost"] / d["length_m"]
        for _, _, d in G.edges(data=True)
        if d["stress_cost"] is not None and d["length_m"]
    ]
    if spm:
        q = statistics.quantiles(spm, n=20, method="inclusive")
        print(
            "Stress per meter: "
            f"min={min(spm):.2f}  p25={q[4]:.2f}  median={q[9]:.2f}  "
            f"p75={q[14]:.2f}  p95={q[18]:.2f}  max={max(spm):.2f}"
        )


def edges_dataframe(G: nx.MultiDiGraph):
    """Return one row per directed edge as a pandas DataFrame.

    Adds a derived stress_per_meter column (None where undefined).
    """
    import pandas as pd

    rows = []
    for u, v, key, d in G.edges(keys=True, data=True):
        row = {"u": u, "v": v, "key": key, **d}
        if d["stress_cost"] is not None and d["length_m"]:
            row["stress_per_meter"] = d["stress_cost"] / d["length_m"]
        else:
            row["stress_per_meter"] = None
        rows.append(row)
    return pd.DataFrame(rows)


def low_stress_subgraph(G: nx.MultiDiGraph, max_stress_per_meter: float) -> nx.MultiDiGraph:
    """Edges at or below a stress-per-meter threshold, as a subgraph view.

    Thresholding on stress_cost / length_m (rather than raw stress_cost)
    keeps the cutoff length-independent: a long pleasant trail segment
    isn't excluded just because its total cost is large.

    Returns a read-only view sharing data with G; call .copy() on the
    result if you need to mutate it. Nodes with no qualifying edges are
    excluded, so connected-component analysis on the result gives the
    "low-stress islands" directly.
    """
    keep = [
        (u, v, k)
        for u, v, k, d in G.edges(keys=True, data=True)
        if d["stress_cost"] is not None
        and d["length_m"]
        and d["stress_cost"] / d["length_m"] <= max_stress_per_meter
    ]
    return G.edge_subgraph(keep)
