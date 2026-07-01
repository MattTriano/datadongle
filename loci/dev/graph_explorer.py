"""Explore and map the components of an exported routing graph.

Takes the in-memory NetworkX DiGraph produced by RoutingGraphExporter
(after _build_graph, before/after _filter_small_components — either is
fine) and provides:

  - Per-component node/edge statistics, sorted largest first.
  - A folium map rendering one or more components, color-coded by
    component, with click popups linking back to OSM.
  - A folium map of just the elements inside a given bbox, useful for
    zooming in on suspicious areas.

This is intended for ad-hoc investigation in a notebook or REPL, not
for the deploy path.

Example:

    G = exporter._build_graph()              # before component filtering
    explorer = GraphExplorer(G)
    explorer.print_summary()

    # Map the two largest components together to see whether they
    # should be connected.
    m = explorer.map_components([0, 1])
    m.save("dc_top_two_components.html")

    # Zoom in on the gap between them.
    m = explorer.map_bbox(BBox(south=38.85, west=-77.05, north=38.88, east=-77.01))
    m.save("dc_gap.html")
"""

from __future__ import annotations

from dataclasses import dataclass

import folium
import networkx as nx
from loci.geo import BBox

# Distinct enough that adjacent components don't visually blend, and
# readable on folium's default OSM basemap. More than 10 components
# cycles through the palette, which is fine for the use case (large
# components are rare; if there are dozens, color isn't the right
# encoding anyway).
_COMPONENT_COLORS = [
    "#1f77b4",  # blue
    "#d62728",  # red
    "#2ca02c",  # green
    "#ff7f0e",  # orange
    "#9467bd",  # purple
    "#8c564b",  # brown
    "#e377c2",  # pink
    "#17becf",  # cyan
    "#bcbd22",  # olive
    "#7f7f7f",  # gray
]

_OSM_NODE_URL = "https://www.openstreetmap.org/node/{node_id}"
_OSM_WAY_URL = "https://www.openstreetmap.org/way/{way_id}"


@dataclass(frozen=True)
class ComponentStats:
    """Summary stats for a single weakly connected component.

    `bbox` is None for degenerate (single-node) components, since BBox
    requires south < north and west < east strictly.
    """

    index: int
    node_count: int
    edge_count: int
    bbox: BBox | None


class GraphExplorer:
    """Inspect and map components of a routing graph.

    The graph is expected to be the NetworkX DiGraph built by
    RoutingGraphExporter: nodes carry `x` (lon) and `y` (lat) attrs,
    edges carry `segment_id` (and the other routing attrs), and
    `G.graph['segment_geometry']` is a dict of segment_id -> tuple of
    (lon, lat) coordinate pairs.

    Components are computed once at construction. If you mutate the
    graph after constructing the explorer, build a new one.
    """

    def __init__(self, G: nx.DiGraph) -> None:
        self.G = G

        # Sorted largest first so component indices are stable and
        # caller-meaningful ("component 0" = the dominant network).
        raw = sorted(nx.weakly_connected_components(G), key=len, reverse=True)
        self._components: list[frozenset[int]] = [frozenset(c) for c in raw]

        # node id -> component index, for O(1) coloring during map render.
        self._node_to_component: dict[int, int] = {}
        for i, c in enumerate(self._components):
            for n in c:
                self._node_to_component[n] = i

    # ------------------------------------------------------------------
    # Component inspection
    # ------------------------------------------------------------------

    @property
    def components(self) -> list[frozenset[int]]:
        """Components as frozensets of node ids, sorted largest first."""
        return self._components

    def component_stats(self, index: int) -> ComponentStats:
        """Node count, edge count, and bbox for a single component."""
        nodes = self._components[index]
        sub = self.G.subgraph(nodes)
        lats = [data["y"] for _, data in sub.nodes(data=True)]
        lons = [data["x"] for _, data in sub.nodes(data=True)]

        bbox: BBox | None
        if min(lats) < max(lats) and min(lons) < max(lons):
            bbox = BBox(
                south=min(lats),
                west=min(lons),
                north=max(lats),
                east=max(lons),
            )
        else:
            bbox = None

        return ComponentStats(
            index=index,
            node_count=sub.number_of_nodes(),
            edge_count=sub.number_of_edges(),
            bbox=bbox,
        )

    def print_summary(self, top_n: int = 20) -> None:
        """Print component sizes, largest first."""
        print(f"Graph has {len(self._components)} weakly connected components")
        for i in range(min(top_n, len(self._components))):
            s = self.component_stats(i)
            bbox_str = (
                f"{s.bbox.south:.4f},{s.bbox.west:.4f} → "
                f"{s.bbox.north:.4f},{s.bbox.east:.4f}"
                if s.bbox is not None
                else "(degenerate)"
            )
            print(
                f"  Component {i}: {s.node_count} nodes, "
                f"{s.edge_count} edges (bbox: {bbox_str})"
            )
        if len(self._components) > top_n:
            tail = self._components[top_n:]
            tail_sizes = [len(c) for c in tail]
            print(
                f"  ... and {len(tail)} smaller components "
                f"(max {max(tail_sizes)} nodes, {sum(tail_sizes)} total)"
            )

    # ------------------------------------------------------------------
    # Mapping
    # ------------------------------------------------------------------

    def map_components(
        self,
        indices: list[int] | None = None,
        show_all_nodes: bool = False,
    ) -> folium.Map:
        """Render one or more components on a folium map.

        Args:
            indices: Component indices to render. None means all
                components. To compare the top two: pass [0, 1].
            show_all_nodes: If False (default), only render intersection
                nodes (degree != 2 in the underlying undirected graph).
                If True, render every node — slower and visually noisier,
                but useful for debugging snap behavior.
        """
        if indices is None:
            indices = list(range(len(self._components)))

        nodes_to_render: set[int] = set()
        for i in indices:
            nodes_to_render |= self._components[i]

        segments_to_render = self._collect_segments(
            keep=lambda u, v, data: u in nodes_to_render and v in nodes_to_render
        )

        return self._render_map(nodes_to_render, segments_to_render, show_all_nodes)

    def map_bbox(self, bbox: BBox, show_all_nodes: bool = False) -> folium.Map:
        """Render the elements that intersect a bounding box.

        A node intersects if its coordinates fall inside the bbox. A
        segment intersects if any of its geometry coordinates fall
        inside (or, if no geometry is stored, if either endpoint does).
        Segments that intersect pull their endpoints into the render
        set even if those endpoints are outside the bbox, so the user
        sees the full edge.
        """
        nodes_in_bbox: set[int] = set()
        for n, data in self.G.nodes(data=True):
            if _point_in_bbox(data["y"], data["x"], bbox):
                nodes_in_bbox.add(n)

        segment_geometry = self.G.graph.get("segment_geometry", {})

        def edge_in_bbox(u: int, v: int, data: dict) -> bool:
            sid = data["segment_id"]
            coords = segment_geometry.get(sid)
            if coords:
                return any(_point_in_bbox(lat, lon, bbox) for lon, lat in coords)
            # Fall back to endpoints if geometry wasn't stored.
            return u in nodes_in_bbox or v in nodes_in_bbox

        segments_to_render = self._collect_segments(keep=edge_in_bbox)

        # Pull in the endpoints of every rendered segment so the edge
        # has visible terminating nodes even if they sit just outside
        # the bbox.
        nodes_to_render = set(nodes_in_bbox)
        for u, v, _ in segments_to_render.values():
            nodes_to_render.add(u)
            nodes_to_render.add(v)

        return self._render_map(nodes_to_render, segments_to_render, show_all_nodes)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _collect_segments(self, keep) -> dict[str, tuple[int, int, dict]]:
        """Walk edges and return one (u, v, data) per unique segment_id
        for which `keep(u, v, data)` returns True. Dedupes the
        forward/backward edges of bidirectional segments.
        """
        out: dict[str, tuple[int, int, dict]] = {}
        for u, v, data in self.G.edges(data=True):
            sid = data["segment_id"]
            if sid in out:
                continue
            if keep(u, v, data):
                out[sid] = (u, v, data)
        return out

    def _render_map(
        self,
        nodes_to_render: set[int],
        segments_to_render: dict[str, tuple[int, int, dict]],
        show_all_nodes: bool,
    ) -> folium.Map:
        if not nodes_to_render and not segments_to_render:
            # Empty selection — return a blank world map rather than
            # crashing on bounds computation.
            return folium.Map(location=[0, 0], zoom_start=2)

        # Use ALL rendered node coords to compute bounds, plus segment
        # geometry coords for segments whose endpoints aren't in the
        # render set (rare, but happens for map_bbox edge spillover).
        lats: list[float] = []
        lons: list[float] = []
        for n in nodes_to_render:
            d = self.G.nodes[n]
            lats.append(d["y"])
            lons.append(d["x"])

        sw = (min(lats), min(lons))
        ne = (max(lats), max(lons))
        m = folium.Map(location=[(sw[0] + ne[0]) / 2, (sw[1] + ne[1]) / 2])
        m.fit_bounds([sw, ne])

        segment_geometry = self.G.graph.get("segment_geometry", {})

        for sid, (u, v, data) in segments_to_render.items():
            component_idx = self._node_to_component.get(u, -1)
            color = self._color_for(component_idx)

            coords = segment_geometry.get(sid)
            if coords:
                line_coords = [(lat, lon) for lon, lat in coords]
            else:
                line_coords = [
                    (self.G.nodes[u]["y"], self.G.nodes[u]["x"]),
                    (self.G.nodes[v]["y"], self.G.nodes[v]["x"]),
                ]

            folium.PolyLine(
                line_coords,
                color=color,
                weight=5,
                opacity=0.8,
                popup=folium.Popup(
                    _edge_popup_html(sid, u, v, data, component_idx),
                    max_width=360,
                ),
            ).add_to(m)


        for n in nodes_to_render:
            if not show_all_nodes and not _is_intersection(self.G, n):
                continue
            component_idx = self._node_to_component.get(n, -1)
            color = self._color_for(component_idx)
            d = self.G.nodes[n]
            folium.CircleMarker(
                location=[d["y"], d["x"]],
                radius=4,
                color=color,
                fill=True,
                fill_color=color,
                fill_opacity=1.0,
                popup=folium.Popup(
                    _node_popup_html(n, d, self.G, component_idx),
                    max_width=320,
                ),
            ).add_to(m)

        return m

    @staticmethod
    def _color_for(component_idx: int) -> str:
        if component_idx < 0:
            return "#7f7f7f"
        return _COMPONENT_COLORS[component_idx % len(_COMPONENT_COLORS)]


# ----------------------------------------------------------------------
# Module helpers
# ----------------------------------------------------------------------


def _point_in_bbox(lat: float, lon: float, bbox: BBox) -> bool:
    return bbox.south <= lat <= bbox.north and bbox.west <= lon <= bbox.east


def _is_intersection(G: nx.DiGraph, node: int) -> bool:
    """True if the node is a real intersection or dead-end.

    Uses the underlying-undirected degree: a degree-2 node is a
    geometry node along the middle of a way, anything else is either
    an intersection (≥3) or a dead-end (1) or an island (0). This
    misses the rare case of two distinct ways meeting end-to-end at
    a single shared node with no branching — those will be hidden
    unless show_all_nodes=True.
    """
    neighbors = set(G.predecessors(node)) | set(G.successors(node))
    return len(neighbors) != 2


def _segment_to_way_id(segment_id: str) -> int | None:
    """Extract the OSM way id from a segment_id of the form
    '{way_id}_{start_node_id}_{end_node_id}'.

    Returns None if the segment_id doesn't parse, which keeps the
    popup rendering robust against future schema changes.
    """
    try:
        return int(segment_id.split("_", 1)[0])
    except (ValueError, AttributeError):
        return None


def _node_popup_html(
    node_id: int, node_data: dict, G: nx.DiGraph, component_idx: int
) -> str:
    osm_url = _OSM_NODE_URL.format(node_id=node_id)
    return (
        f"<b>Node {node_id}</b><br>"
        f"Lat: {node_data['y']:.6f}<br>"
        f"Lon: {node_data['x']:.6f}<br>"
        f"Component: {component_idx}<br>"
        f"In/out degree: {G.in_degree(node_id)}/{G.out_degree(node_id)}<br>"
        f"<a href='{osm_url}' target='_blank'>View on OSM</a>"
    )


# def _edge_popup_html(
#     segment_id: str, u: int, v: int, data: dict, component_idx: int
# ) -> str:
#     way_id = _segment_to_way_id(segment_id)
#     osm_link = (
#         f"<a href='{_OSM_WAY_URL.format(way_id=way_id)}' target='_blank'>"
#         f"View way {way_id} on OSM</a>"
#         if way_id is not None
#         else "(no OSM way link)"
#     )

#     lines = [
#         f"<b>Segment {segment_id}</b>",
#         f"Component: {component_idx}",
#         f"Endpoints: {u} → {v}",
#     ]
#     if data.get("name"):
#         lines.append(f"Name: {data['name']}")
#     if data.get("highway"):
#         lines.append(f"Highway: {data['highway']}")
#     if data.get("infra_type"):
#         lines.append(f"Infra type: {data['infra_type']}")
#     if data.get("length_m") is not None:
#         lines.append(f"Length: {data['length_m']:.1f} m")
#     if data.get("stress_cost") is not None:
#         lines.append(f"Stress cost: {data['stress_cost']:.3f}")
#     lines.append(f"Direction: {'forward' if data.get('forward', True) else 'backward'}")
#     lines.append(osm_link)
#     return "<br>".join(lines)

def _edge_popup_html(
    segment_id: str, u: int, v: int, data: dict, component_idx: int
) -> str:
    way_id = _segment_to_way_id(segment_id)
    osm_link = (
        f"<a href='{_OSM_WAY_URL.format(way_id=way_id)}' target='_blank'>"
        f"View way {way_id} on OSM</a>"
        if way_id is not None
        else "(no OSM way link)"
    )

    # Identity block
    lines = [
        f"<b>Segment {segment_id}</b>",
        f"Component: {component_idx} &nbsp;|&nbsp; "
        f"Direction: {'forward' if data.get('forward', True) else 'backward'}",
        f"Endpoints: {u} → {v}",
    ]
    if data.get("name"):
        lines.append(f"Name: {data['name']}")
    if data.get("highway"):
        lines.append(f"Highway: {data['highway']}")
    if data.get("infra_type"):
        lines.append(f"Infra type: {data['infra_type']}")

    # Cost block
    cost_lines = ["<b>Costs</b>"]
    if data.get("length_m") is not None:
        cost_lines.append(f"Length: {data['length_m']:.1f} m")
    if data.get("stress_cost") is not None:
        cost_lines.append(f"Stress cost (total): {data['stress_cost']:.3f}")
    for label, key in [
        ("Physical", "physical_cost"),
        ("Intersection", "intersection_cost"),
        ("Crash", "crash_cost"),
    ]:
        v_ = data.get(key)
        if v_ is not None:
            cost_lines.append(f"&nbsp;&nbsp;{label}: {v_:.3f}")

    # Factor block — only show factors that aren't 1.0 (unset/identity)
    # to keep the popup short for the common case.
    factor_lines = []
    for label, key in [
        ("Speed", "speed_factor"),
        ("Road type", "road_type_factor"),
        ("Infrastructure", "infrastructure_factor"),
        ("Tunnel", "tunnel_factor"),
        ("Surface", "surface_factor"),
        ("Lighting", "lighting_factor"),
    ]:
        v_ = data.get(key)
        # if v_ is not None and not _is_identity(v_):
        if v_ is not None:
            factor_lines.append(f"&nbsp;&nbsp;{label}: {v_:.3f}")
    if factor_lines:
        factor_lines.insert(0, "<b>Factors</b>")

    sections = ["<br>".join(lines), "<br>".join(cost_lines)]
    if factor_lines:
        sections.append("<br>".join(factor_lines))
    sections.append(osm_link)
    return "<br><br>".join(sections)


def _is_identity(value: float) -> bool:
    """True if a factor is effectively 1.0 (the no-op multiplier)."""
    return abs(value - 1.0) < 1e-6
