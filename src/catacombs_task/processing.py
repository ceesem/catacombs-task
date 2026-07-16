"""Ossify-specific skeleton assembly.

This module holds all of the ``ossify`` logic for a single neuron. It is kept
separate from ``task.py`` (which owns the task-queue plumbing) so the actual
work reads as plain functions that can be imported, tested, and run in a
notebook without touching task-queue at all.

The pipeline for one ``root_id`` is:

    assemble_cell  ->  extract_features  ->  upload_cell

``assemble_cell`` fetches a precomputed "L2" skeleton from the CAVE skeleton
service and rehydrates it into an :class:`ossify.Cell` (skeleton + L2 graph +
pre/post synapse annotations). ``extract_features`` decorates the skeleton with
derived, per-vertex features. ``upload_cell`` serializes the cell to the native
``.osy`` format and writes it to the output location via CloudFiles.
"""

from typing import Optional

import ossify
from ossify.compartments import ProbaVertexModel, StructuredLabeler, TransitionSchema

# Reference tables merged into the synapse annotations. The original meshwork
# pipeline attached these to the post side only via a ``{"post": ...}`` dict;
# ossify applies each reference table to whichever synapses it matches (by
# synapse id), so a flat list of table names is the equivalent input.
DEFAULT_REFERENCE_TABLES = []

COMPARTMENT_SCHEMA = TransitionSchema(
    classes=["dendrite", "axon"],
    transitions={
        ("axon", "dendrite"): 50,
        ("dendrite", "axon"): 2,
    },
    root_classes=["dendrite"],
)


def assemble_cell(
    root_id: int,
    client,
    *,
    restore_properties: bool = True,
    reference_tables: Optional[list[str]] = None,
    reference_suffixes: Optional[dict] = None,
    skeleton_version: int = 4,
) -> ossify.Cell:
    """Rehydrate a precomputed CAVE skeleton into an ossify ``Cell``.

    This is the ossify replacement for ``pcg_skel.get_meshwork_from_client``.
    It fetches the skeleton (vertices, edges, radius, compartment,
    ``mesh_to_skel_map``, ``lvl2_ids``) from the skeleton service and assembles:

    - a ``skeleton`` layer with ``radius`` and ``compartment`` features,
    - a ``graph`` layer of L2 vertices. With ``restore_properties=True`` this
      carries the L2-cache volumetric properties (area, volume, distance
      transform) that the meshwork pipeline added via ``add_volumetric_properties``,
    - ``pre_syn`` / ``post_syn`` point annotations linked to the L2 graph.

    ``restore_graph`` is left False (matching the original): L2 graph edges are
    not fetched, which keeps assembly fast. The mesh is intentionally not loaded.

    ``reference_suffixes`` maps a reference table name to the suffix used for the
    columns it contributes (feeding downstream feature extraction); ossify falls
    back to the table name for any table not in the map. We coerce ``None`` to an
    empty dict before passing it down, so callers can omit it safely regardless of
    the ossify version.
    """
    if reference_tables is None:
        reference_tables = DEFAULT_REFERENCE_TABLES

    return ossify.load_cell_from_client(
        root_id,
        client,
        synapses=True,
        reference_tables=reference_tables,
        reference_suffixes=reference_suffixes or {},
        restore_graph=False,
        restore_properties=restore_properties,
        omit_self_synapses=True,
        skeleton_version=skeleton_version,
    )


def assemble_cell_batch(
    root_ids: list[int],
    client,
    *,
    restore_properties: bool = True,
    reference_tables: Optional[list[str]] = None,
    reference_suffixes: Optional[dict] = None,
    skeleton_version: int = 4,
    timestamp=None,
) -> dict[int, ossify.Cell]:
    """Rehydrate many CAVE skeletons in one pooled batch (the ``ossify`` 0.2.1 path).

    The batch equivalent of :func:`assemble_cell`: it uses the same ossify options
    but pools the synapse and L2-property fetches across the whole batch into a few
    queries (~1.5x throughput at N=10), returning ``{root_id: Cell}``.

    Two behaviours differ from the single-cell loader and are deliberate:

    - Skeletons are only ever *downloaded*, never generated inline: ossify 0.2.1
      fetches cached skeletons via ``client.skeleton.fetch_skeletons`` (default
      ``skeleton_download_method="gcs"``, pulling H5 files straight from the storage
      bucket with a downscoped token, which bypasses the skeleton service's bulk
      rate limit). This assumes a prior ``generate_bulk_skeletons_async`` pre-pass
      (see ``generate_skeletons.py``); roots whose skeleton is not yet cached are
      simply absent from the result.
    - ``skip_invalid=True`` + one shared ``timestamp`` — all roots are validated
      against a single timestamp; those not valid at it are dropped rather than
      raising, so one bad root never sinks the batch. Roots dropped for either
      reason are absent from the returned dict; the caller (``_run_task``)
      re-classifies them (permanent-invalid vs skeleton-not-ready-yet).
    """
    if reference_tables is None:
        reference_tables = DEFAULT_REFERENCE_TABLES

    return ossify.load_cell_batch_from_client(
        root_ids,
        client,
        synapses=True,
        reference_tables=reference_tables,
        reference_suffixes=reference_suffixes or {},
        restore_graph=False,
        restore_properties=restore_properties,
        omit_self_synapses=True,
        skeleton_version=skeleton_version,
        timestamp=timestamp,
        skip_invalid=True,
        # Pin the GCS-direct download (H5 from the bucket via a downscoped token)
        # rather than relying on the default: it bypasses the skeleton service's
        # bulk request rate limit, which is what makes the fan-out viable.
        skeleton_download_method="gcs",
    )


def extract_features(cell: ossify.Cell) -> ossify.Cell:
    """Attach derived per-vertex skeleton features (mutates and returns ``cell``).

    ossify computes topology-derived quantities on demand rather than baking
    them into the loader, so this is where the pipeline chooses which analyses
    to persist alongside the raw skeleton. As shipped it stores the Strahler
    number (part of the old meshwork ``segment_properties``). Other analyses in
    ``ossify.algorithms`` (branch order, axon labeling, synapse betweenness,
    segment aggregates) can be added the same way; the raw radius and L2
    volumetric data needed to compute the rest are already present in the cell.
    """
    strahler = ossify.algorithms.strahler_number(cell)
    cell.skeleton.add_feature(strahler, "strahler")
    return cell


def assign_compartments(cell: ossify.Cell, compartment_model: str) -> ossify.Cell:
    """Label skeleton vertices axon/dendrite (mutates and returns ``cell``).

    ``compartment_model`` is the config for an ``ossify`` ``ProbaVertexModel``
    (a path or name understood by ``ProbaVertexModel.from_config``); it is set
    per-run via the ``COMPARTMENT_MODEL`` profile value and travels in the
    ``compartment_model`` task column. Skipped entirely when unset (see
    ``_run_task``). Adds an ``is_axon`` boolean feature to the skeleton.

    The transition rules (``COMPARTMENT_SCHEMA``) are a module-level default; lift
    them into config the same way if a run ever needs to override them.
    """
    vertex_model = ProbaVertexModel.from_config(compartment_model)

    labeler = StructuredLabeler(
        vertex_model,
        COMPARTMENT_SCHEMA,
        absorb_min_size=10,
    )
    labels = labeler.predict(cell)
    cell.skeleton.add_feature(labels == "axon", "is_axon")

    return cell


def upload_cell(filename: str, cell: ossify.Cell, cloudpath: str) -> None:
    """Serialize ``cell`` to the ossify ``.osy`` format and upload it.

    ``save_cell`` writes an uncompressed tar of feather/npz/json parts. We write
    to an in-memory buffer and push the bytes through the caller's CloudFiles
    handle so uploads share the same credentials and path as the idempotency
    check and error sidecars in ``task.py``.
    """
    ossify.save_cell(cell, f"{cloudpath}/{filename}")
