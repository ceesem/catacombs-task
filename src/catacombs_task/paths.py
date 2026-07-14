"""Filesystem locations derived from a config profile.

Centralizing these here (instead of repeating literal paths in build_tasks.py and
insert_tasks.py) makes the profile name the single source of truth: the task
feather always lives under the profile that owns it, so copying a profile to
start a new run needs no path edits and the writer and reader can never disagree.
"""

import os


def task_feather_path(profile: str) -> str:
    """Path to a profile's task feather.

    Defaults to ``config/<profile>/tasks/<profile>.feather`` so the location
    follows the profile directory automatically. Set ``TASK_PARAMETER_FILENAME``
    in the profile's ``task.env`` to override (e.g. to read a feather from
    elsewhere); the override must still end in ``.feather``.
    """
    override = os.environ.get("TASK_PARAMETER_FILENAME")
    if override:
        if not override.endswith(".feather"):
            raise ValueError("TASK_PARAMETER_FILENAME must end in .feather")
        return override
    return os.path.join("config", profile, "tasks", f"{profile}.feather")
