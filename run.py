import os
from urllib.parse import urlparse

from taskqueue import TaskQueue

from catacombs_task.task import run_task

tq = TaskQueue(os.environ.get("TASKQUEUE_PATH"))

is_filequeue = urlparse(os.environ.get("TASKQUEUE_PATH")).scheme == "fq"

tq.poll(
    lease_seconds=os.environ.get("TQ_LEASE_SECONDS", 300),
    verbose=True,
    tally=is_filequeue,
)
