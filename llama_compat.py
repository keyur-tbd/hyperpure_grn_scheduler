#!/usr/bin/env python3
"""One way to get an extraction agent, whichever LlamaCloud API the account is on.

LlamaCloud replaced named *extraction agents* with named *configurations*. The
two generations are reached through different packages, and those packages
cannot be installed side by side:

    v1   llama-cloud-services  ->  pins llama-cloud==0.1.46
         LlamaExtract().get_agent(name=...)
         GET /api/v1/extraction/extraction-agents/by-name/{name}

    v2   llama-cloud>=2.15.0
         configurations.list(name=...) -> files.create() -> extract.create()
         POST /api/v2/extract

So a repo is on one or the other, never both, and `pip install` decides which.
This module hides that choice behind build_agent(), which returns an object
with the same `.extract(path)` method and the same `.data` on the result that
app.py and supabase_sink.py already expect. Nothing downstream changes.

Which API is used comes from LLAMA_EXTRACT_API (v1 or v2), defaulting to v1 so
that every repo still on llama-cloud-services keeps its existing behaviour when
this file is copied across. Only the repo whose requirements.txt has been
switched to llama-cloud should set LLAMA_EXTRACT_API=v2.

Background: on 2026-08-31 the Hyperpure/NB agents were rebuilt in a new
LlamaCloud account that only offers v2 configurations, so calls to the v1
by-name endpoint returned:

    404 {'detail': 'Extraction agent Hyperpure Agent not found or user does
         not have access.'}
"""
import logging
import os
import time
from typing import Any, Optional

logger = logging.getLogger('llama_compat')

DEFAULT_API = 'v1'

# v2 uploads the file before extracting; the API wants a purpose string. If a
# future release rejects this value the error names the allowed set, so it is
# an env var rather than a constant to edit.
DEFAULT_FILE_PURPOSE = 'extract'

# Ceiling on one document's extraction, not the whole run. supabase_sink's own
# run budget is what stops a run; this stops a single stuck job.
DEFAULT_EXTRACT_TIMEOUT = 600.0


def which_api(explicit: Optional[str] = None) -> str:
    """v1 or v2, from the argument, then the environment, then the default."""
    value = (explicit or os.environ.get('LLAMA_EXTRACT_API') or DEFAULT_API).strip().lower()
    if value not in ('v1', 'v2'):
        raise ValueError(f"LLAMA_EXTRACT_API must be 'v1' or 'v2', not {value!r}")
    return value


class ExtractionResult:
    """Carries the payload on `.data`, the way a v1 agent result did.

    app.py reads `extraction_result.data` and supabase_sink.normalize_extraction
    unwraps `.data` too, so presenting the same attribute keeps both callers
    working unchanged.
    """

    __slots__ = ('data', 'job_id')

    def __init__(self, data: Any, job_id: Optional[str] = None):
        self.data = data
        self.job_id = job_id

    def __repr__(self) -> str:
        kind = type(self.data).__name__
        size = len(self.data) if isinstance(self.data, (list, dict)) else '?'
        return f'<ExtractionResult {kind} of {size} job={self.job_id}>'


class V2Agent:
    """A v1-shaped agent backed by the v2 configuration + extract-job API.

    Resolving the configuration is done once, at construction, so a bad name
    fails immediately with a clear message rather than once per document.
    """

    def __init__(self, client: Any, configuration_id: str, name: str,
                 purpose: str = DEFAULT_FILE_PURPOSE,
                 timeout: float = DEFAULT_EXTRACT_TIMEOUT):
        self._client = client
        self.configuration_id = configuration_id
        self.name = name
        self._purpose = purpose
        self._timeout = timeout

    def extract(self, file_path: str) -> ExtractionResult:
        """Upload, extract, wait. Raises on anything short of a completed job.

        v1 took a local path directly; v2 needs the file uploaded first and
        returns a job to poll, so both steps live here rather than leaking into
        every caller.
        """
        started = time.time()
        with open(file_path, 'rb') as handle:
            uploaded = self._client.files.create(file=handle, purpose=self._purpose)

        job = self._client.extract.create(
            file_input=uploaded.id, configuration_id=self.configuration_id)
        finished = self._client.extract.wait_for_completion(
            job.id, timeout=self._timeout)

        status = (getattr(finished, 'status', '') or '').upper()
        if status != 'COMPLETED':
            detail = getattr(finished, 'error_message', None) or 'no error message given'
            raise RuntimeError(
                f'extraction job {job.id} finished as {status or "UNKNOWN"}: {detail}')

        logger.info('[LLAMA] %s extracted in %.1fs (job %s)',
                    os.path.basename(file_path), time.time() - started, job.id)
        # extract_result is the schema-conforming payload: an object for
        # per_doc, a list for per_page / per_table_row. Passed through as-is so
        # the shape matches whatever the configuration was set up to return.
        return ExtractionResult(getattr(finished, 'extract_result', None), job_id=job.id)


def _v2_client(api_key: str) -> Any:
    try:
        from llama_cloud import LlamaCloud
    except ImportError as exc:                       # pragma: no cover
        raise ImportError(
            'LLAMA_EXTRACT_API=v2 needs the llama-cloud package (>=2.15.0). '
            'Note it cannot be installed alongside llama-cloud-services, which '
            'pins llama-cloud==0.1.46 -- replace it in requirements.txt.') from exc
    return LlamaCloud(api_key=api_key)


def resolve_configuration(client: Any, name: str) -> str:
    """Configuration id for `name`, or a clear error naming what was found.

    configurations.list paginates; latest_only collapses the versions of one
    configuration to the newest, which is the v1 by-name behaviour.
    """
    matches = list(client.configurations.list(name=name, latest_only=True))
    if not matches:
        available = []
        try:
            for item in client.configurations.list(latest_only=True):
                available.append(getattr(item, 'name', '?'))
                if len(available) >= 20:
                    break
        except Exception:                            # pragma: no cover - best effort
            pass
        raise RuntimeError(
            f'no LlamaCloud configuration named {name!r} in this account/project.'
            + (f' Available: {", ".join(available)}' if available
               else ' No configurations were visible to this API key at all.'))
    if len(matches) > 1:
        logger.warning('[LLAMA] %d configurations named %r; using the first',
                       len(matches), name)
    return matches[0].id


def build_agent(name: str, api_key: Optional[str] = None, api: Optional[str] = None,
                timeout: float = DEFAULT_EXTRACT_TIMEOUT) -> Any:
    """Return something with .extract(path), from whichever API is selected."""
    api = which_api(api)
    api_key = (api_key or os.environ.get('LLAMA_CLOUD_API_KEY')
               or os.environ.get('LLAMA_API_KEY') or '').strip()
    if not api_key:
        raise RuntimeError('No LlamaCloud API key: set LLAMA_CLOUD_API_KEY or LLAMA_API_KEY.')
    # Both generations read this variable, and app.py sets it too.
    os.environ['LLAMA_CLOUD_API_KEY'] = api_key

    if api == 'v1':
        from llama_cloud_services import LlamaExtract
        agent = LlamaExtract().get_agent(name=name)
        if agent is None:
            raise RuntimeError(f"LlamaExtract agent {name!r} not found (v1 API)")
        logger.info('[LLAMA] v1 agent %r ready', name)
        return agent

    client = _v2_client(api_key)
    configuration_id = resolve_configuration(client, name)
    logger.info('[LLAMA] v2 configuration %r ready (id %s)', name, configuration_id)
    return V2Agent(client, configuration_id, name,
                   purpose=os.environ.get('LLAMA_FILE_PURPOSE', DEFAULT_FILE_PURPOSE),
                   timeout=timeout)


def check(name: str, api: Optional[str] = None) -> bool:
    """Resolve the agent/configuration and report, without extracting anything.

    Cheap enough to run in CI or by hand before a real run: it proves the key,
    the account and the name all line up, which is the part that broke.
    """
    api = which_api(api)
    print(f'LlamaCloud API : {api}')
    print(f'agent/config   : {name}')
    try:
        agent = build_agent(name, api=api)
    except Exception as exc:
        print(f'RESULT         : FAILED -- {exc}')
        return False
    extra = f' (id {agent.configuration_id})' if isinstance(agent, V2Agent) else ''
    print(f'RESULT         : OK, resolved{extra}')
    return True


if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        print(__doc__)
        print('usage: python llama_compat.py "<agent or configuration name>" [v1|v2]')
        raise SystemExit(2)
    raise SystemExit(0 if check(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None) else 1)
