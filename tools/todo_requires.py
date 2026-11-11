"""Model-free `[requires: ...]` gate for shared queue rows, checked against machines/<PC>.json (machine-profile/1).

A row without the tag behaves exactly as before. Tokens (comma separated, case-insensitive):
  gpu            a dedicated (not shared) GPU with VRAM
  vram>=N        dedicated VRAM of at least N GB (decimal, 1e9 bytes: an "8 GB" card or DIMM passes 8)
  ram>=N         RAM of at least N GB
  runtime:NAME   profile.runtimes[NAME] is present and truthy
  form:NAME      profile.formFactor equals NAME
A requirement this PC certainly fails is `unmet`; an unknown token, a missing profile or a missing field is `uncertain`.
Both keep the row out of automatic take and wake. User approval is not a token: use the existing 보류 status cell.
"""
import re

TAG = re.compile(r'\[requires:([^\]]*)\]', re.IGNORECASE)
SIZE = re.compile(r'(vram|ram)>=(\d+(?:\.\d+)?)')


def requirements(summary):
    """None when the row has no tag (unspecified), else the list of tokens (possibly empty)."""
    found = TAG.findall(summary)
    return [token.strip().lower() for part in found for token in part.split(',') if token.strip()] if found else None


def check(token, profile):
    """True met, False unmet, None unknown. Never raises on a malformed profile."""
    def dedicated():
        return [gpu['vramBytes'] for gpu in profile['gpus'] if not gpu.get('shared') and gpu['vramBytes'] > 0]

    try:
        if token in profile.get('capabilities', {}):
            return profile['capabilities'][token]
        gigabytes = re.fullmatch(r'gpu:(\d+(?:\.\d+)?)gb', token)
        if gigabytes:
            return max(dedicated(),default=0) >= float(gigabytes[1]) * 1e9
        size = SIZE.fullmatch(token)
        if token == 'gpu':
            return bool(dedicated())
        if size:
            have = profile['ramBytes'] if size[1] == 'ram' else max(dedicated(), default=0)
            return have >= float(size[2]) * 1e9
        if token.startswith('runtime:'):
            return bool(profile['runtimes'][token[8:]])
        if token.startswith('form:'):
            return profile['formFactor'].lower() == token[5:]
    except (KeyError, TypeError, AttributeError):
        return None
    return None


def blocked_reason(summary, profile):
    """None when the row may run here automatically, else a one-line reason without any PC identity."""
    tokens = requirements(summary)
    if tokens is None:
        return None
    if not tokens:
        return 'requires uncertain: empty tag'
    if not isinstance(profile, dict):
        return 'requires uncertain: no machine profile for this PC'
    results = {token: check(token, profile) for token in tokens}
    unmet = [token for token, result in results.items() if result is False]
    unknown = [token for token, result in results.items() if result is None]
    if unmet:
        return 'requires unmet: ' + ', '.join(unmet)
    return 'requires uncertain: ' + ', '.join(unknown) if unknown else None

