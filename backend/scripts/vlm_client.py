"""
Client layer for all VLM backends. Supports two providers:

- "anthropic": calls the Anthropic Messages API directly (for Claude models).
  Requires ANTHROPIC_API_KEY in the environment (set it in the .env file at
  the root of the project — docker-compose already passes it to the container
  via env_file: .env).

- "openai_compat" (default): calls any OpenAI-compatible endpoint, covering
  both vLLM servers (for open-source models on GPU) and Ollama (for local
  Mac testing). No API key needed — vLLM accepts "EMPTY".

The pipeline (pipeline.py) never calls this module directly by provider name:
it just calls call_vlm_text() or call_vlm_json(), and the routing happens here
based on the "provider" field of the model_config dict.
"""

import base64
import json
import os
import re

import anthropic as _anthropic_sdk
from json_repair import repair_json
from openai import OpenAI


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def encode_image(image_path: str) -> str:
    with open(image_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


_MEDIA_TYPES = {
    ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
    ".png": "image/png", ".webp": "image/webp",
}


def get_media_type(image_path: str) -> str:
    ext = os.path.splitext(image_path)[1].lower()
    if ext not in _MEDIA_TYPES:
        raise ValueError(
            f"Unsupported image format '{ext}' for {image_path}. "
            f"Supported: {list(_MEDIA_TYPES)}"
        )
    return _MEDIA_TYPES[ext]


def _extract_json(text: str) -> dict:
    """Parse JSON even if the model wrapped it in markdown fences, added
    stray text around it, or produced near-valid JSON with issues like
    unescaped quotes inside a text field (e.g. quoting an artwork title:
    "the painting "Los mulatos" shows..." breaks strict JSON syntax).

    Three attempts, cheapest and strictest first:
      1. Plain json.loads on the fenced/brace-extracted text.
      2. json_repair, which specifically targets common LLM JSON mistakes
         (unescaped quotes, trailing commas, truncated output) without
         requiring another model call.
      3. If both fail, the caller (call_vlm_json) falls back to asking the
         model to redo its answer as a last resort.
    """
    text = text.strip()
    fence_match = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.DOTALL)
    if fence_match:
        text = fence_match.group(1)
    else:
        start, end = text.find("{"), text.rfind("}")
        if start != -1 and end != -1:
            text = text[start:end + 1]

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        repaired = repair_json(text)
        return json.loads(repaired)


def _ensure_schema_shape(parsed: dict, json_schema: dict) -> dict:
    """Fixes a common LLM slip: dropping the single top-level wrapper key
    a schema expects (e.g. returning {"description": ..., "donor_present":
    ...} instead of {"stage_3_narrative": {"description": ..., ...}}).

    If the schema has exactly one required top-level key and the parsed
    JSON is missing it, wrap the whole object under that key — no extra
    API call needed. Deliberately does NOT require every inner field to
    be present first: models often omit an empty list field (e.g.
    "csv_conflicts") entirely rather than writing "csv_conflicts": [],
    and a schema with a single top-level key is only ever used for these
    stage-output objects, so there's no ambiguity about what to wrap."""
    required = json_schema.get("required", [])
    if len(required) != 1:
        return parsed
    key = required[0]
    if key in parsed:
        return parsed
    return {key: parsed}


# ---------------------------------------------------------------------------
# OpenAI-compatible backend (vLLM + Ollama)
# ---------------------------------------------------------------------------

def _openai_build_messages(system_prompt: str, user_prompt: str, image_b64: str, media_type: str) -> list:
    return [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": [
                {"type": "image_url", "image_url": {"url": f"data:{media_type};base64,{image_b64}"}},
                {"type": "text", "text": user_prompt},
            ],
        },
    ]


def _call_openai_text(model_config, image_path, system_prompt, user_prompt, max_tokens, temperature) -> str:
    client = OpenAI(base_url=model_config["base_url"], api_key="EMPTY")
    image_b64 = encode_image(image_path)
    messages = _openai_build_messages(system_prompt, user_prompt, image_b64, get_media_type(image_path))
    response = client.chat.completions.create(
        model=model_config["model_id"],
        messages=messages,
        max_tokens=max_tokens,
        temperature=temperature,
    )
    return response.choices[0].message.content.strip()


def _call_openai_json(model_config, image_path, system_prompt, user_prompt,
                      json_schema, schema_name, max_tokens, temperature) -> dict:
    client = OpenAI(base_url=model_config["base_url"], api_key="EMPTY")
    image_b64 = encode_image(image_path)
    messages = _openai_build_messages(system_prompt, user_prompt, image_b64, get_media_type(image_path))

    kwargs = dict(model=model_config["model_id"], messages=messages,
                  max_tokens=max_tokens, temperature=temperature)

    if model_config.get("supports_structured_output"):
        kwargs["response_format"] = {
            "type": "json_schema",
            "json_schema": {"name": schema_name, "schema": json_schema},
        }

    response = client.chat.completions.create(**kwargs)
    raw_text = response.choices[0].message.content

    try:
        parsed = _extract_json(raw_text)
    except json.JSONDecodeError:
        repair_messages = messages + [
            {"role": "assistant", "content": raw_text},
            {"role": "user", "content": "Your previous response is not valid JSON. "
             "Respond again with ONLY the JSON object, no code blocks or extra text."},
        ]
        repair_kwargs = dict(kwargs)
        repair_kwargs["messages"] = repair_messages
        repair_response = client.chat.completions.create(**repair_kwargs)
        parsed = _extract_json(repair_response.choices[0].message.content)

    return _ensure_schema_shape(parsed, json_schema)


# ---------------------------------------------------------------------------
# Anthropic backend (Claude)
# ---------------------------------------------------------------------------

def _get_anthropic_client():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "ANTHROPIC_API_KEY not found. Add it to your .env file at the root "
            "of the project (same file that holds DRIVE_PATH and HF_TOKEN)."
        )
    return _anthropic_sdk.Anthropic(api_key=api_key)


def _call_anthropic_text(model_config, image_path, system_prompt, user_prompt,
                         max_tokens, temperature) -> str:
    client = _get_anthropic_client()
    image_b64 = encode_image(image_path)

    response = client.messages.create(
        model=model_config["model_id"],
        max_tokens=max_tokens,
        temperature=temperature,
        system=system_prompt,
        messages=[{
            "role": "user",
            "content": [
                {"type": "image", "source": {
                    "type": "base64",
                    "media_type": get_media_type(image_path),
                    "data": image_b64,
                }},
                {"type": "text", "text": user_prompt},
            ],
        }],
    )
    return response.content[0].text.strip()


def _malformed_object_fields(parsed: dict, json_schema: dict) -> list:
    """Occasionally, for schemas with a large/deeply nested object property,
    Claude's tool-use input serializes that field as a raw (and often
    bracket-mismatched) JSON string instead of true nested structure —
    a formatting slip rather than a systematic one. Attempts to repair each
    such field in place via _extract_json; a field is only accepted as
    repaired if the result actually contains that sub-schema's required
    keys, since a bracket-mismatched string can lead json_repair to produce
    a wrong-shaped (but validly-parsing) result. Returns the list of
    top-level keys that are still malformed after the repair attempt."""
    bad = []
    for key, spec in json_schema.get("properties", {}).items():
        if spec.get("type") != "object":
            continue
        value = parsed.get(key)
        if isinstance(value, dict):
            continue
        if isinstance(value, str):
            try:
                fixed = _extract_json(value)
            except (json.JSONDecodeError, ValueError):
                fixed = None
            if fixed is not None and all(k in fixed for k in spec.get("required", [])):
                parsed[key] = fixed
                continue
        bad.append(key)
    return bad


def _call_anthropic_json(model_config, image_path, system_prompt, user_prompt,
                         json_schema, schema_name, max_tokens, temperature) -> dict:
    """Uses Claude's forced tool-use mechanism instead of prompting for JSON
    and parsing free text. This is the robust fix, not another parsing
    patch: with tool_choice forcing a specific tool, the Anthropic API
    itself guarantees the returned "input" matches json_schema exactly —
    every wrapper key, every required field — because the model is
    filling a function call, not writing prose that might drift from
    instructions. This removes the whole class of bugs we were chasing
    (dropped wrapper keys, unescaped quotes, missing optional fields)."""
    client = _get_anthropic_client()
    image_b64 = encode_image(image_path)

    tool = {
        "name": schema_name,
        "description": f"Record the {schema_name} analysis in the exact structure given by the schema.",
        "input_schema": json_schema,
    }
    messages = [{
        "role": "user",
        "content": [
            {"type": "image", "source": {
                "type": "base64",
                "media_type": get_media_type(image_path),
                "data": image_b64,
            }},
            {"type": "text", "text": user_prompt},
        ],
    }]

    def _call():
        return client.messages.create(
            model=model_config["model_id"],
            max_tokens=max_tokens,
            temperature=temperature,
            system=system_prompt,
            tools=[tool],
            tool_choice={"type": "tool", "name": schema_name},
            messages=messages,
        )

    def _extract(response):
        if response.stop_reason == "max_tokens":
            # The tool-use input was cut off mid-generation. Anthropic's
            # partial JSON parser silently returns whatever was successfully
            # parsed up to the cutoff point (often just the first top-level
            # key), which looks like a normal, valid — but incomplete —
            # result. Left unchecked, this fails silently downstream with a
            # confusing KeyError far from the real cause. Fail loudly here
            # instead, with enough detail to fix it by raising max_tokens.
            raise ValueError(
                f"Claude's response for schema '{schema_name}' was cut off "
                f"(stop_reason=max_tokens, limit was {max_tokens}). The "
                f"returned data is incomplete. Raise max_tokens for this call."
            )

        # For schemas with multiple large top-level required keys (e.g.
        # Stage 1's csv_metadata + stage_1_pre_iconographic), Claude
        # sometimes splits its answer across two separate tool_use blocks
        # for the same tool call instead of one combined object — each
        # block.input then only has part of the required keys. Merge every
        # tool_use block's input together so nothing gets silently dropped.
        tool_blocks = [block for block in response.content if block.type == "tool_use"]
        if not tool_blocks:
            raise ValueError(
                f"Claude did not return a tool_use block for schema "
                f"'{schema_name}' despite forced tool_choice — check "
                f"stop_reason: {response.stop_reason}"
            )
        merged = {}
        for block in tool_blocks:
            merged.update(block.input)
        return merged

    # Rare model slip (seen on Haiku, ~1 in 3 tries on content-dense images):
    # a large nested object field comes back as a raw, sometimes
    # bracket-mismatched JSON string instead of true nested structure, and
    # it wasn't repairable with confidence. Each fresh call is an
    # independent draw, so up to two retries (three tries total) brings the
    # odds of hitting it three times running down to roughly 1 in 30.
    merged = _extract(_call())
    bad_fields = _malformed_object_fields(merged, json_schema)
    attempts = 1
    while bad_fields and attempts < 3:
        merged = _extract(_call())
        bad_fields = _malformed_object_fields(merged, json_schema)
        attempts += 1
    if bad_fields:
        raise ValueError(
            f"Claude's tool_use input for schema '{schema_name}' still has "
            f"malformed object field(s) {bad_fields} after {attempts} "
            f"attempts — the nested schema may be too large/complex for "
            f"this model to fill reliably in a single call."
        )

    return merged


# ---------------------------------------------------------------------------
# Public API — the only functions pipeline.py ever calls
# ---------------------------------------------------------------------------

def call_vlm_text(
    model_config: dict,
    image_path: str,
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = 600,
    temperature: float = 0.0,
) -> str:
    if model_config.get("provider") == "anthropic":
        return _call_anthropic_text(model_config, image_path, system_prompt,
                                    user_prompt, max_tokens, temperature)
    return _call_openai_text(model_config, image_path, system_prompt,
                             user_prompt, max_tokens, temperature)


def call_vlm_json(
    model_config: dict,
    image_path: str,
    system_prompt: str,
    user_prompt: str,
    json_schema: dict,
    schema_name: str,
    max_tokens: int = 2000,
    temperature: float = 0.0,
) -> dict:
    if model_config.get("provider") == "anthropic":
        return _call_anthropic_json(model_config, image_path, system_prompt,
                                    user_prompt, json_schema, schema_name,
                                    max_tokens, temperature)
    return _call_openai_json(model_config, image_path, system_prompt,
                             user_prompt, json_schema, schema_name,
                             max_tokens, temperature)