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
from openai import OpenAI


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def encode_image(image_path: str) -> str:
    with open(image_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def _extract_json(text: str) -> dict:
    """Parse JSON even if the model wrapped it in markdown fences or added
    stray text around it."""
    text = text.strip()
    fence_match = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.DOTALL)
    if fence_match:
        text = fence_match.group(1)
    else:
        start, end = text.find("{"), text.rfind("}")
        if start != -1 and end != -1:
            text = text[start:end + 1]
    return json.loads(text)


# ---------------------------------------------------------------------------
# OpenAI-compatible backend (vLLM + Ollama)
# ---------------------------------------------------------------------------

def _openai_build_messages(system_prompt: str, user_prompt: str, image_b64: str) -> list:
    return [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": [
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_b64}"}},
                {"type": "text", "text": user_prompt},
            ],
        },
    ]


def _call_openai_text(model_config, image_path, system_prompt, user_prompt, max_tokens, temperature) -> str:
    client = OpenAI(base_url=model_config["base_url"], api_key="EMPTY")
    image_b64 = encode_image(image_path)
    messages = _openai_build_messages(system_prompt, user_prompt, image_b64)
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
    messages = _openai_build_messages(system_prompt, user_prompt, image_b64)

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
        return _extract_json(raw_text)
    except json.JSONDecodeError:
        repair_messages = messages + [
            {"role": "assistant", "content": raw_text},
            {"role": "user", "content": "Your previous response is not valid JSON. "
             "Respond again with ONLY the JSON object, no code blocks or extra text."},
        ]
        repair_kwargs = dict(kwargs)
        repair_kwargs["messages"] = repair_messages
        repair_response = client.chat.completions.create(**repair_kwargs)
        return _extract_json(repair_response.choices[0].message.content)


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
                    "media_type": "image/jpeg",
                    "data": image_b64,
                }},
                {"type": "text", "text": user_prompt},
            ],
        }],
    )
    return response.content[0].text.strip()


def _call_anthropic_json(model_config, image_path, system_prompt, user_prompt,
                         json_schema, schema_name, max_tokens, temperature) -> dict:
    # Claude reliably follows "respond only with JSON" without needing schema
    # enforcement — the same extraction + repair fallback is sufficient.
    raw_text = _call_anthropic_text(
        model_config, image_path, system_prompt, user_prompt, max_tokens, temperature
    )
    try:
        return _extract_json(raw_text)
    except json.JSONDecodeError:
        client = _get_anthropic_client()
        image_b64 = encode_image(image_path)
        repair_response = client.messages.create(
            model=model_config["model_id"],
            max_tokens=max_tokens,
            temperature=temperature,
            system=system_prompt,
            messages=[
                {"role": "user", "content": [
                    {"type": "image", "source": {
                        "type": "base64", "media_type": "image/jpeg", "data": image_b64,
                    }},
                    {"type": "text", "text": user_prompt},
                ]},
                {"role": "assistant", "content": raw_text},
                {"role": "user", "content": "Your previous response is not valid JSON. "
                 "Respond again with ONLY the JSON object, no code blocks or extra text."},
            ],
        )
        return _extract_json(repair_response.content[0].text)


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
