"""
Thin client around vLLM's OpenAI-compatible server. Works the same way
regardless of which open-source VLM is being served (Llama, DeepSeek-VL2,
Qwen3-VL, etc.) — only model_config changes.
"""

import base64
import json
import re

from openai import OpenAI


def encode_image(image_path: str) -> str:
    with open(image_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def _build_image_message(system_prompt: str, user_prompt: str, image_b64: str) -> list:
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


def _extract_json(text: str) -> dict:
    """Parse JSON even if the model wrapped it in markdown fences or added
    stray text around it — common with smaller open-source VLMs that don't
    always follow 'JSON only' instructions as reliably as larger models."""
    text = text.strip()
    fence_match = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.DOTALL)
    if fence_match:
        text = fence_match.group(1)
    else:
        start, end = text.find("{"), text.rfind("}")
        if start != -1 and end != -1:
            text = text[start:end + 1]
    return json.loads(text)


def call_vlm_text(
    model_config: dict,
    image_path: str,
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = 600,
    temperature: float = 0.0,
) -> str:
    """Free-text call (used for Stage 4 Calls 1 and 2 — opposing readings).
    No JSON schema is enforced here; the output is plain prose."""
    client = OpenAI(base_url=model_config["base_url"], api_key="EMPTY")
    image_b64 = encode_image(image_path)
    messages = _build_image_message(system_prompt, user_prompt, image_b64)

    response = client.chat.completions.create(
        model=model_config["model_id"],
        messages=messages,
        max_tokens=max_tokens,
        temperature=temperature,
    )
    return response.choices[0].message.content.strip()


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
    """Structured call that returns parsed JSON matching json_schema.
    Uses vLLM's response_format (JSON Schema) when the served model
    supports it; otherwise falls back to plain prompting + JSON repair."""

    client = OpenAI(base_url=model_config["base_url"], api_key="EMPTY")
    image_b64 = encode_image(image_path)
    messages = _build_image_message(system_prompt, user_prompt, image_b64)

    kwargs = dict(
        model=model_config["model_id"],
        messages=messages,
        max_tokens=max_tokens,
        temperature=temperature,
    )

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
        # One repair attempt: ask the same model to fix its own formatting.
        repair_messages = messages + [
            {"role": "assistant", "content": raw_text},
            {
                "role": "user",
                "content": "Your previous response is not valid JSON. Respond again with ONLY "
                "the JSON object, with no additional text or code blocks.",
            },
        ]
        repair_kwargs = dict(kwargs)
        repair_kwargs["messages"] = repair_messages
        repair_response = client.chat.completions.create(**repair_kwargs)
        return _extract_json(repair_response.choices[0].message.content)