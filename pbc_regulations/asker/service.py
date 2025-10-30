"""Business logic for mocked institutional and legal Q&A services."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, TypedDict, Tuple
from uuid import uuid4

import requests
from urllib.parse import quote

# In-memory session store used purely for mocked interactions.
_SESSION_HISTORY: Dict[str, List[dict[str, str]]] = {}
_LOGGER = logging.getLogger(__name__)

_DEFAULT_LLM_SYSTEM_PROMPT = (
    "You are a legal assistant that helps analysts map user questions to Chinese "
    "laws and regulations. Respond with valid JSON."
)
_DEFAULT_LLM_USER_PROMPT = (
    "根据用户的问题，给出可能相关的法律法规条款，法律法规来源为法律法规库。\n"
    "已知法律法规库如下：\n{law_library}\n"
    "你可用的工具：`lookup_policy_text`，用于根据法律法规名称或ID查询全文。调用该工具时，仅传入 JSON 对象 {{\"title\": <法律法规名称>}}。\n"
    "工具会返回一个 JSON 对象，其中 `text` 字段是该法规的全文。你必须在收到工具结果后，再继续分析。\n"
    "注意：法律法规库只包含法规名称，不包含条款编号。如果你没有从工具结果中定位到具体条款，`clause` 字段必须返回空字符串，禁止根据先验知识编造条款。\n"
    "请基于这些资料分析下面的问题并返回最相关的条款：\n{question}\n"
    "你的返回要求：输出 JSON 数组，每个元素包含 `title` 和 `clause` 两个字段。例如：{{\"title\":\"中华人民共和国票据法\",\"clause\":\"第三条\"}}。若未知条款编号则 `clause` 设为空字符串。\n"
    "请严格按照法律法规库的内容返回。"
)


def _normalize_str(value: Optional[str]) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _parse_float(value: Optional[str], default: float) -> float:
    if not value:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_int(value: Optional[str], default: int) -> int:
    if not value:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


@dataclass(slots=True)
class RegulationLLMConfig:
    api_base: str
    api_key: str
    model: str
    temperature: float
    max_output_tokens: int
    timeout: float
    system_prompt: str
    user_prompt_template: str


_POLICY_LIBRARY: Optional[List[str]] = None


def _get_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_policy_library() -> Sequence[str]:
    global _POLICY_LIBRARY
    if _POLICY_LIBRARY is not None:
        return _POLICY_LIBRARY
    whitelist_path = _get_project_root() / "policy_whitelist.json"
    try:
        with whitelist_path.open("r", encoding="utf-8") as stream:
            payload = json.load(stream)
    except FileNotFoundError:
        _LOGGER.warning("Policy whitelist file not found: %%s", whitelist_path)
        _POLICY_LIBRARY = []
        return _POLICY_LIBRARY
    except json.JSONDecodeError as exc:
        _LOGGER.warning("Failed to parse policy whitelist: %%s (%%s)", whitelist_path, exc)
        _POLICY_LIBRARY = []
        return _POLICY_LIBRARY

    policy_titles = payload.get("policy_titles")
    if isinstance(policy_titles, list):
        cleaned = [title for title in (_normalize_str(item) for item in policy_titles) if title]
        _POLICY_LIBRARY = cleaned
    else:
        _POLICY_LIBRARY = []
    return _POLICY_LIBRARY


def _load_llm_config() -> Optional[RegulationLLMConfig]:
    api_key = _normalize_str(os.getenv("PBC_REGULATIONS_ASK_LLM_API_KEY"))
    model = _normalize_str(os.getenv("PBC_REGULATIONS_ASK_LLM_MODEL"))
    if not api_key or not model:
        return None

    api_base = _normalize_str(os.getenv("PBC_REGULATIONS_ASK_LLM_API_BASE")) or "https://api.openai.com/v1"
    temperature = _parse_float(os.getenv("PBC_REGULATIONS_ASK_LLM_TEMPERATURE"), 0.0)
    max_tokens = _parse_int(os.getenv("PBC_REGULATIONS_ASK_LLM_MAX_OUTPUT_TOKENS"), 1024)
    timeout = _parse_float(os.getenv("PBC_REGULATIONS_ASK_LLM_TIMEOUT"), 30.0)
    system_prompt = _normalize_str(os.getenv("PBC_REGULATIONS_ASK_LLM_SYSTEM_PROMPT")) or _DEFAULT_LLM_SYSTEM_PROMPT
    user_prompt_template = (
        _normalize_str(os.getenv("PBC_REGULATIONS_ASK_LLM_USER_PROMPT")) or _DEFAULT_LLM_USER_PROMPT
    )

    return RegulationLLMConfig(
        api_base=api_base,
        api_key=api_key,
        model=model,
        temperature=temperature,
        max_output_tokens=max_tokens,
        timeout=timeout,
        system_prompt=system_prompt,
        user_prompt_template=user_prompt_template,
    )


def _build_llm_messages(
    question: str, config: RegulationLLMConfig, law_library: Sequence[str]
) -> List[Dict[str, str]]:
    law_library_text = json.dumps(list(law_library), ensure_ascii=False, indent=2)
    user_prompt = config.user_prompt_template.format(
        question=question,
        law_library=law_library_text,
    )
    return [
        {"role": "system", "content": config.system_prompt},
        {"role": "user", "content": user_prompt},
    ]


def _get_policy_api_settings() -> Tuple[str, float]:
    base = _normalize_str(os.getenv("PBC_REGULATIONS_POLICY_API_BASE")) or "http://localhost:8000"
    timeout = _parse_float(os.getenv("PBC_REGULATIONS_POLICY_API_TIMEOUT"), 10.0)
    return base, timeout


def _lookup_policy_text(title: str) -> Optional[str]:
    policy_title = _normalize_str(title)
    if not policy_title:
        return None

    base, timeout = _get_policy_api_settings()
    url = base.rstrip("/") + "/api/policies/" + quote(policy_title, safe="")
    params = {"include": "text"}

    try:
        response = requests.get(url, params=params, timeout=timeout)
    except Exception as exc:  # pragma: no cover - runtime specific network errors
        _LOGGER.warning("Policy lookup failed for %s: %s", policy_title, exc)
        return None

    if response.status_code >= 400:
        _LOGGER.debug(
            "Policy lookup returned %s for %s: %s",
            response.status_code,
            policy_title,
            response.text,
        )
        return None

    try:
        payload = response.json()
    except ValueError:
        _LOGGER.debug("Policy lookup response is not JSON for %s: %s", policy_title, response.text)
        return None

    text_content = payload.get("text")
    if isinstance(text_content, str):
        return text_content.strip()
    return None


def _handle_tool_call(tool_call: Dict[str, object]) -> str:
    function_data = tool_call.get("function") if isinstance(tool_call, dict) else None
    if not isinstance(function_data, dict):
        return json.dumps({"error": "invalid_tool_call"}, ensure_ascii=False)

    name = _normalize_str(function_data.get("name"))
    arguments_raw = function_data.get("arguments")
    if isinstance(arguments_raw, str) and arguments_raw.strip():
        try:
            arguments = json.loads(arguments_raw)
        except json.JSONDecodeError:
            arguments = {}
    else:
        arguments = {}

    if name != "lookup_policy_text":
        return json.dumps({"error": f"unsupported_tool:{name}"}, ensure_ascii=False)

    title = _normalize_str(arguments.get("title"))
    if not title:
        return json.dumps({"error": "missing_title"}, ensure_ascii=False)

    text = _lookup_policy_text(title)
    return json.dumps({"title": title, "text": text}, ensure_ascii=False)


def _parse_llm_references(content: str) -> List[str]:
    stripped = _normalize_str(content)
    if not stripped:
        return []

    candidate_text = stripped
    if "```" in stripped:
        try:
            start = stripped.index("[")
            end = stripped.rindex("]") + 1
            candidate_text = stripped[start:end]
        except ValueError:
            candidate_text = stripped

    parsed: object
    try:
        parsed = json.loads(candidate_text)
    except json.JSONDecodeError:
        _LOGGER.debug("LLM response is not valid JSON: %s", stripped)
        return []

    items: Iterable[object]
    if isinstance(parsed, list):
        items = parsed
    elif isinstance(parsed, dict):
        for key in ("items", "references", "data", "result", "results"):
            value = parsed.get(key)  # type: ignore[index]
            if isinstance(value, list):
                items = value
                break
        else:
            items = []
    else:
        items = []

    results: List[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        title = _normalize_str(item.get("title") or item.get("law") or item.get("name"))
        clause = _normalize_str(
            item.get("clause")
            or item.get("article")
            or item.get("citation")
            or item.get("reference")
        )
        if not title and not clause:
            continue
        if title and clause:
            results.append(f"{title} {clause}")
        else:
            results.append(title or clause)
    return results


def _call_llm_for_regulations(
    question: str, config: RegulationLLMConfig, law_library: Sequence[str]
) -> List[str]:
    messages = _build_llm_messages(question, config, law_library)
    headers = {
        "Authorization": f"Bearer {config.api_key}",
        "Content-Type": "application/json",
    }
    url = config.api_base.rstrip("/") + "/chat/completions"

    tools: List[Dict[str, object]] = [
        {
            "type": "function",
            "function": {
                "name": "lookup_policy_text",
                "description": "根据法规标题获取法规全文，返回 JSON 对象 {\"title\": 标题, \"text\": 全文或 null}",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "title": {
                            "type": "string",
                            "description": "法律法规在法律法规库中的名称",
                        }
                    },
                    "required": ["title"],
                },
            },
        }
    ]

    for _ in range(6):
        payload: Dict[str, object] = {
            "model": config.model,
            "messages": messages,
            "temperature": config.temperature,
            "tools": tools,
        }
        if config.max_output_tokens:
            payload["max_output_tokens"] = config.max_output_tokens
            payload["max_tokens"] = config.max_output_tokens

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=config.timeout)
        except Exception as exc:  # pragma: no cover - network issues are runtime specific
            _LOGGER.warning("LLM request failed: %s", exc)
            return []

        if response.status_code >= 400:
            _LOGGER.warning("LLM request returned %s: %s", response.status_code, response.text)
            return []

        try:
            body = response.json()
        except ValueError:
            _LOGGER.warning("LLM response is not JSON: %s", response.text)
            return []

        choices = body.get("choices")
        if not isinstance(choices, list) or not choices:
            _LOGGER.debug("LLM response missing choices: %s", body)
            return []

        message = choices[0].get("message")
        if not isinstance(message, dict):
            _LOGGER.debug("LLM response missing message: %s", body)
            return []

        assistant_message: Dict[str, object] = {
            "role": "assistant",
            "content": message.get("content") or "",
        }
        tool_calls = message.get("tool_calls")
        if isinstance(tool_calls, list) and tool_calls:
            assistant_message["tool_calls"] = tool_calls
        messages.append(assistant_message)

        if isinstance(tool_calls, list) and tool_calls:
            for tool_call in tool_calls:
                tool_result = _handle_tool_call(tool_call)
                tool_message = {
                    "role": "tool",
                    "tool_call_id": (tool_call.get("id") if isinstance(tool_call, dict) else None) or "",
                    "content": tool_result,
                }
                messages.append(tool_message)
            continue

        content = _normalize_str(assistant_message.get("content"))
        if not content:
            return []
        return _parse_llm_references(content)

    _LOGGER.warning("LLM conversation exceeded maximum iterations")
    return []


class LegalReferenceSection(TypedDict):
    """A section highlighted inside a referenced legal document."""

    id: str
    title: str
    text: str


class LegalReference(TypedDict):
    """Structure describing the origin of an answer."""

    id: str
    title: str
    citation: str
    fullText: str
    focusSectionId: str
    sections: List[LegalReferenceSection]


class LegalAnswer(TypedDict):
    """Response payload returned by the mocked legal Q&A helper."""

    text: str
    references: List[LegalReference]


def get_legal_answer(question: str) -> LegalAnswer:
    """Return a simulated legal answer based on the question content."""

    normalized_question = question.strip()

    if "退款" in normalized_question or "退货" in normalized_question:
        return {
            "text": (
                "根据《支付结算办法》第三十二条规定，收单机构应在收到退款申请后5个工作日内完成审核及处理。"
                "对于符合退款条件的，应在审核通过后2个工作日内完成退款操作。特殊情况需要延长处理时间的，应提前通知"
                "客户并说明原因，最长不得超过15个工作日。"
            ),
            "references": [
                {
                    "id": "payment-settlement-32",
                    "title": "《支付结算办法》",
                    "citation": "第三十二条 退款审核与处理时限",
                    "fullText": "\n".join(
                        [
                            "《支付结算办法》",
                            "第一章 总则",
                            "第一条 为规范支付结算行为，保护交易双方的合法权益，维护支付市场秩序，根据有关法律法规制定本办法。",
                            "第二条 从事支付结算业务的机构，应当遵循安全、高效、诚实守信的原则，建立健全风险管理制度和内部控制机制。",
                            "",
                            "第四章 退款管理",
                            "第三十一条 收单机构应当建立完备的退款处理流程，对客户提交的退款申请进行登记、审核和跟踪反馈。",
                            "第三十二条 收单机构应当自收到退款申请之日起五个工作日内完成审核处理。符合退款条件的，应当自审核通过之日起两个工作日内将款项退回原支付渠道；确需延长处理时间的，应当事先告知申请人并说明原因，但最长不得超过十五个工作日。",
                            "第三十三条 收单机构应当妥善留存退款处理记录和相关沟通材料，记录保存期限不少于五年；对于涉及纠纷的，应当配合监管机构调查并提供所需证据材料。",
                            "",
                            "第七章 附则",
                            "第五十二条 本办法由中国人民银行负责解释，自发布之日起施行。",
                        ]
                    ),
                    "focusSectionId": "payment-settlement-32-article",
                    "sections": [
                        {
                            "id": "payment-settlement-32-article",
                            "title": "第三十二条",
                            "text": (
                                "收单机构应当自收到退款申请之日起五个工作日内完成审核处理。符合退款条件的，应当自审核通过之日起两个工作日内将款项退回原支付渠道；确需延长处理时间的，应当事先告知申请人并说明原因，但最长不得超过十五个工作日。"
                            ),
                        },
                        {
                            "id": "payment-settlement-32-article-2",
                            "title": "第三十二条第二款",
                            "text": (
                                "因系统故障、网络异常等特殊情形导致无法在前款期限内完成退款的，收单机构应当主动记录处理过程并在恢复后立即完成退款，同时向客户提供必要的沟通与反馈渠道。"
                            ),
                        },
                    ],
                }
            ],
        }

    if "手续费" in normalized_question or "收费" in normalized_question:
        return {
            "text": (
                "根据《关于规范支付服务市场收费的通知》第五条规定，支付机构可根据服务成本自主制定手续费标准，但应提前公示并与客户协商一致。"
                "手续费标准变更时，应提前30天通知客户，客户不同意的，支付机构应允许客户在合理期限内解除服务协议，不得收取违约金。"
            ),
            "references": [
                {
                    "id": "pricing-notice-5",
                    "title": "《关于规范支付服务市场收费的通知》",
                    "citation": "第五条 收费公示与协议管理",
                    "fullText": "\n".join(
                        [
                            "《关于规范支付服务市场收费的通知》",
                            "第一部分 总体要求",
                            "第一条 各支付机构应当严格遵守国家关于价格管理的规定，按照公平、公开的原则合理制定收费项目与标准，增强成本管控与服务透明度。",
                            "第二条 各相关部门应当加强对支付服务收费行为的监督，防止出现强制收费、变相收费等侵害消费者权益的行为。",
                            "",
                            "第二部分 具体规范",
                            "第五条 支付机构应当按照服务成本和合理利润原则制定收费项目与标准，并以显著方式对外公示。收费标准调整的，应当提前三十日告知客户，并保障客户在合理期限内解除服务协议的权利，不得附加不合理条件或收取违约金。",
                            "第六条 支付机构应当建立收费公示档案，详细记录收费项目、收费标准、优惠措施以及变更时间，档案保存期限不少于五年，以备监管部门检查。",
                            "",
                            "第三部分 监督检查",
                            "第十条 各级监管机构应当定期组织对支付服务收费情况的检查，对违反本通知的机构依法依规予以处理，并及时向社会公布典型案例。",
                        ]
                    ),
                    "focusSectionId": "pricing-notice-5-clause",
                    "sections": [
                        {
                            "id": "pricing-notice-5-clause",
                            "title": "第五条",
                            "text": (
                                "支付机构应当按照服务成本和合理利润原则制定收费项目与标准，并以显著方式对外公示。收费标准调整的，应当提前三十日告知客户，并保障客户在合理期限内解除服务协议的权利，不得附加不合理条件或收取违约金。"
                            ),
                        }
                    ],
                }
            ],
        }

    if "反洗钱" in normalized_question:
        return {
            "text": (
                "《反洗钱法》第十六条规定，金融机构应当按照规定建立客户身份识别制度，对要求建立业务关系或者办理规定金额以上的一次性金融业务的客户身份进行识别，登记客户身份基本信息，并留存有效身份证件或者其他身份证明文件的复印件或者影印件。"
                "支付机构作为反洗钱义务主体，应当执行客户身份识别、大额交易和可疑交易报告等反洗钱义务。"
            ),
            "references": [
                {
                    "id": "aml-law-16",
                    "title": "《反洗钱法》",
                    "citation": "第十六条 客户身份识别",
                    "fullText": "\n".join(
                        [
                            "《反洗钱法》",
                            "第一章 总则",
                            "第一条 为了防止洗钱活动，维护金融秩序，制定本法。",
                            "第二条 在中华人民共和国境内设立的金融机构和特定非金融机构应当依照本法履行反洗钱义务。",
                            "",
                            "第二章 反洗钱义务",
                            "第十六条 金融机构应当按照规定建立健全客户身份识别制度，对要求建立业务关系或者办理规定金额以上的一次性金融业务的客户身份进行识别，登记客户身份基本信息，并留存有效身份证件或者其他身份证明文件的复印件或者影印件。金融机构应当持续关注客户身份识别信息的有效性，在业务关系存续期间，根据风险状况适时更新客户身份资料，并将识别信息及交易记录至少保存五年。",
                            "第十七条 金融机构应当建立和完善客户身份资料及交易记录保存制度，确保识别信息真实、完整、可追溯。",
                            "第十八条 金融机构发现大额交易和可疑交易的，应当依照规定向反洗钱行政主管部门报告。",
                            "",
                            "第四章 监督检查",
                            "第二十七条 反洗钱行政主管部门依法对金融机构履行反洗钱义务的情况进行监督检查；违反本法规定的，依法依规予以处罚。",
                        ]
                    ),
                    "focusSectionId": "aml-law-16-article",
                    "sections": [
                        {
                            "id": "aml-law-16-article",
                            "title": "第十六条",
                            "text": (
                                "金融机构应当按照规定建立健全客户身份识别制度，对要求建立业务关系或者办理规定金额以上的一次性金融业务的客户身份进行识别，登记客户身份基本信息，并留存有效身份证件或者其他身份证明文件的复印件或者影印件。"
                            ),
                        },
                        {
                            "id": "aml-law-16-obligation",
                            "title": "第十六条第二款",
                            "text": (
                                "金融机构应当持续关注客户身份识别信息的有效性，在业务关系存续期间，根据风险状况适时更新客户身份资料，并将识别信息及交易记录至少保存五年。"
                            ),
                        },
                    ],
                }
            ],
        }

    if "信息披露" in normalized_question:
        return {
            "text": (
                "《非金融机构支付服务管理办法》第二十八条规定，支付机构应当公开披露其支付服务的收费项目、收费标准、服务时限等信息。"
                "支付机构应当在营业场所显著位置披露上述信息，也可以通过网站等其他方式披露。支付机构调整收费项目、收费标准的，应当提前30天在营业场所显著位置或者网站等其他方式通知客户。"
            ),
            "references": [
                {
                    "id": "non-financial-28",
                    "title": "《非金融机构支付服务管理办法》",
                    "citation": "第二十八条 信息披露要求",
                    "fullText": "\n".join(
                        [
                            "《非金融机构支付服务管理办法》",
                            "第一章 总则",
                            "第一条 为加强对非金融机构支付服务的监督管理，促进支付服务市场健康发展，制定本办法。",
                            "第二条 非金融机构提供支付服务，应当遵守国家法律法规，接受人民银行监督管理，保护客户合法权益。",
                            "",
                            "第四章 信息披露与服务管理",
                            "第二十七条 支付机构应当建立健全服务信息披露制度，对业务流程、办理时限、收费项目等重要信息进行梳理和定期更新。",
                            "第二十八条 支付机构应当按照公平、公开原则披露支付服务的收费项目、收费标准、服务时限等与客户权益密切相关的信息。在营业场所、官方网站等渠道应当有显著展示，确保客户易于获取。支付机构调整收费项目或者收费标准的，应当提前三十日通过营业场所、官方网站等渠道向客户公告，并提供咨询和异议反馈渠道。",
                            "第二十九条 支付构应当建立客户意见收集与处理机制，及时回应客户对信息披露内容的咨询与投诉。",
                            "",
                            "第六章 附则",
                            "第四十五条 本办法自发布之日起施行，由中国人民银行负责解释。",
                        ]
                    ),
                    "focusSectionId": "non-financial-28-article",
                    "sections": [
                        {
                            "id": "non-financial-28-article",
                            "title": "第二十八条",
                            "text": (
                                "支付机构应当按照公平、公开原则披露支付服务的收费项目、收费标准、服务时限等与客户权益密切相关的信息。在营业场所、官方网站等渠道应当有显著展示，确保客户易于获取。"
                            ),
                        },
                        {
                            "id": "non-financial-28-change",
                            "title": "第二十八条第二款",
                            "text": (
                                "支付机构调整收费项目或者收费标准的，应当提前三十日通过营业场所、官方网站等渠道向客户公告，并提供咨询和异议反馈渠道。"
                            ),
                        },
                    ],
                }
            ],
        }

    return {
        "text": (
            "根据相关法律法规规定，支付结算业务应当遵循安全、效率、诚信和公平竞争的原则。支付机构应当遵守反洗钱、反恐怖融资相关规定，确保支付业务的合规性。如您有具体问题，请提供更多细节，以便我为您提供更准确的法律依据。"
        ),
        "references": [],
    }


@dataclass(slots=True)
class SingleTurnAskRequest:
    """Payload for a single-turn institutional Q&A request."""

    question: str
    policy_hint: Optional[str] = None


@dataclass(slots=True)
class MultiTurnAskRequest:
    """Payload for a multi-turn Q&A request that belongs to a session."""

    message: str
    session_id: Optional[str] = None
    policy_hint: Optional[str] = None


@dataclass(slots=True)
class AskResponse:
    """Standard response structure for the mocked Q&A endpoints."""

    answer: str
    created_at: datetime = field(default_factory=datetime.utcnow)
    references: List[str] = field(default_factory=list)
    session_id: Optional[str] = None
    follow_up_questions: List[str] = field(default_factory=list)


def find_related_regulations(question: str) -> List[str]:
    """Return a list of regulation references that best match the given question."""

    normalized_question = question.strip()
    if not normalized_question:
        return []

    llm_config = _load_llm_config()
    law_library = _load_policy_library()
    if llm_config is not None:
        llm_references = _call_llm_for_regulations(
            normalized_question, llm_config, law_library
        )
        print("5555")
        if llm_references:
            return llm_references

    print("6666")
    legal_answer = get_legal_answer(normalized_question)
    results: List[str] = []
    for ref in legal_answer.get("references", []):
        title = ref.get("title")
        citation = ref.get("citation")
        if title and citation:
            results.append(f"{title} {citation}")
        elif title:
            results.append(title)
        elif citation:
            results.append(citation)
    return results


def ask_institution_once(request: SingleTurnAskRequest) -> AskResponse:
    """Return a mocked answer for a standalone institutional policy question."""

    question = request.question.strip()
    if not question:
        raise ValueError("question must not be empty")

    answer = (
        "这是一个示例回答，用于说明制度问答接口的返回结果。"
        "未来这里会接入真实的大模型或检索服务。"
    )
    references = find_related_regulations(question)
    if not references:
        references = [
            "mock://policy/2024-001",
            "mock://policy/2023-099",
        ]
    follow_ups = [
        "该制度的适用范围是什么？",
        "是否存在最新修订版本？",
    ]
    return AskResponse(
        answer=answer,
        references=references,
        follow_up_questions=follow_ups,
    )


def ask_with_session(request: MultiTurnAskRequest) -> AskResponse:
    """Return a mocked answer that participates in a conversational session."""

    message = request.message.strip()
    if not message:
        raise ValueError("message must not be empty")

    session_id = request.session_id or str(uuid4())
    history = _SESSION_HISTORY.setdefault(session_id, [])
    history.append({"role": "user", "content": message})

    answer = (
        "这是一个会话式问答的示例回复，我们会根据历史上下文来调整回答。"
        "当前实现仅返回模拟内容，并不会调用真实模型。"
    )
    history.append({"role": "assistant", "content": answer})

    follow_ups = [
        "需要我总结一下目前的讨论吗？",
        "是否要查询相关的合规条款？",
    ]

    references = ["mock://conversation/context"]

    return AskResponse(
        answer=answer,
        references=references,
        session_id=session_id,
        follow_up_questions=follow_ups,
    )


__all__ = [
    "AskResponse",
    "LegalAnswer",
    "LegalReference",
    "LegalReferenceSection",
    "MultiTurnAskRequest",
    "SingleTurnAskRequest",
    "ask_institution_once",
    "ask_with_session",
    "get_legal_answer",
]
