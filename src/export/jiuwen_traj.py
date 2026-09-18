#!/usr/bin/env python3
"""Keep each group's last Jiuwen snapshot and restore historical reasoning.

Each proxy file contains the request in ``messages`` and that call's answer in
``response.content``. Harness carries the answer into the next request but
drops blocks such as ``{"type": "thinking", ...}``. This program indexes
historical responses by their exact non-reasoning content and inserts missing
reasoning blocks into matching assistant messages of the final snapshot.

Historical files are parsed once and released immediately. Existing values
are never overwritten, and conflicting sources are rejected as ambiguous.
"""

from __future__ import annotations

import argparse
import copy
import json
import shutil
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable

JSON = dict[str, Any]
REASONING_BLOCK_TYPES = {"thinking", "reasoning", "redacted_thinking"}


class TrajectoryError(RuntimeError):
    """Raised when a trajectory cannot be processed safely."""


@dataclass
class SourceIndex:
    sources: dict[str, list[Any]] = field(default_factory=dict)
    ambiguous: set[str] = field(default_factory=set)
    responses_with_reasoning: int = 0
    indexed_responses: int = 0
    unanchored_responses: int = 0


@dataclass
class GroupStats:
    group: str
    input_files: int
    selected_file: str
    responses_with_reasoning: int = 0
    indexed_responses: int = 0
    unanchored_responses: int = 0
    ambiguous_source_keys: int = 0
    assistant_messages: int = 0
    matched_messages: int = 0
    changed_messages: int = 0
    restored_blocks: int = 0
    restored_characters: int = 0


def load_json(path: Path) -> JSON:
    try:
        with path.open("r", encoding="utf-8-sig") as stream:
            value = json.load(stream)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise TrajectoryError(f"cannot read JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise TrajectoryError(f"top-level JSON value is not an object: {path}")
    return value


def dump_json(path: Path, value: JSON) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def is_reasoning_block(block: Any) -> bool:
    return isinstance(block, dict) and block.get("type") in REASONING_BLOCK_TYPES


def split_reasoning_gaps(content: list[Any]) -> tuple[list[Any], list[list[Any]]]:
    """Split content into stable blocks and reasoning gaps around them."""
    structural: list[Any] = []
    gaps: list[list[Any]] = [[]]
    for block in content:
        if is_reasoning_block(block):
            gaps[-1].append(block)
        else:
            structural.append(block)
            gaps.append([])
    return structural, gaps


def content_identity(content: Any) -> str | None:
    if not isinstance(content, list):
        return None
    structural, _ = split_reasoning_gaps(content)
    # A reasoning-only response has no stable representation in later
    # messages, so matching it would require guessing.
    return canonical(structural) if structural else None


def response_content(document: JSON) -> list[Any] | None:
    response = document.get("response")
    if not isinstance(response, dict):
        return None
    content = response.get("content")
    return content if isinstance(content, list) else None


def build_response_index(paths: Iterable[Path]) -> SourceIndex:
    """Read each historical file once and index its response reasoning."""
    result = SourceIndex()
    fingerprints: dict[str, str] = {}
    for path in paths:
        content = response_content(load_json(path))
        if not content or not any(is_reasoning_block(block) for block in content):
            continue
        result.responses_with_reasoning += 1
        identity = content_identity(content)
        if identity is None:
            result.unanchored_responses += 1
            continue
        fingerprint = canonical(content)
        previous = fingerprints.get(identity)
        if previous is None:
            fingerprints[identity] = fingerprint
            result.sources[identity] = copy.deepcopy(content)
            result.indexed_responses += 1
        elif previous != fingerprint:
            # Do not choose arbitrarily between different reasoning chains
            # that happen to produce identical visible/tool-call content.
            result.ambiguous.add(identity)
    for identity in result.ambiguous:
        result.sources.pop(identity, None)
    return result


def iter_assistant_messages(document: JSON) -> Iterable[JSON]:
    messages = document.get("messages")
    if not isinstance(messages, list):
        return
    for message in messages:
        if isinstance(message, dict) and message.get("role") == "assistant":
            yield message


def reasoning_characters(blocks: Iterable[Any]) -> int:
    total = 0
    for block in blocks:
        if not isinstance(block, dict):
            continue
        for key in ("thinking", "reasoning", "text"):
            value = block.get(key)
            if isinstance(value, str):
                total += len(value)
                break
    return total


def restore_message(target: JSON, source_content: list[Any]) -> tuple[int, int]:
    """Fill empty reasoning gaps while preserving every existing block."""
    target_content = target.get("content")
    if not isinstance(target_content, list):
        return 0, 0
    target_structural, target_gaps = split_reasoning_gaps(target_content)
    source_structural, source_gaps = split_reasoning_gaps(source_content)
    if target_structural != source_structural:
        raise AssertionError("content identity collision: structural blocks differ")

    rebuilt: list[Any] = []
    restored_blocks: list[Any] = []
    for index, structural_block in enumerate(target_structural):
        target_gap = target_gaps[index]
        source_gap = source_gaps[index]
        chosen = target_gap
        if not target_gap and source_gap:
            chosen = copy.deepcopy(source_gap)
            restored_blocks.extend(chosen)
        rebuilt.extend(chosen)
        rebuilt.append(structural_block)
    target_gap = target_gaps[-1]
    source_gap = source_gaps[-1]
    chosen = target_gap
    if not target_gap and source_gap:
        chosen = copy.deepcopy(source_gap)
        restored_blocks.extend(chosen)
    rebuilt.extend(chosen)
    if restored_blocks:
        target["content"] = rebuilt
    return len(restored_blocks), reasoning_characters(restored_blocks)


def build_source_index(documents: list[JSON]) -> SourceIndex:
    """Read each historical document and index its response reasoning.

    In-memory variant of :func:`build_response_index`: callers that already
    hold parsed documents (e.g. merged triplets in reformat_jiuwen) can skip
    the write-then-read round-trip to a scratch directory.
    """
    result = SourceIndex()
    fingerprints: dict[str, str] = {}
    for document in documents:
        content = response_content(document)
        if not content or not any(is_reasoning_block(block) for block in content):
            continue
        result.responses_with_reasoning += 1
        identity = content_identity(content)
        if identity is None:
            result.unanchored_responses += 1
            continue
        fingerprint = canonical(content)
        previous = fingerprints.get(identity)
        if previous is None:
            fingerprints[identity] = fingerprint
            result.sources[identity] = copy.deepcopy(content)
            result.indexed_responses += 1
        elif previous != fingerprint:
            result.ambiguous.add(identity)
    for identity in result.ambiguous:
        result.sources.pop(identity, None)
    return result


def process_documents(
    documents: list[JSON],
    output_path: Path,
    group_name: str = "",
    selected_name: str = "",
) -> GroupStats:
    """Restore missing reasoning into the last document, write it to *output_path*.

    This is the in-memory core of :func:`process_group`: callers that already
    hold parsed documents (e.g. merged triplets in reformat_jiuwen) pass them
    here directly, avoiding a write-then-read round-trip through a scratch
    directory.  *documents* must be ordered the same way ``process_group``
    orders files — by name — so that ``documents[-1]`` is the final snapshot.
    """
    if not documents:
        raise TrajectoryError("documents list is empty")
    sources = build_source_index(documents[:-1])
    final_document = documents[-1]
    if not isinstance(final_document.get("messages"), list):
        raise TrajectoryError("messages is not an array in the final document")
    stats = GroupStats(
        group=group_name,
        input_files=len(documents),
        selected_file=selected_name,
        responses_with_reasoning=sources.responses_with_reasoning,
        indexed_responses=sources.indexed_responses,
        unanchored_responses=sources.unanchored_responses,
        ambiguous_source_keys=len(sources.ambiguous),
    )
    for message in iter_assistant_messages(final_document):
        stats.assistant_messages += 1
        identity = content_identity(message.get("content"))
        source = sources.sources.get(identity) if identity is not None else None
        if source is None:
            continue
        stats.matched_messages += 1
        blocks, characters = restore_message(message, source)
        if blocks:
            stats.changed_messages += 1
            stats.restored_blocks += blocks
            stats.restored_characters += characters

    output_path.parent.mkdir(parents=True, exist_ok=True)
    dump_json(output_path, final_document)
    return stats


def process_group(group_dir: Path, output_root: Path) -> GroupStats:
    files = sorted(group_dir.glob("*.json"), key=lambda path: path.name)
    if not files:
        raise TrajectoryError(f"group contains no JSON files: {group_dir}")

    documents = [load_json(path) for path in files]
    output_path = output_root / group_dir.name / files[-1].name
    return process_documents(
        documents,
        output_path=output_path,
        group_name=group_dir.name,
        selected_name=files[-1].name,
    )


def strip_message_reasoning(document: JSON) -> JSON:
    """Remove only assistant reasoning blocks for invariant verification."""
    result = copy.deepcopy(document)
    for message in iter_assistant_messages(result):
        content = message.get("content")
        if isinstance(content, list):
            message["content"] = [
                block for block in content if not is_reasoning_block(block)
            ]
    return result


def collect_difference_paths(before: Any, after: Any, path: str = "$") -> list[str]:
    if type(before) is not type(after):
        return [path]
    if isinstance(before, dict):
        paths: list[str] = []
        for key in before.keys() | after.keys():
            child = f"{path}.{key}"
            if key not in before or key not in after:
                paths.append(child)
            else:
                paths.extend(collect_difference_paths(before[key], after[key], child))
        return paths
    if isinstance(before, list):
        if len(before) != len(after):
            return [path]
        paths: list[str] = []
        for index, (left, right) in enumerate(zip(before, after)):
            paths.extend(collect_difference_paths(left, right, f"{path}[{index}]"))
        return paths
    return [] if before == after else [path]


def verify_output(input_root: Path, output_root: Path) -> JSON:
    reports: list[JSON] = []
    all_passed = True
    for group_dir in sorted(path for path in input_root.iterdir() if path.is_dir()):
        source_files = sorted(group_dir.glob("*.json"), key=lambda path: path.name)
        output_dir = output_root / group_dir.name
        output_files = sorted(output_dir.glob("*.json")) if output_dir.is_dir() else []
        selected = source_files[-1].name if source_files else None
        correct_file = len(output_files) == 1 and output_files[0].name == selected
        difference_paths: list[str] = []
        if correct_file:
            source = strip_message_reasoning(load_json(source_files[-1]))
            output = strip_message_reasoning(load_json(output_files[0]))
            difference_paths = collect_difference_paths(source, output)
        passed = bool(source_files) and correct_file and not difference_paths
        all_passed = all_passed and passed
        reports.append(
            {
                "group": group_dir.name,
                "source_file_count": len(source_files),
                "output_file_count": len(output_files),
                "selected_file": selected,
                "only_reasoning_changed": not difference_paths,
                "non_reasoning_difference_paths": difference_paths,
                "passed": passed,
            }
        )
    return {"passed": all_passed, "groups": reports}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="directory containing groups")
    parser.add_argument("output", type=Path, help="new output directory")
    parser.add_argument("--report", type=Path, help="optional JSON report")
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    input_root = args.input.resolve()
    output_root = args.output.resolve()
    if not input_root.is_dir():
        print(f"error: input directory does not exist: {input_root}", file=sys.stderr)
        return 2
    if input_root == output_root or input_root in output_root.parents:
        print("error: output must be outside and different from input", file=sys.stderr)
        return 2
    if output_root.exists():
        if not args.overwrite:
            print(f"error: output exists (use --overwrite): {output_root}", file=sys.stderr)
            return 2
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True)
    groups = sorted(path for path in input_root.iterdir() if path.is_dir())
    if not groups:
        print(f"error: no trajectory group directories in {input_root}", file=sys.stderr)
        return 2

    stats = [process_group(group, output_root) for group in groups]
    verification = verify_output(input_root, output_root)
    summary = {
        "groups": len(stats),
        "input_files": sum(item.input_files for item in stats),
        "output_files": len(stats),
        "historical_responses_with_reasoning": sum(
            item.responses_with_reasoning for item in stats
        ),
        "unanchored_responses": sum(item.unanchored_responses for item in stats),
        "ambiguous_source_keys": sum(item.ambiguous_source_keys for item in stats),
        "changed_messages": sum(item.changed_messages for item in stats),
        "restored_blocks": sum(item.restored_blocks for item in stats),
        "restored_characters": sum(item.restored_characters for item in stats),
        "verification_passed": verification["passed"],
    }
    report = {
        "input": str(input_root),
        "output": str(output_root),
        "summary": summary,
        "processing": [asdict(item) for item in stats],
        "verification": verification,
    }
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if verification["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
