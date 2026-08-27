#!/usr/bin/env python3
"""Emit aggregate, read-only capacity benchmark evidence as runner JSONL."""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import pathlib
import re
import shlex
import stat
import subprocess
import time
from typing import Iterable


ADMISSION_MESSAGE = "sandbox start intent admitted"
CAPACITY_RECONCILED_MESSAGE = "capacity reconciled"
CONTROLLER_AUDIT_MESSAGES = {
    "controller_started",
    "scale_write_started",
    "scale_write_finished",
    "audit_checkpoint",
}
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
ANSI_ESCAPE_PATTERN = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
ASG_LAUNCH_DESCRIPTION_PATTERN = re.compile(
    r"^Launching a new EC2 instance(?:: i-[0-9a-f]+)?\.?$"
)


class CommandRunner:
    def run(self, argv: list[str]) -> str:
        result = subprocess.run(
            argv,
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        )
        return result.stdout


class ControllerSnapshotNotReadyError(ValueError):
    pass


def aws_context_args(
    credential_source: str,
    *,
    region: str,
    profile: str | None,
) -> list[str]:
    if credential_source == "local-profile":
        if not profile:
            raise ValueError("AWS profile is required for local-profile credentials")
        return ["--profile", profile, "--region", region]
    if credential_source == "instance-role":
        if profile:
            raise ValueError("AWS profile must be omitted for instance-role credentials")
        return ["--region", region]
    raise ValueError(f"unsupported credential source {credential_source!r}")


def wait_until_epoch_ms(t0_epoch_ms: int, *, clock_epoch_ms, sleep) -> None:
    delay_ms = t0_epoch_ms - clock_epoch_ms()
    if not isinstance(delay_ms, (int, float)) or delay_ms <= 0:
        raise ValueError("t0 must still be in the future")
    while delay_ms > 0:
        sleep(delay_ms / 1000)
        delay_ms = t0_epoch_ms - clock_epoch_ms()


def parse_t0(value: str) -> int:
    if not re.fullmatch(r"\d{13}", value):
        raise ValueError("t0 must be a 13-digit epoch millisecond timestamp")
    return int(value)


def parse_journal_jsonl(text: str, t0_epoch_ms: int) -> Iterable[tuple[float, str]]:
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
            timestamp_ms = int(row["__REALTIME_TIMESTAMP"]) / 1000
            message = row["MESSAGE"]
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
            raise ValueError(f"invalid journal JSON at line {line_number}") from error
        # journald serializes messages containing ANSI control bytes as an
        # array of byte values. Decode that documented representation strictly;
        # unrelated repeated/non-byte fields still cannot match our markers.
        if isinstance(message, list) and len(message) <= 1024 * 1024:
            if all(isinstance(value, int) and 0 <= value <= 255 for value in message):
                try:
                    message = bytes(message).decode("utf-8")
                except UnicodeDecodeError:
                    continue
        if not isinstance(message, str):
            continue
        if timestamp_ms >= t0_epoch_ms:
            yield timestamp_ms, message


def parse_log_fields(message: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    try:
        tokens = shlex.split(message)
    except ValueError as error:
        raise ValueError("invalid structured journal message") from error
    for token in tokens:
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        fields[key] = value
    return fields


def parse_structured_fields(message: str) -> dict[str, str]:
    cleaned = ANSI_ESCAPE_PATTERN.sub("", message).strip()
    fields = parse_log_fields(cleaned)

    json_candidates = [cleaned]
    object_start = cleaned.find("{")
    if object_start > 0:
        json_candidates.append(cleaned[object_start:])
    for candidate in json_candidates:
        try:
            row = json.loads(candidate)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(row, dict):
            continue
        for key, value in row.items():
            if isinstance(value, bool):
                fields[str(key)] = str(value).lower()
            elif value is not None and not isinstance(value, (dict, list)):
                fields[str(key)] = str(value)

    if fields.get("msg") is None:
        known_messages = [
            ADMISSION_MESSAGE,
            CAPACITY_RECONCILED_MESSAGE,
            *CONTROLLER_AUDIT_MESSAGES,
        ]
        for known_message in known_messages:
            if re.search(
                rf"(?:^|\s){re.escape(known_message)}(?=\s+(?:\{{|[A-Za-z_][A-Za-z0-9_]*=)|\s*$)",
                cleaned,
            ):
                fields["msg"] = known_message
                break
    return fields


def parse_api_admissions(
    text: str,
    *,
    t0_epoch_ms: int,
    expected_capacity_mode: str,
    expected_run_hash: str,
    target: int,
) -> Iterable[float]:
    admitted: list[float] = []
    for timestamp_ms, message in parse_journal_jsonl(text, t0_epoch_ms):
        fields = parse_structured_fields(message)
        if fields.get("msg") != ADMISSION_MESSAGE:
            continue
        observed_mode = fields.get("capacity_mode")
        if observed_mode != expected_capacity_mode:
            raise ValueError(
                f"admission log has unexpected capacity_mode {observed_mode!r}; "
                f"expected {expected_capacity_mode!r}"
            )
        observed_run_hash = fields.get("benchmark_run_hash")
        if observed_run_hash != expected_run_hash:
            raise ValueError(
                "admission log is missing the expected benchmark run hash; "
                "foreign or uncorrelated traffic was observed"
            )
        admitted.append(timestamp_ms)
        if len(admitted) > target:
            raise ValueError(
                "admission count exceeds the benchmark target; non-runner traffic was observed"
            )
    yield from admitted


def integer_field(fields: dict[str, str], name: str) -> int:
    try:
        value = int(fields[name])
    except (KeyError, ValueError) as error:
        raise ValueError(f"controller journal field {name!r} is not an integer") from error
    if value < 0:
        raise ValueError(f"controller journal field {name!r} is negative")
    return value


def parse_controller_audits(
    text: str,
    *,
    t0_epoch_ms: int,
) -> Iterable[tuple[float, dict[str, object], str]]:
    for timestamp_ms, message in parse_journal_jsonl(text, t0_epoch_ms):
        fields = parse_structured_fields(message)
        event = fields.get("msg")
        if event not in CONTROLLER_AUDIT_MESSAGES:
            continue
        audit: dict[str, object] = {
            "event": event,
            "controllerInstanceID": fields.get("controller_instance_id"),
            "mode": fields.get("mode"),
        }
        if not audit["controllerInstanceID"] or not audit["mode"]:
            raise ValueError("controller audit is missing instance ID or mode")
        if event != "controller_started":
            if event == "audit_checkpoint":
                checkpoint_generated_epoch_ms = integer_field(
                    fields, "checkpoint_generated_epoch_ms"
                )
                checkpoint_generated_elapsed_ms = (
                    checkpoint_generated_epoch_ms - t0_epoch_ms
                )
                if checkpoint_generated_elapsed_ms < 0:
                    raise ValueError("controller audit checkpoint predates T0")
                audit.update(
                    {
                        "scaleWriteSequence": integer_field(
                            fields, "scale_write_sequence"
                        ),
                        "auditDroppedTotal": integer_field(
                            fields, "audit_dropped_total"
                        ),
                        "checkpointGeneratedElapsedMs": checkpoint_generated_elapsed_ms,
                    }
                )
                yield timestamp_ms, audit, message
                continue
            audit.update(
                {
                    "scaleWriteSequence": integer_field(
                        fields, "scale_write_sequence"
                    ),
                    "workloadCount": integer_field(fields, "workload_count"),
                    "currentDesired": integer_field(fields, "current_desired"),
                    "target": integer_field(fields, "target"),
                    "batchTrigger": fields.get("batch_trigger"),
                    "batchAgeMs": integer_field(fields, "batch_age_ms"),
                    "batchIdleAgeMs": integer_field(fields, "batch_idle_age_ms"),
                }
            )
            if audit["batchTrigger"] not in {"idle", "max"}:
                raise ValueError("controller audit has an invalid batch trigger")
        if event == "scale_write_finished":
            audit.update(
                {
                    "outcome": fields.get("outcome"),
                    "durationMs": integer_field(fields, "duration_ms"),
                    "awsRequestId": fields.get("aws_request_id", ""),
                    "error": fields.get("error", ""),
                }
            )
            if audit["outcome"] not in {"success", "error"}:
                raise ValueError("controller audit has an invalid outcome")
        yield timestamp_ms, audit, message


def parse_latest_controller_workload(
    text: str,
    *,
    since_epoch_ms: int,
    before_epoch_ms: int,
) -> int:
    latest: tuple[float, int] | None = None
    for timestamp_ms, message in parse_journal_jsonl(text, since_epoch_ms):
        if timestamp_ms >= before_epoch_ms:
            continue
        fields = parse_structured_fields(message)
        if fields.get("msg") != CAPACITY_RECONCILED_MESSAGE:
            continue
        if fields.get("mode") != "start-intent-v1" or fields.get("outcome") != "success":
            continue
        workload = integer_field(fields, "workload_count")
        if latest is None or timestamp_ms > latest[0]:
            latest = (timestamp_ms, workload)
    if latest is None:
        raise ControllerSnapshotNotReadyError(
            "no successful start-intent controller snapshot after T0"
        )
    return latest[1]


def parse_asg_counts(text: str) -> tuple[int, int]:
    try:
        row = json.loads(text)
        desired = row["DesiredCapacity"]
        in_service = row["InService"]
    except (json.JSONDecodeError, KeyError, TypeError) as error:
        raise ValueError("invalid aggregate ASG response") from error
    if not isinstance(desired, int) or desired < 0:
        raise ValueError("ASG desired capacity is not a non-negative integer")
    if not isinstance(in_service, int) or in_service < 0:
        raise ValueError("ASG InService count is not a non-negative integer")
    return desired, in_service


def parse_scaling_activities_page(
    text: str,
) -> tuple[list[dict[str, object]], str | None]:
    try:
        row = json.loads(text)
        raw_activities = row["Activities"]
        next_token = row.get("NextToken")
    except (json.JSONDecodeError, KeyError, TypeError) as error:
        raise ValueError("invalid ASG scaling activities response") from error
    if not isinstance(raw_activities, list):
        raise ValueError("ASG scaling activities are not a list")
    if next_token is not None and not isinstance(next_token, str):
        raise ValueError("ASG scaling activities next token is invalid")

    activities: list[dict[str, object]] = []
    for raw in raw_activities:
        if not isinstance(raw, dict):
            raise ValueError("ASG scaling activity is not an object")
        activity_id = raw.get("ActivityId")
        start_time = raw.get("StartTime")
        status_code = raw.get("StatusCode")
        if not all(isinstance(value, str) and value for value in [activity_id, start_time, status_code]):
            raise ValueError("ASG scaling activity is missing required fields")
        activity: dict[str, object] = {
            "activityId": activity_id,
            "startTime": start_time,
            "statusCode": status_code,
        }
        for source, target in [
            ("EndTime", "endTime"),
            ("Cause", "cause"),
            ("Description", "description"),
        ]:
            value = raw.get(source)
            if value is not None:
                if not isinstance(value, str):
                    raise ValueError(f"ASG scaling activity {source} is not text")
                activity[target] = value
        description = activity.get("description", "")
        activity["action"] = (
            "launch"
            if isinstance(description, str)
            and ASG_LAUNCH_DESCRIPTION_PATTERN.fullmatch(description)
            else "other"
        )
        activities.append(activity)

    return activities, next_token


def parse_nomad_ready(text: str) -> int:
    try:
        nodes = json.loads(text)
    except json.JSONDecodeError as error:
        raise ValueError("invalid Nomad nodes response") from error
    if not isinstance(nodes, list):
        raise ValueError("Nomad nodes response is not a list")
    return sum(
        1
        for node in nodes
        if isinstance(node, dict)
        and node.get("Status") == "ready"
        and node.get("Drain") is not True
    )


class ObserverCollector:
    def __init__(
        self,
        *,
        run_id: str,
        t0_epoch_ms: int,
        expected_capacity_mode: str,
        target: int,
        credential_source: str,
        aws_region: str,
        aws_profile: str | None,
        asg_name: str,
        api_unit: str,
        controller_unit: str,
        nomad_nodes_url: str,
        commands: CommandRunner,
        clock_epoch_ms,
    ) -> None:
        self.run_id = run_id
        self.t0_epoch_ms = t0_epoch_ms
        self.expected_capacity_mode = expected_capacity_mode
        self.target = target
        self.run_hash = hashlib.sha256(run_id.encode()).hexdigest()
        self.aws_context = aws_context_args(
            credential_source,
            region=aws_region,
            profile=aws_profile,
        )
        self.asg_name = asg_name
        self.api_unit = api_unit
        self.controller_unit = controller_unit
        self.nomad_nodes_url = nomad_nodes_url
        self.commands = commands
        self.clock_epoch_ms = clock_epoch_ms
        self.emitted_controller_rows: set[tuple[float, str]] = set()

    def _scaling_activities(self, sampled_at_ms: float) -> list[dict[str, object]]:
        start_time = datetime.datetime.fromtimestamp(
            self.t0_epoch_ms / 1000,
            datetime.UTC,
        ).isoformat().replace("+00:00", "Z")
        end_time = datetime.datetime.fromtimestamp(
            sampled_at_ms / 1000,
            datetime.UTC,
        ).isoformat().replace("+00:00", "Z")
        activities: list[dict[str, object]] = []
        next_token: str | None = None
        seen_tokens: set[str] = set()
        while True:
            argv = [
                "aws",
                "autoscaling",
                "describe-scaling-activities",
                "--auto-scaling-group-name",
                self.asg_name,
                "--filters",
                f"Name=StartTimeLowerBound,Values={start_time}",
                f"Name=StartTimeUpperBound,Values={end_time}",
                "--max-records",
                "100",
                *self.aws_context,
                "--output",
                "json",
                "--no-cli-pager",
                "--no-paginate",
            ]
            if next_token is not None:
                argv.extend(["--next-token", next_token])
            page, next_token = parse_scaling_activities_page(
                self.commands.run(argv)
            )
            activities.extend(page)
            if next_token is None:
                return activities
            if next_token in seen_tokens:
                raise ValueError("ASG scaling activities pagination token repeated")
            seen_tokens.add(next_token)

    def _journal(self, unit: str, *, since_epoch_ms: int | None = None) -> str:
        since_epoch_ms = self.t0_epoch_ms if since_epoch_ms is None else since_epoch_ms
        return self.commands.run(
            [
                "journalctl",
                "--unit",
                unit,
                "--since",
                f"@{since_epoch_ms / 1000:.3f}",
                "--output=json",
                "--no-pager",
            ]
        )

    def _current_capacity(self) -> tuple[int, int, int]:
        asg_text = self.commands.run(
            [
                "aws",
                "autoscaling",
                "describe-auto-scaling-groups",
                "--auto-scaling-group-names",
                self.asg_name,
                *self.aws_context,
                "--query",
                "AutoScalingGroups[0].{DesiredCapacity:DesiredCapacity,InService:length(Instances[?LifecycleState==`InService`])}",
                "--output",
                "json",
                "--no-cli-pager",
            ]
        )
        asg_desired, ec2_in_service = parse_asg_counts(asg_text)
        nomad_text = self.commands.run(
            [
                "curl",
                "--fail",
                "--silent",
                "--show-error",
                "--max-time",
                "5",
                self.nomad_nodes_url,
            ]
        )
        return asg_desired, ec2_in_service, parse_nomad_ready(nomad_text)

    def baseline(self) -> dict[str, object]:
        sampled_at_ms = self.clock_epoch_ms()
        controller_journal = self._journal(self.controller_unit)
        workload = parse_latest_controller_workload(
            controller_journal,
            since_epoch_ms=self.t0_epoch_ms,
            before_epoch_ms=int(sampled_at_ms) + 1,
        )
        if workload != 0:
            raise ValueError(
                f"controller workload must remain zero at the post-T0 baseline barrier, observed {workload}"
            )
        asg_desired, ec2_in_service, nomad_ready = self._current_capacity()
        return {
            "runId": self.run_id,
            "elapsedMs": round(max(0, sampled_at_ms - self.t0_epoch_ms), 3),
            "baseline": True,
            "benchmarkRunHash": self.run_hash,
            "serverAdmitted": 0,
            "runningSandboxes": workload,
            "asgDesired": asg_desired,
            "ec2InService": ec2_in_service,
            "nomadReady": nomad_ready,
        }

    def sample(self) -> list[dict[str, object]]:
        api_journal = self._journal(self.api_unit)
        controller_journal = self._journal(self.controller_unit)
        admission_timestamps = list(
            parse_api_admissions(
                api_journal,
                t0_epoch_ms=self.t0_epoch_ms,
                expected_capacity_mode=self.expected_capacity_mode,
                expected_run_hash=self.run_hash,
                target=self.target,
            )
        )

        pending_audits: list[tuple[float, dict[str, object], str]] = []
        for timestamp_ms, audit, raw_message in parse_controller_audits(
            controller_journal,
            t0_epoch_ms=self.t0_epoch_ms,
        ):
            row_key = (timestamp_ms, raw_message)
            if row_key in self.emitted_controller_rows:
                continue
            self.emitted_controller_rows.add(row_key)
            pending_audits.append((timestamp_ms, audit, raw_message))

        asg_desired, ec2_in_service, nomad_ready = self._current_capacity()
        sampled_at_ms = self.clock_epoch_ms()
        emitted_elapsed_ms = round(max(0, sampled_at_ms - self.t0_epoch_ms), 3)
        activities = self._scaling_activities(sampled_at_ms)
        events = [
            {
                "runId": self.run_id,
                "elapsedMs": emitted_elapsed_ms,
                "sourceElapsedMs": round(timestamp_ms - self.t0_epoch_ms, 3),
                "controllerAudit": audit,
            }
            for timestamp_ms, audit, _ in pending_audits
        ]
        events.append(
            {
                "runId": self.run_id,
                "elapsedMs": emitted_elapsed_ms,
                "benchmarkRunHash": self.run_hash,
                "serverAdmitted": len(admission_timestamps),
                **(
                    {
                        "serverAdmissionFirstElapsedMs": round(
                            min(admission_timestamps) - self.t0_epoch_ms, 3
                        ),
                        "serverAdmissionLastElapsedMs": round(
                            max(admission_timestamps) - self.t0_epoch_ms, 3
                        ),
                    }
                    if admission_timestamps
                    else {}
                ),
                "asgDesired": asg_desired,
                "ec2InService": ec2_in_service,
                "nomadReady": nomad_ready,
                "asgActivityEvidence": {
                    "complete": True,
                    "asgName": self.asg_name,
                    "finalDesired": asg_desired,
                    "windowStartEpochMs": self.t0_epoch_ms,
                    "windowEndEpochMs": int(sampled_at_ms),
                    "activities": activities,
                },
            }
        )
        return events


def wait_for_cold_baseline(
    collector: ObserverCollector,
    *,
    timeout_seconds: float,
    clock_monotonic,
    sleep,
) -> dict[str, object]:
    deadline = clock_monotonic() + timeout_seconds
    while True:
        try:
            return collector.baseline()
        except ControllerSnapshotNotReadyError:
            remaining = deadline - clock_monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    "controller did not publish a zero-workload snapshot after T0"
                )
            sleep(min(0.1, remaining))


def prepare_output(path: pathlib.Path) -> None:
    if path.is_symlink():
        raise ValueError("observer output must not be a symbolic link")
    if not path.exists():
        raise ValueError("observer output must exist before the benchmark starts")
    if not path.is_file():
        raise ValueError("observer output must be a regular file")
    if path.stat().st_size != 0:
        raise ValueError("observer output must be empty before collection starts")
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)


def append_events(path: pathlib.Path, events: Iterable[dict[str, object]]) -> None:
    with path.open("a", encoding="utf-8") as output:
        for event in events:
            output.write(json.dumps(event, separators=(",", ":"), sort_keys=True))
            output.write("\n")
        output.flush()
        os.fsync(output.fileno())


def verify_aws_account(
    commands: CommandRunner,
    credential_source: str,
    *,
    region: str,
    profile: str | None,
    expected_account: str,
) -> None:
    account = commands.run(
        [
            "aws",
            "sts",
            "get-caller-identity",
            *aws_context_args(
                credential_source,
                region=region,
                profile=profile,
            ),
            "--query",
            "Account",
            "--output",
            "text",
            "--no-cli-pager",
        ]
    ).strip()
    if account != expected_account:
        raise ValueError("AWS account does not match the explicit benchmark account")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--t0",
        required=True,
        help="runner T0 as a 13-digit epoch millisecond timestamp",
    )
    parser.add_argument("--output", required=True, type=pathlib.Path)
    parser.add_argument("--target", type=int, default=500)
    parser.add_argument("--aws-region", required=True)
    parser.add_argument("--expected-aws-account", required=True)
    parser.add_argument("--asg-name", required=True)
    parser.add_argument("--aws-profile")
    parser.add_argument("--api-unit", required=True)
    parser.add_argument("--controller-unit", required=True)
    parser.add_argument("--nomad-nodes-url", required=True)
    parser.add_argument(
        "--credential-source",
        required=True,
        choices=("local-profile", "instance-role"),
    )
    parser.add_argument(
        "--capacity-mode",
        required=True,
        choices=("dual-write", "start-intent-v1"),
    )
    parser.add_argument("--interval-seconds", type=float, default=5.0)
    parser.add_argument("--baseline-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--once", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if not RUN_ID_PATTERN.fullmatch(args.run_id):
        raise ValueError("run ID contains unsupported characters")
    if args.target <= 0:
        raise ValueError("target must be positive")
    if args.interval_seconds <= 0:
        raise ValueError("interval must be positive")
    if args.baseline_timeout_seconds <= 0:
        raise ValueError("baseline timeout must be positive")
    t0_epoch_ms = parse_t0(args.t0)
    prepare_output(args.output)

    commands = CommandRunner()
    verify_aws_account(
        commands,
        args.credential_source,
        region=args.aws_region,
        profile=args.aws_profile,
        expected_account=args.expected_aws_account,
    )
    collector = ObserverCollector(
        run_id=args.run_id,
        t0_epoch_ms=t0_epoch_ms,
        expected_capacity_mode=args.capacity_mode,
        target=args.target,
        credential_source=args.credential_source,
        aws_region=args.aws_region,
        aws_profile=args.aws_profile,
        asg_name=args.asg_name,
        api_unit=args.api_unit,
        controller_unit=args.controller_unit,
        nomad_nodes_url=args.nomad_nodes_url,
        commands=commands,
        clock_epoch_ms=lambda: time.time_ns() / 1_000_000,
    )
    wait_until_epoch_ms(
        t0_epoch_ms,
        clock_epoch_ms=lambda: time.time_ns() / 1_000_000,
        sleep=time.sleep,
    )
    append_events(
        args.output,
        [
            wait_for_cold_baseline(
                collector,
                timeout_seconds=args.baseline_timeout_seconds,
                clock_monotonic=time.monotonic,
                sleep=time.sleep,
            )
        ],
    )
    while True:
        append_events(args.output, collector.sample())
        if args.once:
            return 0
        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
