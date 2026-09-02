#!/usr/bin/env python3

import importlib.util
import json
import pathlib
import tempfile
import unittest


SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
MODULE_PATH = SCRIPT_DIR / "capacity-smoke-observer.py"


def load_observer_module():
    spec = importlib.util.spec_from_file_location("observe_progress", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeCommands:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def run(self, argv):
        self.calls.append(argv)
        key = tuple(argv[:2])
        if key not in self.responses:
            raise AssertionError(f"unexpected command: {argv!r}")
        response = self.responses[key]
        if isinstance(response, list):
            return response.pop(0)
        return response


class ObserveProgressTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.observer = load_observer_module()

    def test_cli_accepts_workload_v2_shadow_admission_mode(self):
        args = self.observer.build_parser().parse_args(
            [
                "--run-id",
                "workload-v2-shadow-test",
                "--t0",
                "1787724000000",
                "--output",
                "observer.jsonl",
                "--aws-region",
                "us-west-2",
                "--expected-aws-account",
                "123456789012",
                "--asg-name",
                "benchmark-workers",
                "--api-unit",
                "benchmark-api.service",
                "--controller-unit",
                "benchmark-controller.service",
                "--nomad-nodes-url",
                "http://127.0.0.1:4646/v1/nodes",
                "--credential-source",
                "instance-role",
                "--capacity-mode",
                "workload-v2-shadow",
            ]
        )

        self.assertEqual(args.capacity_mode, "workload-v2-shadow")

    def test_cli_accepts_workload_v2_admission_mode(self):
        args = self.observer.build_parser().parse_args(
            [
                "--run-id",
                "workload-v2-test",
                "--t0",
                "1787724000000",
                "--output",
                "observer.jsonl",
                "--aws-region",
                "us-west-2",
                "--expected-aws-account",
                "123456789012",
                "--asg-name",
                "workers",
                "--api-unit",
                "benchmark-api.service",
                "--controller-unit",
                "benchmark-controller.service",
                "--nomad-nodes-url",
                "http://127.0.0.1:4646/v1/nodes",
                "--credential-source",
                "instance-role",
                "--capacity-mode",
                "workload-v2",
            ]
        )

        self.assertEqual(args.capacity_mode, "workload-v2")

    def test_parses_workload_v2_admission_marker(self):
        run_hash = "4e65d3fbe8ad6535681b021b30785b12b6c0e3f8878859a4148b3f58b8835db0"
        journal = json.dumps(
            {
                "__REALTIME_TIMESTAMP": "1787724000100000",
                "MESSAGE": json.dumps(
                    {
                        "msg": "sandbox workload lease admitted",
                        "capacity_mode": "workload-v2",
                        "benchmark_run_hash": run_hash,
                    }
                ),
            }
        )

        admitted = list(
            self.observer.parse_api_admissions(
                journal,
                t0_epoch_ms=1787724000000,
                expected_capacity_mode="workload-v2",
                expected_run_hash=run_hash,
                target=1,
            )
        )

        self.assertEqual(admitted, [1787724000100.0])

    def test_collects_runner_jsonl_without_sensitive_identifiers(self):
        api_journal = "\n".join(
            [
                json.dumps(
                    {
                        "__REALTIME_TIMESTAMP": "1787724000100000",
                        "MESSAGE": '2026-08-26T10:00:00.100Z  INFO  sandbox start intent admitted  {"service":"api","capacity_mode":"dual-write","benchmark_run_hash":"dac1b8273f2366bb0a9070d29383cc70f4cd35a71fbf26ed5fb41fa08ed3fe60","sandbox_id":"sbx-secret","team_id":"team-secret"}',
                    }
                ),
                json.dumps(
                    {
                        "__REALTIME_TIMESTAMP": "1787724000200000",
                        "MESSAGE": '{"level":"info","msg":"sandbox start intent admitted","capacity_mode":"dual-write","benchmark_run_hash":"dac1b8273f2366bb0a9070d29383cc70f4cd35a71fbf26ed5fb41fa08ed3fe60","sandbox_id":"sbx-secret-2"}',
                    }
                ),
            ]
        )
        controller_journal = "\n".join(
            [
                json.dumps(
                    {
                        "__REALTIME_TIMESTAMP": "1787724000300000",
                        "MESSAGE": "msg=controller_started controller_instance_id=controller-1 mode=start-intent-v1",
                    }
                ),
                json.dumps(
                    {
                        "__REALTIME_TIMESTAMP": "1787724000320000",
                        "MESSAGE": "msg=scale_write_started controller_instance_id=controller-1 scale_write_sequence=7 mode=start-intent-v1 workload_count=40 current_desired=1 target=2 batch_trigger=idle batch_age_ms=1000 batch_idle_age_ms=1000",
                    }
                ),
                json.dumps(
                    {
                        "__REALTIME_TIMESTAMP": "1787724000330000",
                        "MESSAGE": "msg=scale_write_finished controller_instance_id=controller-1 scale_write_sequence=7 mode=start-intent-v1 workload_count=40 current_desired=1 target=2 batch_trigger=idle batch_age_ms=1000 batch_idle_age_ms=1000 outcome=success duration_ms=10 aws_request_id=request-1 error=''",
                    }
                ),
                json.dumps(
                    {
                        "__REALTIME_TIMESTAMP": "1787724000340000",
                        "MESSAGE": "msg=audit_checkpoint controller_instance_id=controller-1 scale_write_sequence=7 mode=start-intent-v1 audit_dropped_total=0 checkpoint_generated_epoch_ms=1787724000340",
                    }
                ),
            ]
        )
        commands = FakeCommands(
            {
                ("journalctl", "--unit"): [api_journal, controller_journal],
                ("aws", "autoscaling"): [
                    json.dumps({"DesiredCapacity": 2, "InService": 1}),
                    json.dumps(
                        {
                            "Activities": [
                                {
                                    "ActivityId": "activity-1",
                                    "StartTime": "2026-08-26T10:00:00Z",
                                    "StatusCode": "Successful",
                                    "Description": "Launching a new EC2 instance: i-0123456789abcdef0",
                                }
                            ]
                        }
                    ),
                ],
                ("curl", "--fail"): json.dumps(
                    [
                        {"ID": "node-secret", "Status": "ready", "Drain": False},
                        {"ID": "node-secret-2", "Status": "down", "Drain": False},
                    ]
                ),
            }
        )
        collector = self.observer.ObserverCollector(
            run_id="capacity-smoke-test",
            t0_epoch_ms=1787724000000,
            expected_capacity_mode="dual-write",
            target=500,
            credential_source="local-profile",
            aws_region="us-west-2",
            aws_profile="benchmark-profile",
            asg_name="benchmark-workers",
            api_unit="benchmark-api.service",
            controller_unit="benchmark-controller.service",
            nomad_nodes_url="http://127.0.0.1:4646/v1/nodes",
            commands=commands,
            clock_epoch_ms=lambda: 1787724000500,
        )

        events = collector.sample()

        self.assertEqual(
            events,
            [
                {
                    "runId": "capacity-smoke-test",
                    "elapsedMs": 500.0,
                    "sourceElapsedMs": 300.0,
                    "controllerAudit": {
                        "event": "controller_started",
                        "controllerInstanceID": "controller-1",
                        "mode": "start-intent-v1",
                    },
                },
                {
                    "runId": "capacity-smoke-test",
                    "elapsedMs": 500.0,
                    "sourceElapsedMs": 320.0,
                    "controllerAudit": {
                        "event": "scale_write_started",
                        "controllerInstanceID": "controller-1",
                        "scaleWriteSequence": 7,
                        "mode": "start-intent-v1",
                        "workloadCount": 40,
                        "currentDesired": 1,
                        "target": 2,
                        "batchTrigger": "idle",
                        "batchAgeMs": 1000,
                        "batchIdleAgeMs": 1000,
                    },
                },
                {
                    "runId": "capacity-smoke-test",
                    "elapsedMs": 500.0,
                    "sourceElapsedMs": 330.0,
                    "controllerAudit": {
                        "event": "scale_write_finished",
                        "controllerInstanceID": "controller-1",
                        "scaleWriteSequence": 7,
                        "mode": "start-intent-v1",
                        "workloadCount": 40,
                        "currentDesired": 1,
                        "target": 2,
                        "batchTrigger": "idle",
                        "batchAgeMs": 1000,
                        "batchIdleAgeMs": 1000,
                        "outcome": "success",
                        "durationMs": 10,
                        "awsRequestId": "request-1",
                        "error": "",
                    },
                },
                {
                    "runId": "capacity-smoke-test",
                    "elapsedMs": 500.0,
                    "sourceElapsedMs": 340.0,
                    "controllerAudit": {
                        "event": "audit_checkpoint",
                        "controllerInstanceID": "controller-1",
                        "scaleWriteSequence": 7,
                        "mode": "start-intent-v1",
                        "auditDroppedTotal": 0,
                        "checkpointGeneratedElapsedMs": 340,
                    },
                },
                {
                    "runId": "capacity-smoke-test",
                    "elapsedMs": 500.0,
                    "benchmarkRunHash": self.observer.hashlib.sha256(
                        b"capacity-smoke-test"
                    ).hexdigest(),
                    "serverAdmitted": 2,
                    "serverAdmissionFirstElapsedMs": 100.0,
                    "serverAdmissionLastElapsedMs": 200.0,
                    "asgDesired": 2,
                    "ec2InService": 1,
                    "nomadReady": 1,
                    "asgActivityEvidence": {
                        "complete": True,
                        "asgName": "benchmark-workers",
                        "finalDesired": 2,
                        "windowStartEpochMs": 1787724000000,
                        "windowEndEpochMs": 1787724000500,
                        "activities": [
                            {
                                "activityId": "activity-1",
                                "startTime": "2026-08-26T10:00:00Z",
                                "statusCode": "Successful",
                                "description": "Launching a new EC2 instance: i-0123456789abcdef0",
                                "action": "launch",
                            }
                        ],
                    },
                },
            ],
        )
        encoded = "\n".join(json.dumps(event) for event in events)
        for secret in ["sbx-secret", "team-secret", "node-secret"]:
            self.assertNotIn(secret, encoded)
        asg_call = next(call for call in commands.calls if call[:2] == ["aws", "autoscaling"])
        self.assertIn("--profile", asg_call)
        self.assertEqual(asg_call[asg_call.index("--profile") + 1], "benchmark-profile")
        self.assertEqual(asg_call[asg_call.index("--region") + 1], "us-west-2")
        activities_call = next(
            call
            for call in commands.calls
            if call[:3] == ["aws", "autoscaling", "describe-scaling-activities"]
        )
        self.assertNotIn("--start-time", activities_call)
        self.assertNotIn("--end-time", activities_call)
        filters_index = activities_call.index("--filters")
        self.assertEqual(
            activities_call[filters_index + 1 : filters_index + 3],
            [
                "Name=StartTimeLowerBound,Values=2026-08-26T06:00:00Z",
                "Name=StartTimeUpperBound,Values=2026-08-26T06:00:00.500000Z",
            ],
        )

    def test_baseline_uses_latest_trusted_controller_snapshot_after_t0(self):
        controller_journal = "\n".join(
            [
                json.dumps(
                    {
                        "__REALTIME_TIMESTAMP": "1787724000100000",
                        "MESSAGE": "msg='capacity reconciled' mode=start-intent-v1 workload_count=1 outcome=success",
                    }
                ),
                json.dumps(
                    {
                        "__REALTIME_TIMESTAMP": "1787724000200000",
                        "MESSAGE": "msg='capacity reconciled' mode=start-intent-v1 workload_count=0 outcome=success",
                    }
                ),
            ]
        )
        commands = FakeCommands(
            {
                ("journalctl", "--unit"): controller_journal,
                ("aws", "autoscaling"): json.dumps(
                    {"DesiredCapacity": 1, "InService": 1}
                ),
                ("curl", "--fail"): json.dumps(
                    [{"Status": "ready", "Drain": False}]
                ),
            }
        )
        collector = self.observer.ObserverCollector(
            run_id="capacity-smoke-test",
            t0_epoch_ms=1787724000000,
            expected_capacity_mode="dual-write",
            target=500,
            credential_source="instance-role",
            aws_region="us-west-2",
            aws_profile=None,
            asg_name="benchmark-workers",
            api_unit="benchmark-api.service",
            controller_unit="benchmark-controller.service",
            nomad_nodes_url="http://127.0.0.1:4646/v1/nodes",
            commands=commands,
            clock_epoch_ms=lambda: 1787724000300,
        )

        baseline = collector.baseline()

        self.assertEqual(baseline["elapsedMs"], 300.0)
        self.assertTrue(baseline["baseline"])
        self.assertEqual(baseline["runningSandboxes"], 0)
        self.assertEqual(baseline["asgDesired"], 1)
        self.assertEqual(baseline["ec2InService"], 1)
        self.assertEqual(baseline["nomadReady"], 1)

    def test_scaling_activities_are_fully_paginated(self):
        commands = FakeCommands(
            {
                ("aws", "autoscaling"): [
                    json.dumps(
                        {
                            "Activities": [
                                {
                                    "ActivityId": "activity-2",
                                    "StartTime": "2026-08-26T10:00:02Z",
                                    "StatusCode": "InProgress",
                                    "Description": "Launching a new EC2 instance: i-0123456789abcdef0",
                                }
                            ],
                            "NextToken": "page-2",
                        }
                    ),
                    json.dumps(
                        {
                            "Activities": [
                                {
                                    "ActivityId": "activity-1",
                                    "StartTime": "2026-08-26T10:00:01Z",
                                    "StatusCode": "Successful",
                                    "Description": "Terminating EC2 instance: i-0123456789abcdef0",
                                }
                            ]
                        }
                    ),
                ]
            }
        )
        collector = self.observer.ObserverCollector(
            run_id="capacity-smoke-test",
            t0_epoch_ms=1787724000000,
            expected_capacity_mode="dual-write",
            target=500,
            credential_source="instance-role",
            aws_region="us-west-2",
            aws_profile=None,
            asg_name="benchmark-workers",
            api_unit="benchmark-api.service",
            controller_unit="benchmark-controller.service",
            nomad_nodes_url="http://127.0.0.1:4646/v1/nodes",
            commands=commands,
            clock_epoch_ms=lambda: 1787724005000,
        )

        activities = collector._scaling_activities(1787724005000)

        self.assertEqual(
            [activity["activityId"] for activity in activities],
            ["activity-2", "activity-1"],
        )
        self.assertNotIn("--next-token", commands.calls[0])
        self.assertEqual(
            commands.calls[1][commands.calls[1].index("--next-token") + 1],
            "page-2",
        )

    def test_rejects_wrong_capacity_mode_and_foreign_admission(self):
        wrong_mode = self.observer.parse_api_admissions(
            json.dumps(
                {
                    "__REALTIME_TIMESTAMP": "1787724000100000",
                    "MESSAGE": 'msg="sandbox start intent admitted" capacity_mode=legacy-failure-ledger',
                }
            ),
            t0_epoch_ms=1787724000000,
            expected_capacity_mode="start-intent-v1",
            expected_run_hash="run-hash",
            target=500,
        )
        with self.assertRaisesRegex(ValueError, "unexpected capacity_mode"):
            list(wrong_mode)

        foreign = self.observer.parse_api_admissions(
            json.dumps(
                {
                    "__REALTIME_TIMESTAMP": "1787724000100000",
                    "MESSAGE": 'msg="sandbox start intent admitted" capacity_mode=start-intent-v1 benchmark_run_hash=other-run',
                }
            ),
            t0_epoch_ms=1787724000000,
            expected_capacity_mode="start-intent-v1",
            expected_run_hash="run-hash",
            target=500,
        )
        with self.assertRaisesRegex(ValueError, "foreign or uncorrelated"):
            list(foreign)

        too_many = "\n".join(
            json.dumps(
                {
                    "__REALTIME_TIMESTAMP": str(1787724000000000 + index),
                    "MESSAGE": 'msg="sandbox start intent admitted" capacity_mode=start-intent-v1 benchmark_run_hash=run-hash',
                }
            )
            for index in range(501)
        )
        with self.assertRaisesRegex(ValueError, "non-runner traffic"):
            list(
                self.observer.parse_api_admissions(
                    too_many,
                    t0_epoch_ms=1787724000000,
                    expected_capacity_mode="start-intent-v1",
                    expected_run_hash="run-hash",
                    target=500,
                )
            )

    def test_ignores_non_text_journal_messages(self):
        colored_message = (
            '2026-08-26T10:00:00.075Z  \x1b[34mINFO\x1b[0m  '
            'sandbox start intent admitted  {"capacity_mode":"dual-write","benchmark_run_hash":"run-hash"}'
        )
        journal = "\n".join(
            [
                json.dumps(
                    {
                        "__REALTIME_TIMESTAMP": "1787724000050000",
                        "MESSAGE": ["binary", "or repeated field"],
                    }
                ),
                json.dumps(
                    {
                        "__REALTIME_TIMESTAMP": "1787724000075000",
                        "MESSAGE": list(colored_message.encode("utf-8")),
                    }
                ),
                json.dumps(
                    {
                        "__REALTIME_TIMESTAMP": "1787724000100000",
                        "MESSAGE": 'msg="sandbox start intent admitted" capacity_mode=dual-write benchmark_run_hash=run-hash',
                    }
                ),
            ]
        )
        self.assertEqual(
            list(
                self.observer.parse_api_admissions(
                    journal,
                    t0_epoch_ms=1787724000000,
                    expected_capacity_mode="dual-write",
                    expected_run_hash="run-hash",
                    target=40,
                )
            ),
            [1787724000075.0, 1787724000100.0],
        )

    def test_static_aws_contract_is_read_only_and_deployment_agnostic(self):
        source = MODULE_PATH.read_text(encoding="utf-8")
        self.assertIn("describe-auto-scaling-groups", source)
        self.assertIn("describe-scaling-activities", source)
        for deployment_value in ["today-aws-2", "097279986318", "e2b-scale500-workers"]:
            self.assertNotIn(deployment_value, source)

        forbidden = [
            "set-desired-capacity",
            "update-auto-scaling-group",
            "terminate-instance-in-auto-scaling-group",
            "set-instance-protection",
            "run-instances",
            "terminate-instances",
            "ssm send-command",
        ]
        for command in forbidden:
            self.assertNotIn(command, source)
        self.assertNotIn("shell=True", source)

    def test_credential_source_is_explicit_without_fallback(self):
        self.assertEqual(
            self.observer.aws_context_args(
                "local-profile", region="us-west-2", profile="benchmark-profile"
            ),
            ["--profile", "benchmark-profile", "--region", "us-west-2"],
        )
        self.assertEqual(
            self.observer.aws_context_args(
                "instance-role", region="us-west-2", profile=None
            ),
            ["--region", "us-west-2"],
        )
        with self.assertRaisesRegex(ValueError, "credential source"):
            self.observer.aws_context_args(
                "auto", region="us-west-2", profile=None
            )

        commands = FakeCommands({("aws", "sts"): "123456789012\n"})
        self.observer.verify_aws_account(
            commands,
            "instance-role",
            region="us-west-2",
            profile=None,
            expected_account="123456789012",
        )
        self.assertNotIn("--profile", commands.calls[0])
        self.assertEqual(
            commands.calls[0][commands.calls[0].index("--region") + 1],
            "us-west-2",
        )

    def test_observer_waits_for_future_t0(self):
        sleeps = []
        times = iter([1787724000000, 1787724005000])
        self.observer.wait_until_epoch_ms(
            1787724005000,
            clock_epoch_ms=lambda: next(times),
            sleep=lambda seconds: sleeps.append(seconds),
        )
        self.assertEqual(sleeps, [5.0])
        with self.assertRaisesRegex(ValueError, "future"):
            self.observer.wait_until_epoch_ms(
                1787724005000,
                clock_epoch_ms=lambda: 1787724005000,
                sleep=lambda _: None,
            )
        for invalid in ["1787724005", "2026-08-26T10:00:05Z", "not-a-time"]:
            with self.assertRaisesRegex(ValueError, "epoch millisecond"):
                self.observer.parse_t0(invalid)

    def test_output_must_be_empty_and_is_written_as_jsonl(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output = pathlib.Path(temp_dir) / "observer.jsonl"
            output.write_text("stale\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "must be empty"):
                self.observer.prepare_output(output)

            output.write_text("", encoding="utf-8")
            self.observer.prepare_output(output)
            self.observer.append_events(
                output,
                [{"runId": "run", "elapsedMs": 1.0, "serverAdmitted": 0}],
            )
            self.assertEqual(
                json.loads(output.read_text(encoding="utf-8")),
                {"runId": "run", "elapsedMs": 1.0, "serverAdmitted": 0},
            )


if __name__ == "__main__":
    unittest.main()
