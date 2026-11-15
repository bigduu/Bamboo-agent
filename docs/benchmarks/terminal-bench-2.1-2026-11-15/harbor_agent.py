"""Harbor 0.25.0 adapter for the recorded Bamboo Terminal-Bench run."""

import json
import os
from pathlib import Path
import shlex

from harbor.agents.installed.base import BaseInstalledAgent, with_prompt_template


class BambooBench(BaseInstalledAgent):
    @staticmethod
    def name() -> str:
        return "bamboo"

    def get_version_command(self) -> str:
        return "/installed-agent/bamboo --version"

    def parse_version(self, stdout: str) -> str:
        return stdout.strip().removeprefix("bamboo ")

    async def install(self, environment) -> None:
        binary = Path(os.environ["BAMBOO_BENCH_BINARY"]).expanduser().resolve(strict=True)
        await environment.upload_file(binary, "/installed-agent/bamboo")
        result = await self.exec_as_root(
            environment,
            'test "$(uname -m)" = x86_64 && '
            "chmod 0755 /installed-agent/bamboo && /installed-agent/bamboo --version",
        )
        if result.return_code != 0:
            raise RuntimeError("Bamboo x86-64 binary failed to start in the task container")

    @with_prompt_template
    async def run(self, instruction: str, environment, context) -> None:
        config_path = Path(__file__).with_name("run-config.json")
        settings = json.loads(config_path.read_text(encoding="utf-8"))["recorded_run"]
        model = settings["model_alias"]
        data_dir = "/tmp/bamboo-harbor"
        config = {
            "provider": "openai",
            "providers": {
                "openai": {
                    "base_url": os.environ["BAMBOO_BENCH_BASE_URL"],
                    "model": model,
                    "sticky_tool_loading": settings["sticky_tool_loading"],
                },
            },
            "tools": {"disabled": settings["disabled_tools"]},
        }
        setup = await self.exec_as_agent(
            environment,
            f"umask 077 && mkdir -p {data_dir} && "
            f"printf '%s\\n' {shlex.quote(json.dumps(config))} > {data_dir}/config.json",
        )
        if setup.return_code != 0:
            raise RuntimeError("Bamboo could not initialize the trial data directory")

        command = shlex.join([
            "/installed-agent/bamboo", "-p", instruction, "--workspace", ".",
            "--data-dir", data_dir, "--model", f"openai:{model}", "--stream-json",
            "--permission-mode", settings["permission_mode"],
            "--reasoning-effort", settings["reasoning_effort"],
        ])
        log_dir = shlex.quote(self.environment_logs_dir.as_posix())
        result = await self.exec_as_agent(
            environment,
            f"{command} > {log_dir}/bamboo-events.jsonl 2> {log_dir}/bamboo-stderr.log",
            env={
                "BAMBOO_JIANDU_DATA_DIR": f"{data_dir}/jiandu",
                "BAMBOO_OPENAI_API_KEY": os.environ["BAMBOO_BENCH_API_KEY"],
            },
        )
        context.metadata = {"bamboo_exit_code": result.return_code, "architecture": "x86_64"}
