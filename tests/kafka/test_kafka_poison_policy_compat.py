from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from agora.core.failures import PoisonRecordClassification

_MODELS_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "agora_plugins"
    / "kafka"
    / "sources"
    / "_models.py"
)


def test_kafka_models_define_fallback_poison_policy_when_core_lacks_export(
    monkeypatch,
) -> None:
    import agora.core.failures as failures

    monkeypatch.delattr(failures, "PoisonRecordPolicy")
    module_name = "tests._compat_kafka_models"
    spec = importlib.util.spec_from_file_location(module_name, _MODELS_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
        policy = module.KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE
        info = module.KafkaPoisonRecordInfo(
            classification=PoisonRecordClassification.UNKNOWN,
            policy=policy,
        )

        assert policy.value == "dlq_and_continue"
        assert info.to_dict()["policy"] == "dlq_and_continue"
    finally:
        sys.modules.pop(module_name, None)
