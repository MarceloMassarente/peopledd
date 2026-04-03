from peopledd.models.contracts import InputPayload
from peopledd.orchestrator import run_pipeline


def test_pipeline_generates_report(tmp_path):
    payload = InputPayload(company_name="Empresa Exemplo")
    report = run_pipeline(payload, output_dir=str(tmp_path))

    assert report.entity_resolution.input_company_name == "Empresa Exemplo"
    assert report.degradation_profile.service_level in {"SL1", "SL2", "SL3", "SL4", "SL5"}
