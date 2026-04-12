from test_db.generator.workload_generator import generate_workload


def test_generate_workload_smoke():
    workload = generate_workload()
    assert workload.sql_text.strip() != ""
    assert len(workload.statements) > 0