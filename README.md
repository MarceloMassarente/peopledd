# peopledd

Pipeline de referência para o **SPEC v1.1 — Company Organization & Governance X-ray**.

## O que está implementado

- Estrutura completa do pipeline `n0` a `n9`.
- Modelos Pydantic para entrada, saídas intermediárias e relatório final.
- Dual-track de governança (`formal` CVM + `current` RI) e reconciliação explícita.
- Classificação `holding|opco|subsidiary|mixed|unknown`.
- Service levels `SL1..SL5` e matriz de degradação explícita.
- Scoring de cobertura com normalização por tamanho do órgão.
- Evidence pack auditável e persistência dos artefatos por execução.

> Esta implementação é um esqueleto funcional e extensível, com conectores externos em modo stub para facilitar evolução incremental em gates.

## Executar

```bash
python -m peopledd.cli --company-name "Itaú Unibanco" --output-dir run --output-mode both
```

Ou via script instalado:

```bash
peopledd --company-name "Itaú Unibanco" --output-dir run --output-mode both
```

## Estrutura

```text
src/peopledd/
  cli.py
  orchestrator.py
  models/
  nodes/
  services/
  utils/
```

## Roadmap sugerido (gates)

1. Gold Set manual (5–8 empresas).
2. Hardening de `n0/n1/n1b`.
3. Benchmark de `useful_coverage` em `n2/n3`.
4. Integração assistida de `n4..n7`.
5. Escala controlada por setor e por modo de empresa.
