from peopledd.services.cvm_client import parse_cad_cia_aberta_lines


def test_parse_official_style_header_setor_and_site():
    csv_lines = [
        "CNPJ_CIA;DENOM_SOCIAL;DENOM_COMERC;SIT;CD_CVM;SETOR_ATIV;SITE_RELAC_INVESTIDORES",
        "60.872.504/0001-23;Itau Unibanco Holding S.A.;ITAU UNIBANCO;ATIVO;19348;Bancos;https://ri.itau.com.br",
    ]
    found = parse_cad_cia_aberta_lines(csv_lines, "itau")
    assert len(found) == 1
    c = found[0]
    assert c.cnpj == "60.872.504/0001-23"
    assert "Itau" in c.nome_razao_social
    assert c.cod_cvm == "19348"
    assert c.setor == "Bancos"
    assert c.site_ri == "https://ri.itau.com.br"


def test_parse_match_by_cnpj():
    csv_lines = [
        "CNPJ_CIA;DENOM_SOCIAL;DENOM_COMERC;SIT;CD_CVM;SETOR_ATIV",
        "12.345.678/0001-99;Acme Brasil S.A.;ACME;ATIVO;1;Industria",
    ]
    found = parse_cad_cia_aberta_lines(csv_lines, "12345678000199")
    assert len(found) == 1
    assert found[0].cnpj == "12.345.678/0001-99"
    assert found[0].setor == "Industria"
    assert found[0].site_ri is None


def test_parse_setor_fallback_column_setor():
    csv_lines = [
        "CNPJ_CIA;DENOM_SOCIAL;DENOM_COMERC;SIT;SETOR",
        "12.345.678/0001-99;Gamma S.A.;GAMMA;ATIVO;Utilities",
    ]
    found = parse_cad_cia_aberta_lines(csv_lines, "gamma")
    assert len(found) == 1
    assert found[0].setor == "Utilities"
