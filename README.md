# source-kwai-ads

Conector Airbyte (Python CDK) para a Kwai Marketing API (MAPI), escopo `ad_mapi_report`.

## Streams

| Stream | Endpoint MAPI | Modo |
|---|---|---|
| `advertisers` | `crmAccountQueryByAgentOrCorp` | full_refresh (parent) |
| `campaigns` | `dspCampaignEffectQuery` | full_refresh |
| `ad_groups` | `dspUnitEffectQuery` | full_refresh |
| `ads` | `dspCreativeEffectQuery` | full_refresh |
| `ads_reports_daily` | `dspCreativeEffectQuery` (granularity=3) | incremental |

`campaigns`, `ad_groups` e `ads` trazem apenas id, nome e métricas do período — a MAPI não expõe metadado completo (budget, bid, status, targeting) sob o escopo `ad_mapi_report`.

## Limitações conhecidas

- Não há segmentação por idade, gênero ou região sub-nacional em nenhum endpoint documentado da MAPI. O único corte demográfico/geográfico disponível é país e sistema operacional.
- Uma conta nova adicionada sob o token do app leva até 6h para aparecer nas operações da MAPI (cache interno do Kwai).
- Rate limits: 100.000 requests/dia por developer; 10.000/dia por conta de anunciante por interface.

## Configuração local

```bash
poetry install
poetry run source-kwai-ads spec
poetry run source-kwai-ads check --config secrets/config.json
poetry run source-kwai-ads discover --config secrets/config.json
poetry run source-kwai-ads read --config secrets/config.json --catalog integration_tests/configured_catalog.json
```

`secrets/config.json` nunca deve ser versionado (já está no `.gitignore`).

## Build e publicação da imagem

```bash
docker build --platform linux/amd64 -t jaberpablo/source-kwai-ads:0.1.0 .
docker run --rm jaberpablo/source-kwai-ads:0.1.0 spec
docker login
docker push jaberpablo/source-kwai-ads:0.1.0
```

Import no Airbyte self-hosted: **Settings → Sources → New connector → Add a new Docker connector**, usando `jaberpablo/source-kwai-ads` como Docker repository name e `0.1.0` como tag.
