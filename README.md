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

O projeto usa uma venv **padrão do Python** (`python -m venv`) em `.venv/`, na raiz do repositório — sem nenhuma integração de shell do pyenv envolvida (nem `pyenv activate`, nem `.python-version`, nem shims). Isso evita depender de configuração de shell que pode ou não estar presente na sua máquina (`pyenv virtualenv-init` no `.zshrc`/`.bashrc`).

O pyenv só entra pra garantir que existe um interpretador Python 3.11 instalado — não pra ativar nada.

**Primeira vez** (criar a venv):

```bash
pyenv install 3.11.15   # se ainda não tiver essa versão
~/.pyenv/versions/3.11.15/bin/python3.11 -m venv /var/www/source_kwai_ads/.venv
```

**Toda vez que for rodar algo** (ativar a venv já existente):

```bash
source /var/www/source_kwai_ads/.venv/bin/activate
cd /var/www/source_kwai_ads

poetry install
poetry run source-kwai-ads spec
poetry run source-kwai-ads check --config secrets/config.json
poetry run source-kwai-ads discover --config secrets/config.json
poetry run source-kwai-ads read --config secrets/config.json --catalog integration_tests/configured_catalog.json
```

`source .venv/bin/activate` é o script de ativação padrão do módulo `venv` da biblioteca padrão — funciona em qualquer shell, sem depender de nenhuma configuração adicional. Rodar `poetry run ...` sem antes ativar a venv (ex.: `pyenv activate` sem a integração correta no shell, ou esquecendo o `source` acima) falha silenciosamente com `ModuleNotFoundError: No module named 'airbyte_cdk'`, porque o Poetry cai de volta no interpretador Python 3.11 "pelado" em vez da venv do projeto.

`secrets/config.json` nunca deve ser versionado (já está no `.gitignore`, assim como `.venv/`). Ele precisa de `account_ids` preenchido — a listagem de contas via `agent_id`/`corp_id` é rejeitada para esta conta (ver histórico do projeto), então os streams de relatório só funcionam com IDs de conta explícitos.

## Build e publicação da imagem

```bash
docker build --platform linux/amd64 -t jaberpablo/source-kwai-ads:0.1.0 .
docker run --rm jaberpablo/source-kwai-ads:0.1.0 spec
docker login
docker push jaberpablo/source-kwai-ads:0.1.0
```

Import no Airbyte self-hosted: **Settings → Sources → New connector → Add a new Docker connector**, usando `jaberpablo/source-kwai-ads` como Docker repository name e `0.1.0` como tag.
