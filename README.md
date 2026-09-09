# source-kwai-ads

Conector Airbyte (Python CDK) para a Kwai Marketing API (MAPI), escopo `ad_mapi_report`.

## Streams

| Stream | Endpoint MAPI | Modo | Primary key | Cursor |
|---|---|---|---|---|
| `advertisers` | `crmAccountQueryByAgentOrCorp` | full_refresh (parent) | `accountId` | — |
| `campaigns` | `dspCampaignEffectQuery` (granularity=1) | full_refresh | `campaignId` | — |
| `ad_groups` | `dspUnitEffectQuery` (granularity=1) | full_refresh | `unitId` | — |
| `ads` | `dspCreativeEffectQuery` (granularity=1) | full_refresh | `creativeId` | — |
| `ads_reports_daily` | `dspCreativeEffectQuery` (granularity=3) | incremental | `accountId` + `creativeId` + `date` | `date` |

`campaigns`, `ad_groups` e `ads` trazem apenas id, nome e métricas **consolidadas do período** — a MAPI não expõe metadado completo (budget, bid, status, targeting) sob o escopo `ad_mapi_report`. `ads_reports_daily` é a tabela-fato diária, uma linha por criativo por dia.

Todo stream de relatório recebe `cost_decimal` (o `cost` da Kwai vem em micro-unidades, 10^6).

## Quais contas serão extraídas

**Leia esta seção antes de configurar.** Um `corp_id` identifica a **anunciante**, não a sua agência. Uma mesma corporação normalmente contém as contas de várias agências concorrentes que atendem aquele anunciante — deixar `account_ids` vazio sincroniza **todas elas**, inclusive contas que a sua organização pode não ter direito de ler.

O modo recomendado:

1. Configure `corp_id` e selecione **apenas** o stream `advertisers` numa execução de auditoria, para descobrir quais contas existem.
2. Identifique as que pertencem à sua organização.
3. Preencha `account_ids` com essa lista e mantenha `advertisers` desmarcado nas sincronizações regulares.

Com `account_ids` preenchido, os streams de relatório consultam exatamente essas contas e **nunca** chamam o endpoint de listagem. Repita o passo 1 periodicamente para detectar contas novas — a lista fixa não se atualiza sozinha.

`agent_id` é desaconselhado: registros de app do tipo "channel developer" são recusados ao listar contas por `agentId` (`The channel developer can not use agentId to query account list`). Use `corp_id`.

## Comportamento incremental

`ads_reports_daily` mantém cursor **por conta** (`{"<account_id>": {"date": "2026-08-01"}}`), porque contas diferentes da mesma agência avançam em ritmos diferentes e um cursor único faria o sync repetir — ou pular — dias da conta mais atrasada.

Cada execução retoma de `max(start_date, última_data − lookback_window_days)`, recobrindo alguns dias para capturar revisões retroativas de conversão. O cursor só avança depois que uma janela inteira é lida.

`date` é derivado de `time` (epoch em milissegundos, no fuso de `time_zone`). O `time` bruto é preservado no registro. **Use `date`, não `time`, como chave em qualquer modelagem downstream**: `time` é um inteiro, e destinos que validam tipos anulam campos incompatíveis — um componente de primary key nulo colapsa o histórico inteiro de um criativo numa linha só.

## Limitações conhecidas

- Não há segmentação por idade, gênero ou região sub-nacional em nenhum endpoint documentado da MAPI. O único corte demográfico/geográfico disponível é país e sistema operacional, via `dspPopulationAnalysisEffectQuery` (não usado por este conector).
- Uma conta nova adicionada sob o token do app leva até 6h para aparecer nas operações da MAPI (cache interno do Kwai).
- Rate limits: 100.000 requests/dia por developer; 10.000/dia por conta de anunciante por interface.

### Divergências entre a documentação oficial e o comportamento real

Confirmadas em testes ao vivo contra os endpoints `*EffectQuery`:

| Campo | Documentação | Comportamento real |
|---|---|---|
| `granularity` | `1` = consolidado, `2` = diário | `1` = consolidado, `2` = **horário** (máx. 3 dias), `3` = **diário**, `4` = inválido |
| paginação | campo `page` | `page` é **ignorado**; o campo honrado é `pageNo` |
| `time` | string `YYYY-MM-DD` | inteiro, epoch em milissegundos |
| envelope de sucesso | `{status: "OK"}` | `{status: 200, data: {data: [...]}}` |
| envelope de erro | `{status, message}` | `{result, err_msg, host, port, timestamp, traceId}` |

A tabela de `granularity` da documentação descreve `dspPopulationAnalysisEffectQuery` e não vale para os endpoints `*EffectQuery`.

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

`secrets/config.json` nunca deve ser versionado (já está no `.gitignore`, assim como `.venv/`). Preencha `account_ids` com as contas que a sua organização tem direito de ler — ver [Quais contas serão extraídas](#quais-contas-serão-extraídas). Listar contas por `agent_id` é recusado para registros "channel developer"; por `corp_id` funciona, mas devolve a corporação inteira, incluindo contas de outras agências.

Testes unitários:

```bash
poetry run pytest unit_tests/ -q
```

Auditoria de escopo de credenciais (compara o que dois conjuntos de credenciais alcançam de fato, em vez de confiar no `scope` anunciado):

```bash
poetry run python scripts/compare_scopes.py secrets/config.json secrets/config_outro.json
```

## Build e publicação da imagem

```bash
docker build --platform linux/amd64 -t devpablo20/source-kwai-ads:0.2.0 .
docker run --rm devpablo20/source-kwai-ads:0.2.0 spec
docker login
docker push devpablo20/source-kwai-ads:0.2.0
```

Import no Airbyte self-hosted: **Settings → Sources → New connector → Add a new Docker connector**, usando `devpablo20/source-kwai-ads` como Docker repository name e `0.2.0` como tag.
