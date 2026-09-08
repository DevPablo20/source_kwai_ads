# Changelog

## 0.2.0

Correções de deduplicação e de grão dos registros. **Esta versão muda a primary key e o
cursor de `ads_reports_daily`** — após atualizar, faça um *refresh schema* na conexão e
uma re-sincronização completa do stream.

### Deduplicação: registros colapsavam no destino

`time` é epoch em milissegundos (inteiro), mas o schema o declarava `string`. Todos os
registros violavam o próprio schema declarado, e `time` era ao mesmo tempo componente da
primary key e cursor. Destinos que validam tipos anulam o valor incompatível, e uma
primary key com componente nulo colapsa o histórico inteiro de um criativo numa única
linha.

- `ads_reports_daily` passa a expor `date` (`YYYY-MM-DD`, derivado de `time` no fuso
  configurado). Primary key agora é `accountId` + `creativeId` + `date`; cursor agora é
  `date`, coerente com o formato do state.
- `time` é redeclarado como inteiro em todos os schemas de relatório e continua no
  registro com o valor bruto.

### `campaigns` / `ad_groups` / `ads` reportavam um dia arbitrário como total do período

Esses streams pediam `granularity=3` (diário) e depois descartavam em memória tudo menos
a primeira linha de cada entidade, de modo que as métricas sobreviventes descreviam um
único dia sob uma coluna que se lê como total do período.

- Passam a usar `granularity=1`, o consolidado real da API, e nenhum registro é
  descartado. Confirmado que o consolidado reconcilia exatamente com a soma das linhas
  diárias.

### `end_date` perdia o último dia

`_date_range_ms` interpretava o `end_date` inclusivo como meia-noite do *início* do dia,
descartando o dia final de todos os streams de entidade.

### Streams vazios ao usar `corp_id` sem `account_ids`

Os quatro streams de relatório compartilhavam uma única instância do stream parent
`advertisers`. O parent é consumido via `Stream.read()`, que registra conclusão no
próprio cursor de um stream full-refresh resumível — então o primeiro stream a ler
recebia as contas e os outros três recebiam lista vazia, **sincronizando nada e
reportando sucesso**. O caminho só se torna alcançável com `account_ids` vazio, por isso
passou despercebido.

- Cada stream de relatório recebe a própria instância do parent, e a lista de contas é
  resolvida uma única vez por stream. Nenhuma das duas proteções sozinha é suficiente.

### Outros

- `cost_decimal` passa a ser adicionado a todos os streams de relatório (antes só em
  `ads_reports_daily`, embora os demais schemas já o declarassem).
- `advertisers` expõe `currency` e `legalEntityName`, que a API retorna.
- Descrições da spec reescritas: `corp_id` identifica a **anunciante**, não a agência, e
  listar por ele expõe contas de outras agências da mesma corporação. `agent_id` marcado
  como desaconselhado.
- `advertisers` removido do `configured_catalog.json` padrão — vira opt-in para
  auditoria, já que lista a corporação inteira.
- `scripts/compare_scopes.py`: audita o que um conjunto de credenciais alcança de fato,
  em vez de confiar no `scope` anunciado no OAuth.

## 0.1.0

Versão inicial.
