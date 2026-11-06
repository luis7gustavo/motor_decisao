# S06 — proposta de retomada após HTTP 301

Data: 28/10/2026. Estado atual: orçamento `pncp:public-read:shared-docker-egress` desabilitado, missão de sonda pausada, **4/12** tentativas consumidas. Nenhuma tentativa será apagada ou renovada.

A tentativa 4 usou o detalhe da API de Integração em `/api/pncp` e recebeu 301 com indicação de porta 55969. O [OpenAPI oficial da API de Consulta](https://pncp.gov.br/api/consulta/swagger-ui/index.html) também documenta `GET /v1/orgaos/{cnpj}/compras/{ano}/{sequencial}`. A retomada proposta não segue o endereço indicado pela resposta; constrói uma nova requisição apenas na base oficial `https://pncp.gov.br/api/consulta` já usada nas sondas 2 e 3.

1. Conferir novamente hash dos dois contratos, OID/revisão do destino, estado do C2 e orçamento persistente 4/12.
2. Alterar somente o construtor de URL da sonda de `detail` para a base de Consulta e verificar a URL gerada sem rede.
3. Reativar o **mesmo** orçamento e missão, com limite 12 e contagem 4 preservados.
4. Fazer uma tentativa de detalhe para a identidade observada na tentativa 2. Reservar antes de DNS, salvar corpo/recibo/hash e resultado.
5. Se a resposta for 200 e validar a identidade, continuar os recursos de itens, resultados, arquivos e histórico apenas em rotas documentadas, uma de cada vez, dentro das oito tentativas restantes. Qualquer novo 3xx, 401/403, 400/422 estrutural, 429 ou divergência de schema pausa o orçamento imediatamente; não seguir redirecionamento nem trocar porta por inferência.

Mesmo com respostas positivas, esta etapa ainda será amostra operacional. Cobertura inicial, semântica das bordas, quota oficial, dimensionamento nacional e integridade de anexos permanecem gates separados da missão histórica principal. Redis/Prefect compartilhado não participa desta retomada.
