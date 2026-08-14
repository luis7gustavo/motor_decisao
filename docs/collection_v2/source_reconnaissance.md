# Reconnaissance de novas fontes

Verificação realizada em 12/08/2026, sem login e sem contornar controles de acesso. A classificação considera a existência de preço utilizável pelo SILLO, não apenas uma página institucional pública.

| Fonte | Tipo | URL analisada | Público / JS / login | Estrutura encontrada | Estratégia e viabilidade | Complexidade |
|---|---|---|---|---|---|---|
| Fujioka Distribuidor | supplier/national_b2b | [site e termos](https://www.fujiokadistribuidor.com.br/institucional/termos-uso) | Catálogo público parcial; preço correto exige login/CNPJ | Loja B2B com preço condicionado ao cliente | `AUTH_REQUIRED`; adapter futuro, desativado | Média após autorização |
| Mega Market | supplier/national_b2b | Identidade oficial não confirmada inequivocamente | Indeterminado | Resultados homônimos e portais de login | `NOT_VIABLE` até confirmar empresa/URL | Indeterminada |
| Agis Distribuição | supplier/national_b2b | [e-commerce](https://vendas.agis.com.br/) | Login/cadastro obrigatório para estoque e valores | Vitrine de produtos sem evidência comercial pública útil | `AUTH_REQUIRED`; não coletar | Média após autorização |
| Bluevix / Bluecase | supplier/national_b2b | [site](https://www.bluevix.com.br/) | Área de conta/sessão; preço B2B público não confirmado | Site institucional e conta | `AUTH_REQUIRED`; não coletar | Média/alta |
| Grupo TEK | supplier/regional_b2b | [catálogo](https://www.tekdistribuidor.com.br/) | HTML público, sem JS obrigatório e sem login para preços | Cards Tray, nome, URL e preço atual | `PUBLIC_HTTP` com Parsel; implementado e habilitado, schedule pausado | Baixa |
| Martins Atacado | supplier/national_b2b | [informática](https://www.martinsatacado.com.br/departamentos/informatica) | Catálogo público, mas preço/estoque dependem de cadastro e região | Lista de categorias/produtos com “Ver preço” | `AUTH_REQUIRED` para preço útil | Média/alta |
| Allied | supplier/national_b2b | [negócios](https://alliedbrasil.com.br/nossos-negocios/) | Contato corporativo; sem catálogo/preço público encontrado | Formulário e portfólio institucional | `COMMERCIAL_INTEGRATION_REQUIRED` | Alta |
| TD SYNNEX | supplier/national_b2b | [seja parceiro](https://lac.tdsynnex.com/br/pt-br/seja-um-parceiro/) | Cadastro, CNPJ/CNAE e aprovação | Portal de parceiros/revendedores | `COMMERCIAL_INTEGRATION_REQUIRED` | Alta |
| Band Atacadista | supplier/regional_b2b | [site](https://bandatacadista.com.br/) | Site público; catálogo/preço não confirmados | Conteúdo institucional e contato comercial | `COMMERCIAL_INTEGRATION_REQUIRED` | Média/alta |
| OMNI Distribuidora | supplier/regional_b2b | [site](https://www.omnidistribuidora.com.br/) | Público institucional; sem catálogo/preço encontrado | Linhas atendidas e contato | `COMMERCIAL_INTEGRATION_REQUIRED` | Média |
| Sol Atacadista | supplier/national_b2b | [landing page](https://lp.solatacadista.com.br/) | Landing page; sem catálogo/preço público encontrado | Conteúdo comercial | `COMMERCIAL_INTEGRATION_REQUIRED` | Média/alta |
| FCInfo | market/local_retailer | [loja](https://www.fcinfo.com.br/) | Preços indexados publicamente; conexão HTTP do ambiente de teste foi instável | Cards de hardware e acessórios com nome/preço | `PUBLIC_HTTP`; adapter declarativo implementado, desativado até canary | Baixa/média |
| Netshop Informática | market/local_retailer | [produtos](https://www.netshopinformatica.com.br/produtos) | Página pública; estrutura dinâmica/instável | Catálogo e páginas de produto, seletor não confirmado | `PUBLIC_BROWSER`; adapter Adaptive preparado, desativado até canary | Média |
| Cia da Informática | market/local_retailer | [loja](https://www.ciainfor.com.br/loja/) | WooCommerce HTML público, sem login | `li.product`, título, preço e URL | `PUBLIC_HTTP` com Parsel; implementado e habilitado, schedule pausado | Baixa |
| GamerStar | market/local_retailer | [site](https://www.gamerstar.com.br/) | HTTP 403 no reconnaissance | Conteúdo indexado, acesso direto bloqueado | `COLLECTION_BLOCKED`; não contornar | Alta/indeterminada |

## Lotes de implementação

### Lote A — público confirmado

- `grupo_tek`: grava Bronze de fornecedor por `CrawleeSupplierCollector`.
- `cia_informatica`: grava evidência de mercado por `CrawleeCatalogMarketCollector`.

Ambos usam parser declarativo, retries, rate limit, timeout, canary e idempotência das tabelas Bronze existentes. A agenda é criada pausada até validação individual.

### Lote B — adapter pronto, ativação pendente

- `fcinfo`: confirmar a estabilidade TLS e os seletores com fixture capturada do ambiente de execução.
- `netshop_informatica`: confirmar se o HTML útil chega por HTTP antes de manter AdaptivePlaywright.

### Lote C — autorização externa necessária

Fujioka, Agis, Bluevix e Martins só podem avançar após credenciais de revendedor explicitamente autorizadas. Allied, TD SYNNEX, Band, OMNI e Sol dependem de canal comercial/feed oficial. Credenciais futuras devem vir por `.env` ou secret; logs já redigem campos sensíveis.
