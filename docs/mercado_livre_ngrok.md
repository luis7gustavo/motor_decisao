# Mercado Livre OAuth com ngrok

Este fluxo serve para desenvolvimento local. O ngrok entrega uma URL HTTPS
publica que aponta para a API local em `http://127.0.0.1:8010`.

## 1. Configurar ngrok

O ngrok ja esta instalado nesta maquina. Falta conectar a conta uma vez:

```powershell
ngrok config add-authtoken SEU_TOKEN_NGROK
```

Depois suba a API local:

```powershell
cd "C:\Users\luisg\revenda assistida\motor_decisao"
docker compose up -d postgres redis api
Invoke-RestMethod http://127.0.0.1:8010/health
```

Abra o tunel:

```powershell
ngrok http 8010
```

Copie a URL `https://...ngrok-free.app` mostrada em `Forwarding`.

## 2. Cadastrar redirect URI no Mercado Livre

No app do Mercado Livre, cadastre exatamente:

```text
https://SUA-URL-NGROK.ngrok-free.app/integracoes/mercado-livre/callback
```

Importante: a URL de redirect precisa bater exatamente com a URL usada na
autorizacao e na troca do token. Se o ngrok gratuito gerar uma URL nova, atualize
tambem o cadastro do app e o `.env`.

## 3. Preencher `.env`

```dotenv
ML_CLIENT_ID=seu_app_id
ML_CLIENT_SECRET=sua_secret_key
ML_REDIRECT_URI=https://SUA-URL-NGROK.ngrok-free.app/integracoes/mercado-livre/callback
```

Recrie o container da API para carregar o `.env`:

```powershell
docker compose up -d --force-recreate api
```

## 4. Gerar URL de autorizacao

Fluxo normal:

```powershell
docker compose run --rm api python scripts/mercado_livre_oauth.py auth-url
```

Se o aplicativo estiver com PKCE habilitado no Mercado Livre:

```powershell
docker compose run --rm api python scripts/mercado_livre_oauth.py auth-url --pkce
```

Abra `authorization_url` no navegador, faça login com a conta administradora e
autorize o app. O Mercado Livre vai redirecionar para o callback local via ngrok.

## 5. Trocar o code por tokens

O callback vai mostrar o `code` e sugerir o comando:

```powershell
docker compose run --rm api python scripts/mercado_livre_oauth.py exchange --code "TG-..."
```

Se usou `--pkce`, adicione o `code_verifier` retornado no passo anterior:

```powershell
docker compose run --rm api python scripts/mercado_livre_oauth.py exchange --code "TG-..." --code-verifier "..."
```

Copie `access_token` e `refresh_token` para o `.env`:

```dotenv
ML_ACCESS_TOKEN=APP_USR-...
ML_REFRESH_TOKEN=TG-...
```

Recrie a API:

```powershell
docker compose up -d --force-recreate api
```

## 6. Usar no coletor

```powershell
docker compose run --rm api python scripts/collect_mercado_livre.py --query notebook --max-items 50
```

Para renovar o token:

```powershell
docker compose run --rm api python scripts/mercado_livre_oauth.py refresh
```
