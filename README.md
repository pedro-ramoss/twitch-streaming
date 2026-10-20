# Twitch Streaming

Coleta transmissões ao vivo da Twitch usando a API Helix e salva a resposta em
`data/raw/twitch_streams.json`.

## Configuração

1. Crie um aplicativo no [Twitch Developer Console](https://dev.twitch.tv/console/apps).
2. Defina `TWITCH_CLIENT_ID` e `TWITCH_CLIENT_SECRET` no arquivo `.env`:

   ```env
   TWITCH_CLIENT_ID=seu_client_id
   TWITCH_CLIENT_SECRET=seu_client_secret
   ```

3. Instale as dependências:

   ```bash
   pip install -r requirements.txt
   ```

## Uso

Execute o cliente a partir da raiz do projeto:

```bash
python -m src.ingestion.twitch_client
```

## Testes

```bash
pytest
```