<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/logo-dark.png">
  <source media="(prefers-color-scheme: light)" srcset="docs/assets/logo-light.png">
  <img src="docs/assets/logo-light.png" alt="fazer.ai" width="200">
</picture>

<h1>fazer.ai WhatsApp Connector</h1>

<p>WhatsApp pelo QR code no Chatwoot fazer.ai.</p>
<p>Sessões no seu servidor, sem API paga de terceiros.</p>

**Português (Brasil)** · [English](README-en.md)

[![Release](https://img.shields.io/github/v/release/fazer-ai/whatsapp-connector)](https://github.com/fazer-ai/whatsapp-connector/releases)
[![Imagem Docker](https://img.shields.io/badge/ghcr.io-whatsapp--connector-2496ED?logo=docker&logoColor=fff)](https://github.com/fazer-ai/whatsapp-connector/pkgs/container/whatsapp-connector)
![Licença: MIT](https://img.shields.io/badge/licen%C3%A7a-MIT-3B82F6)
![Go](https://img.shields.io/badge/Go-00ADD8?logo=go&logoColor=fff)

</div>

> [!IMPORTANT]
> O canal está em beta e aparece no Chatwoot como “WhatsApp (nativo)”, com selo de beta. Vem ligado em toda conta da instalação. A conexão é não oficial, pareada como um aparelho conectado, igual ao WhatsApp Web. Para a API oficial da Meta, use a caixa WhatsApp Cloud do Chatwoot. Relate problemas nas [issues deste repositório](https://github.com/fazer-ai/whatsapp-connector/issues).

## O que é

O fazer.ai WhatsApp Connector é um serviço em Go que mantém sessões de WhatsApp de várias contas num único processo. Cada sessão funciona como um aparelho conectado da conta, pelo protocolo multi-device, usando a biblioteca [whatsmeow](https://github.com/tulir/whatsmeow).

O conector troca eventos e comandos com o [Chatwoot fazer.ai](https://github.com/fazer-ai/chatwoot) pelo Redis que a instalação já usa. Essa comunicação segue o contrato versionado em [`contract/`](contract/). O Chatwoot recebe eventos canônicos, sem depender de detalhes da biblioteca.

A sessão do WhatsApp mantém um socket aberto por longos períodos e cuida da própria reconexão. Como o protocolo do WhatsApp muda com frequência, o conector tem imagem e ciclo de release próprios. Assim, ele pode ser atualizado sem esperar uma release do Chatwoot.

## O que ele faz

### Conexão

- Pareamento por QR code ou por código de 8 caracteres digitado no celular.
- Quando o WhatsApp pede confirmação por passkey, o desafio segue para o Chatwoot e o código de confirmação aparece para o operador.
- A sessão volta sozinha depois de um reinício do conector, sem parear de novo.
- Proxy por sessão, com http, https ou socks5. Se o proxy cair, a sessão nunca se conecta diretamente.
- Importação opcional do histórico do celular ao conectar e pedido de mensagens mais antigas de uma conversa.

### Mensagens

- Texto, imagem, vídeo, áudio, documento, figurinha, localização e contatos, nos dois sentidos.
- Respostas citando mensagens, menções, reações, edição e exclusão.
- Respeita o temporizador de mensagens temporárias da conversa.
- Mídia recebida baixada na hora e entregue ao Chatwoot por HTTP. Se o arquivo sumir do servidor do WhatsApp, o conector pede ao celular do remetente que o reenvie.
- Mídia enviada lida da URL que o Chatwoot informa, em fluxo, sem carregar o arquivo inteiro na memória.
- Mídia de visualização única aparece como indisponível e nunca é guardada.

### Conversa

- Confirmações de entrega e leitura, além da opção de marcar como lida.
- Indicadores de “digitando” e “gravando áudio” nos dois sentidos, além da presença do contato.

### Grupos

- Criar, renomear, alterar descrição, foto e configurações, além de sair do grupo.
- Adicionar, remover, promover e rebaixar participantes.
- Link de convite e pedidos de entrada.

### Contatos e chamadas

- Verificação de número com WhatsApp, consulta de perfil e foto do contato.
- Chamadas recebidas aparecem na caixa de entrada, com recusa automática opcional.

## Feito para não perder mensagem

- O conector só confirma uma mensagem ao WhatsApp depois de publicá-la no Redis. Se o Redis cair, o WhatsApp reentrega a mensagem.
- Quando o Redis volta, o conector encerra a própria conexão com o WhatsApp para receber de novo, na hora, o que ficou sem confirmação. Isso não precisa esperar a próxima queda da conexão.
- Comandos são idempotentes: um envio repetido pela fila não envia a mensagem duas vezes.
- Várias instâncias podem rodar juntas. Cada sessão tem um único dono por vez, definido por uma lease no Redis. Se uma instância cair, outra assume a sessão.
- Se uma instância ficar sem Redis por mais tempo que a lease, ela fecha a sessão sozinha. Outra pode assumir sem que as duas atendam ao mesmo tempo.
- Métricas Prometheus em `/metrics` e endpoints de healthcheck em `/healthz` e `/readyz`, na porta 8080.

## Instalar com o Chatwoot fazer.ai

### Requisitos

- Chatwoot fazer.ai. Para o modo embutido, use uma versão que já traz o conector na imagem. O provedor nativo vem nas duas edições.
- O mesmo Redis do Chatwoot, na versão 6.2 ou mais nova.
- No modo embutido, o usuário do PostgreSQL do Chatwoot precisa poder criar banco. O superusuário criado pela imagem oficial do postgres tem essa permissão. Se o seu usuário não tiver, crie o banco à mão ou configure `WAC_DATABASE_URL`.

### Escolher o modo

- **Embutido (padrão):** o conector roda junto no container do Sidekiq, sem serviço novo na stack. A versão do conector é a que a imagem do Chatwoot fixa.
- **Separado:** o conector roda como serviço próprio, com a imagem `ghcr.io/fazer-ai/whatsapp-connector`. Use para atualizar o conector sem esperar uma release do Chatwoot ou para rodar mais de uma instância.

Os dois modos têm um compose de exemplo: [`docker-compose.embedded.yaml`](examples/docker-compose.embedded.yaml) e [`docker-compose.separate.yaml`](examples/docker-compose.separate.yaml). Os detalhes estão em [docs/deployment.md](docs/deployment.md).

Para experimentar, copie o arquivo de variáveis:

```bash
cp examples/.env.example .env
```

Preencha no `.env` os três segredos: `SECRET_KEY_BASE`, `POSTGRES_PASSWORD` e `REDIS_PASSWORD`. Gere cada um com `openssl rand -hex 32`. O modo separado pede também `WAC_MEDIA_TOKEN`.

Para subir o exemplo embutido:

```bash
docker compose -f examples/docker-compose.embedded.yaml --env-file .env up -d --wait
```

O Chatwoot sobe na porta 3000.

### Modo embutido

É o padrão do Chatwoot e não pede configuração: a integração inteira, o consumidor de eventos no Sidekiq e o próprio conector já vêm ligados. Para desligar tudo, configure no Chatwoot:

```bash
WHATSAPP_CONNECTOR_ENABLED=false
```

O conector usa o mesmo Redis do Chatwoot e cria um banco próprio no mesmo PostgreSQL no primeiro start. O nome é o do banco do Chatwoot seguido de `_whatsapp_connector`. A mídia fica num diretório local do container, e o token de mídia é gerado a cada start do container. O Chatwoot lê esse token sozinho.

Qualquer variável `WAC_*` definida no container do Sidekiq vale no lugar da configuração derivada do Chatwoot. O pareamento sobrevive a reinícios porque fica no banco.

Se o conector cair, ele volta sozinho, sem derrubar o Sidekiq. Ao parar o container, o conector devolve as sessões antes de sair.

### Modo separado

No Chatwoot, configure:

```bash
WHATSAPP_CONNECTOR_EMBEDDED=false
```

Use a imagem pública `ghcr.io/fazer-ai/whatsapp-connector:latest`. Há tags por release, como `0.5.1` e `0.5`. Para fixar a versão, use a tag correspondente.

No conector, configure no mínimo estas variáveis com os valores da sua instalação:

```bash
REDIS_URL=redis://redis:6379
REDIS_PASSWORD=a-senha-do-redis
WAC_ENGINE=whatsmeow
WAC_DATABASE_URL=postgres://wac:senha@postgres:5432/wac
WAC_MEDIA_ROOT=/data/media
WAC_MEDIA_TOKEN=um-token-longo-e-aleatorio
```

- `REDIS_URL` e `REDIS_PASSWORD` usam de propósito os mesmos nomes de variável e apontam para o mesmo servidor do Chatwoot.
- Sem `WAC_MEDIA_ROOT`, nenhuma mídia recebida chega ao Chatwoot. Quando ela está definida, `WAC_MEDIA_TOKEN` é obrigatório. O Chatwoot lê o token sozinho, pelo registro do conector no Redis.
- `WAC_EVENT_SHARDS`, com padrão 16, precisa ser igual a `WHATSAPP_CONNECTOR_EVENT_SHARDS` do Chatwoot.
- Use um banco PostgreSQL próprio do conector para várias instâncias ou SQLite para uma instância só. O compose de exemplo usa SQLite num volume.
- Use armazenamento persistente para o banco e para `WAC_MEDIA_ROOT`.
- A tabela completa de variáveis está em [docs/operations.md](docs/operations.md).

### Atualizar uma instalação com o conector separado

> [!WARNING]
> Se você já roda o conector como serviço separado, configure `WHATSAPP_CONNECTOR_EMBEDDED=false` no Chatwoot antes de atualizar para uma imagem que traz o conector. Sem isso, um segundo conector sobe dentro do Sidekiq, com um banco sem os pareamentos, disputando as mesmas sessões pelo mesmo Redis.

### Tirar o canal de uma conta

Toda conta enxerga o canal. Para tirá-lo de uma conta, use o console Rails do Chatwoot:

```ruby
Account.find(ID_DA_CONTA).update!(whatsapp_native_disabled: true)
```

### Criar a caixa de entrada

No Chatwoot, vá a **Configurações > Caixas de entrada > Adicionar > WhatsApp > WhatsApp (nativo)**. Pareie pelo QR code ou pelo código digitado no celular.

### Migrar uma caixa existente

Você pode converter uma caixa existente, por exemplo Baileys, para o nativo sem perder o histórico de conversas da caixa:

```bash
bundle exec rails "whatsapp:providers:convert[ID_DA_CAIXA,native]"
APPLY=1 bundle exec rails "whatsapp:providers:convert[ID_DA_CAIXA,native]"
```

A primeira linha mostra o que vai acontecer. A segunda executa a conversão. Depois, pareie de novo na caixa de entrada.

Para instalar pelo Coolify, siga o [guia de deploy do Chatwoot fazer.ai](https://github.com/fazer-ai/chatwoot/blob/main/docker/README-coolify-deploy.md), em português, que cobre os dois modos.

## Limitações conhecidas

- A recusa automática de chamada não silencia quem liga pelo WhatsApp Web. O navegador de quem ligou continua chamando.
- Números em Coexistence, com app WhatsApp Business e API oficial no mesmo número, não recebem chamadas, grupos, listas de transmissão, mensagens temporárias, visualização única nem localização em tempo real. É uma regra da plataforma.
- Mensagens de tipos que esta versão ainda não reconhece, como uma enquete, aparecem na conversa como um aviso de que algo foi enviado, sem o conteúdo.
- Veja os detalhes em [docs/limitations.md](docs/limitations.md). As limitações abertas e os próximos recursos estão nas [issues do repositório](https://github.com/fazer-ai/whatsapp-connector/issues) e em [docs/roadmap.md](docs/roadmap.md).

## Documentação

- [Arquitetura](docs/architecture.md): organização do serviço, posse das sessões e resumo do protocolo.
- [Operação](docs/operations.md): como rodar o conector, todas as variáveis e Redis suportado.
- [Limitações conhecidas](docs/limitations.md): restrições e comportamentos que afetam o uso.
- [Roadmap](docs/roadmap.md): o que já está pronto e o que falta.
- [Protocolo](contract/PROTOCOL.md): o contrato com o cliente, em inglês.
- [Contribuição](CONTRIBUTING.md): desenvolvimento, testes e como mudar o protocolo.

## Desenvolvimento

Use a versão de Go declarada no `go.mod` e golangci-lint v2. Para preparar o ambiente e rodar as verificações que não dependem de servidores:

```bash
make setup
make check-offline
```

O `make check` roda todas as verificações da CI e precisa de PostgreSQL e Redis. Os detalhes estão em [CONTRIBUTING.md](CONTRIBUTING.md).

## Suporte e comunidade

- Dúvidas de instalação e uso: [perguntas e respostas da Comunidade Lucas Moreira](https://www.lucasmoreira.ai/c/perguntas-e-respostas).
- Bugs e pedidos de funcionalidade: [issues no GitHub](https://github.com/fazer-ai/whatsapp-connector/issues).

## Licença

Licença MIT, copyright (c) 2026 FAZER.AI LTDA. Consulte [LICENSE](LICENSE). O conector usa a whatsmeow, sob Mozilla Public License 2.0. Componentes de terceiros mantêm suas respectivas licenças.

## Links

- [Chatwoot fazer.ai](https://github.com/fazer-ai/chatwoot)
- [whatsmeow](https://github.com/tulir/whatsmeow)
- [fazer.ai](https://fazer.ai)

Mantido pela fazer.ai.
