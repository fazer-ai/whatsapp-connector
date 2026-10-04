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
> O canal está em beta e aparece no Chatwoot como “WhatsApp (nativo)”, com selo de beta. Quem administra a instalação libera cada conta, conforme os passos de instalação abaixo. A conexão é não oficial, pareada como um aparelho conectado, igual ao WhatsApp Web. Para a API oficial da Meta, use a caixa WhatsApp Cloud do Chatwoot. Relate problemas nas [issues deste repositório](https://github.com/fazer-ai/whatsapp-connector/issues).

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

- Chatwoot fazer.ai recente. O provedor nativo já vem nas duas edições.
- O mesmo Redis do Chatwoot, na versão 6.2 ou mais nova.
- Um banco para os pareamentos: PostgreSQL próprio do conector para várias instâncias ou SQLite para uma instância só.

### Imagem

Use a imagem pública `ghcr.io/fazer-ai/whatsapp-connector:latest`. Há tags por release, como `0.5.0` e `0.5`. Para fixar a versão, use a tag correspondente.

### Variáveis do conector

Configure, no mínimo, estas variáveis com os valores da sua instalação:

```bash
REDIS_URL=redis://redis:6379
WAC_ENGINE=whatsmeow
WAC_DATABASE_URL=postgres://wac:senha@postgres:5432/wac
WAC_MEDIA_ROOT=/data/media
WAC_MEDIA_TOKEN=um-token-longo-e-aleatorio
```

- `REDIS_URL` usa de propósito o mesmo nome de variável e o mesmo servidor do Chatwoot.
- Sem `WAC_MEDIA_ROOT`, nenhuma mídia recebida chega ao Chatwoot. Quando ela está definida, `WAC_MEDIA_TOKEN` é obrigatório. O Chatwoot lê o token sozinho, pelo registro do conector no Redis.
- `WAC_EVENT_SHARDS`, com padrão 16, precisa ser igual a `WHATSAPP_CONNECTOR_EVENT_SHARDS` do Chatwoot.
- Use armazenamento persistente para `WAC_MEDIA_ROOT` e para o banco. O pareamento sobrevive a reinícios porque fica no banco.
- A tabela completa de variáveis está em [docs/operations.md](docs/operations.md).

### No Chatwoot

```bash
WHATSAPP_CONNECTOR_ENABLED=true
```

Essa variável liga a integração inteira, inclusive o consumidor de eventos que roda no Sidekiq.

### Liberar o canal para uma conta

Durante o beta, o canal aparece só para contas liberadas individualmente. Quem administra a instalação faz a liberação no console Rails do Chatwoot, de propósito fora da tela de administração:

```ruby
Account.find(ID_DA_CONTA).update!(whatsapp_native_enabled: true)
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

Para instalar pelo Coolify, siga o [guia de deploy do Chatwoot fazer.ai](https://github.com/fazer-ai/chatwoot/blob/main/docker/README-coolify-deploy.md), em português. Ele inclui o conector.

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
