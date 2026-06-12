# Synaflow: Filosofia de Design e Decisões Arquiteturais

Este documento registra os princípios fundamentais e as decisões de design arquitetural do Synaflow. Ele serve como o guia definitivo para a evolução do framework, garantindo que novas funcionalidades respeitem a visão original.

## 1. Princípios Fundamentais

### 1.1. Make Simple Things Easy, Complex Things Possible
A curva de aprendizado do framework deve ser amigável. Configurações padrão devem ser intuitivas e resolver 90% dos casos de uso de forma transparente (ex: usar `list` ou `set` como materializadores nativos). No entanto, o framework deve expor protocolos e interfaces (como *Factories* ricas em contexto) para permitir engenharia avançada (ex: persistência em disco particionada por tipo de dado).

### 1.2. Convention Over Configuration
O código do usuário deve ser focado em regras de negócio, não em conectar fios.
- O DAG descobre as dependências lendo os tipos das assinaturas (Type Hints).
- Opções globais (ex: Materializadores, Timeouts) são configuradas uma única vez na raiz do `pipeline` e propagadas por convenção, em vez de exigir que o usuário reconfigure cada nó. Exceções a regras (overrides) são explícitas por nó.

### 1.3. Lazy by Default (Stream Processing)
O framework assume processamento em *Stream* (Lazy Evaluation) como padrão sempre que possível, para proteger a memória (RAM) e otimizar tempo de CPU.
- O tratamento de erros padrão é `OnError.CONTINUE`, permitindo que um item falho seja descartado sem interromper a esteira contínua.

## 2. Decisões Arquiteturais e Padrões (Log de Decisões)

### 2.1. Injeção de Parâmetros Transparente
**Decisão:** Parâmetros (`params`) definidos como `NamedTuple` são disponibilizados globalmente e de forma transparente para qualquer step da cadeia, não apenas para o primeiro nó do pipeline.
**Motivo:** Reduzir "boilerplate" de repassar parâmetros pelo fluxo. O executor (`_resolve_node_arguments`) faz um merge das chaves da `NamedTuple` com as saídas dos nós upstream, fazendo com que steps intermediários possam assinar requisições a esses parâmetros diretamente.

### 2.2. A Regra do `OnError.STOP` e Materialização Forçada
**Decisão:** Quando um nó é configurado com `OnError.STOP`, o framework é forçado a quebrar o paradigma *Lazy* para esse passo, materializando inteiramente os dados produzidos antes de liberar a execução dos nós consumidores.
**Motivo:** Integridade transacional do pipeline. Se o processamento parar no meio por um erro e a propagação for lazy, o nó downstream receberá lixo ou uma fração da coleção, corrompendo o fluxo lógico do sistema e dificultando o controle de concorrência e limpeza.

### 2.3. Separação de Protocolo: Materializadores vs. Fábricas de Materialização
**Decisão:** A responsabilidade de persistência e buffer das coleções foi separada em duas camadas semânticas:
- **Materializer (Protocolo de Execução):** Um mero `Callable[[Iterator], Iterable]`. Funções nativas da linguagem como `list`, `set` e `dict` se encaixam nativamente aqui.
- **Materializer Factory (Protocolo de Configuração):** Um `Callable[[MaterializeContext], Materializer]`. É a ponte entre a inteligência do DAG e o executor, recebendo um Contexto rico (nome do dataset, step, pipeline, type hints) e retornando o `Materializer` configurado.
**Motivo:** Seguir o princípio *Simple Things Easy* (usuários podem dar `materializer=list` no step override) mantendo *Complex Things Possible* (usuários definem uma Fábrica com nomeação de arquivos auto-descoberta pelo `Context` no construtor raiz do `pipeline`).

---
*(Este documento deve ser evoluído iterativamente sempre que um novo contrato arquitetural for estabelecido na base de código do Synaflow).*
