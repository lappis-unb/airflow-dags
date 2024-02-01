# Como contribuir com o AIRFLOW-DAGS?

Estamos muito felizes por você estar lendo isso, seja bem vindo ao time de serviços de dados.
Segue algumas informações necessárias para iniciar com sua contribuição!

## Politica de Branchs
A nomeclatura de branches que segue o padrão "tipo-da-mudança/descrição" é uma abordagem estruturada e informativa para gerenciar branches em repositórios de controle de versão, especialmente no contexto de commits convencionais. Essa convenção de nomenclatura utiliza tokens derivados dos tipos de mudanças definidos no padrão de commits convencionais para categorizar as branches de forma clara e compreensível.
A estrutura "tipo-da-mudança/nome-descritivo" pode ser desdobrada da seguinte maneira:


Tipo da Mudança (Token): O tipo da mudança é um token que descreve a natureza da alteração que está sendo realizada na branch. Esses tokens são extraídos dos tipos descritos no Conventional Commits, como "feat" para novos recursos, "fix" para correções de bugs, "chore" para tarefas de manutenção, "docs" para documentação, entre outros. Isso ajuda a categorizar a natureza da branch.


descrição: É uma breve descrição que esclarece o propósito da branch de forma clara e concisa. Deve explicar o que está sendo desenvolvido na branch, para que outras pessoas da equipe possam entender facilmente a sua finalidade.


Por exemplo, se você estiver criando uma branch para adicionar um novo recurso de pesquisa em um projeto de software, a nomeclatura pode seguir o formato "feat/search-feature" ou "feat/implement-search." Isso torna evidente que a branch se destina à implementação de um novo recurso de pesquisa.

## Politica de Commits
<details>
<summary>Prefixo</summary>

## feat:

Utilizado para novas funcionalidades ou adições ao código.


## fix:

Usado para correções de bugs.


## chore:

Geralmente associado a tarefas de manutenção, ajustes de configuração ou outras atividades não relacionadas a funcionalidades ou bugs.

## docs:

Reservado para alterações na documentação.

## refactor:

Utilizado quando há refatoração de código sem alterar comportamento externo.

## test:

Associado a adições ou modificações nos testes.

## build:

Relacionado a mudanças no sistema de build ou dependências.

## ci:

Envolvendo ajustes ou melhorias em configurações de integração contínua.

## perf:

Usado para melhorias de desempenho.

## revert:

Utilizado para reverter uma alteração anterior.

</details>

--- 

<details>
<summary>Sufixo</summary>

<br>

No sufixo, deve estar em parentese aonde que está sendo feita a mudança, seguido de dois pontos e sua mensagem.

**Exemplo**: `docs(dag-notify-proposals): documentação inicial da dag de envio de mensagem de novas propostas`.

</details>

[< voltar](padroes-do-projeto)

## Politica de Merge Request
<details>
<summary>Description</summary>

<br>

![image](uploads/584baff990dec8c9b2e66f6e29dc38ad/image.png)

## Template

```md
### Descrição
Este pedido de merge aborda e descreve o problema ou história do usuário que está sendo tratado.

### Alterações Realizadas
Forneça trechos de código ou capturas de tela conforme necessário.

### Problemas Relacionados
Forneça links para os problemas ou solicitações de funcionalidades relacionados.

### Notas Adicionais
Inclua qualquer informação extra ou considerações para os revisores, como áreas impactadas no código-fonte.

### Listas de Verificação do Pedido de Merge
- [ ] O código segue as diretrizes de codificação do projeto.
- [ ] A documentação reflete as alterações realizadas.
- [ ] Já cobri os testes unitários.

### Issue referenciada

Closes <link-da-issue>
```

</details>

---

<details>
<summary>Tags</summary>

<br>

![image](uploads/877964133f49473f83916df5eab2267a/image.png)

## Assignees

Indique os responsáveis pelo desenvolvimento do PR/MR.

## Reviewers

Se houver algum supervisor acompanhando o desenvolvimento da Issue referente ao PR/MR, marque-o. Caso contrário, deixe em branco.

## Milestone

Não se aplica a menos que seja solicitado.

## Labels

Utilize as labels que se aplicam ao problema, sendo obrigatório o uso das labels de MR, demarcadas por `MR::XXX...XXX`, e o uso da label `REVIEW::Needed`.

### Feature

Usado quando o PR/MR inclue alguma nova funcionalidade ao produto.

### Bug

Usado quando o PR/MR resolve algum bug.

### Hotfix

Usado quando o PR/MR inclui alguma modificacao relativamente curta e rapida de revisar.

### Refactor

Usado quando o PR/MR inclui refatoracao de codigo.

### Testing

Usado quando o PR/MR inclui testes para o software.

### Enhancement

Usado quando o PR/MR inclui melhorias ou modificacoes que nao se enquadram em uma nova funcionalidade.

### Documentation

Usado quando o PR/MR inclui modificacoes em documentacao.

</details>

[< voltar](padroes-do-projeto)

## Padrões de qualidade do código e testes

