<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Índice**

- [Como contribuir?](#como-contribuir)
    - [Git workflow](#git-workflow)
    - [Politica de Commits](#politica-de-commits)
    - [Revisão e qualidade do código](#revis%C3%A3o-e-qualidade-do-c%C3%B3digo)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Como contribuir?

Antes de iniciar sua contribuição, recomendamos que revise nosso [Onboarding](https://gitlab.com/groups/lappis-unb/decidimbr/servicos-de-dados/-/wikis/Onboarding).

Esse guia apresenta normas e informações para auxiliar no fluxo de desenvolvimento e manutenção de DAGs.

## Git workflow

O modelo que vamos seguir se baseia em duas branches principais, a "main" e "development", além das ramificações secundárias para features ou correções de bugs. Essa estrutura torna mais claro o ciclo de vida do desenvolvimento, desde a criação de novas funcionalidades até a implantação de versões estáveis.

### Diagrama do git flow

![02_Feature_branches.svg](docs/img/git_flow.svg)

#### Fluxo de Desenvolvimento

Primeiramente, **cada nova mudança deve ser desenvolvida em uma branch exclusiva**, o que facilita o gerenciamento de mudanças e a colaboração da equipe.

Uma vez que a mudança está pronta para ser incorporada ao projeto, um "Merge Request" (MR) deve ser aberto para a branch "development". Isso permite que a equipe revise a alteração, compartilhe feedback e assegure a aderência aos padrões de qualidade e ao objetivo do projeto.

Após a aprovação e testes bem-sucedidos em um ambiente de teste ou "stage", a alteração está pronta para ser mesclada na branch "development". Somente após ter sido testada e aprovada nesse ambiente que um commit de "development" para "main" deve ser realizado. Essa abordagem ajuda a manter a branch "main" como uma representação estável e confiável do código do projeto, garantindo que apenas as alterações devidamente validadas sejam incorporadas à versão principal.

#### Nomeclatura de branches

A nomeclatura de branches que segue o padrão **"tipo-da-mudança/descrição"** é uma abordagem estruturada e informativa para gerenciar branches em repositórios de controle de versão, especialmente no contexto de commits convencionais. Essa convenção de nomenclatura utiliza tokens derivados dos tipos de mudanças definidos no padrão de commits convencionais para categorizar as branches de forma clara e compreensível.

A estrutura **"tipo-da-mudança/nome-descritivo"** pode ser desdobrada da seguinte maneira:

1. **Tipo da Mudança (Token)**: O tipo da mudança é um token que descreve a natureza da alteração que está sendo realizada na branch. Esses tokens são extraídos dos tipos descritos no Conventional Commits, e estão descritos na seção `Politica de Commits.`

2. **descrição**: É uma breve descrição que esclarece o propósito da branch de forma clara e concisa. Deve explicar o que está sendo desenvolvido na branch, para que outras pessoas da equipe possam entender facilmente a sua finalidade.

Por exemplo, se você estiver criando uma branch para adicionar um novo recurso de pesquisa em um projeto de software, a nomeclatura pode seguir o formato "feat/relatorio-mobilizacao" ou "feat/busca." Isso torna evidente que a branch se destina à implementação de um novo recurso de pesquisa.

## Politica de Commits

A nossa política de commits adota o padrão [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) como diretriz fundamental para o registro de alterações no nosso código. Este padrão incentiva a clareza e consistência nas mensagens de commit, facilitando a compreensão e rastreamento das mudanças ao longo do tempo.

Padão de commit

### **prefixo(sufixo): descrição**

**Exemplo**:

```markdown
docs(dag-notificacao-proposta): documentação inicial da dag de envio de mensagem de novas propostas.
```

### Prefixo

- **feat**: Utilizado para novas funcionalidades ou adições ao código. Feat de features.
- **fix**: Usado para correções de bugs.
- **chore**: Geralmente associado a tarefas de manutenção, ajustes de configuração ou outras atividades não relacionadas a funcionalidades ou bugs.
- **docs**: Reservado para alterações na documentação.
- **refactor**: Utilizado quando há refatoração de código sem alterar comportamento externo.
- **test**: Associado a adições ou modificações nos testes.
- **build**: Relacionado a mudanças no sistema de build ou dependências.
- **ci**: Envolvendo ajustes ou melhorias em configurações de integração contínua.
- **perf**: Usado para melhorias de desempenho.
- **revert**: Utilizado para reverter uma alteração anterior.

### Sufixo

No sufixo, deve estar em parentese aonde que está sendo feita a mudança, seguido de dois pontos e sua mensagem.

## Revisão e qualidade do código

Para manter a qualidade do código, adotamos uma série de medidas automatizadas e de padrão de código.

### Regras de desenvolvimento

1. Criar pipelines mais simples possivel.
    - ***Justificativa:*** Simplificar o fluxo de controle aprimora a clareza do código e facilita a análise. Evitar recursividade resulta em um grafo de chamadas de função acíclico, permitindo limites no uso da pilha e execuções limitadas.

        - *Exemplo Ruim:*

        ```python
        if condicao:
            tarefa_1 >> tarefa_2
        else:
            tarefa_1 >> tarefa_3
        ```

        - *Bom Exemplo:*
          - Utilizando o [operador branch do airflow](https://docs.astronomer.io/learn/airflow-branch-operator) podemos implementar o mesmo fluxo.

        ```python
        tarefa_1 >> [tarefa_2, tarefa_3]
        ```

2. Definir um limite para os loops
   - ***Justificativa:*** Garantir limites previsíveis nos loops, sejam limites superiores ou inferiores. Isso simplifica a análise estática e contribui para uma pipeline mais robusta.

      - *Exemplo Ruim:*

        ```python
        for i in range(len(data)):
            # lógica de processamento
        ```

      - *Bom Exemplo:*

        ```python
        for arquivo_de_dados in arquivos_de_dados[:100]:  # Limite superior fixo
            # lógica de processamento
        ```

3. Não inicializar objetos vazios.
   - ***Justificativa:*** Alocação dinâmica de memória pode levar a comportamentos imprevisíveis. Evitar essa prática promove uma gestão mais previsível e eficiente dos recursos.
     - *Exemplo Ruim:*

       ```python
       resultado = []
       for arquivo_de_dados in arquivos_de_dados:
           resultado.append(processa(arquivo_de_dados))
       ```

     - *Bom Exemplo:*

       ```python
       resultado = [processa(arquivo_de_dados) for arquivo_de_dados in arquivo_de_dados]
       ```

       - [Porque ***list comprehension*** é mais rapida que um for normal com append](https://stackoverflow.com/a/30245465/11281346)

4. Nenhuma função deve ser maior que 60 linhas.
   - ***Justificativa:*** No contexto do Airflow, funções mais curtas e focadas são cruciais para manter a clareza das tarefas. Isso facilita a manutenção, testes e compreensão das DAGs.

     - *Exemplo Ruim:*

       ```python
       @task
       def complex_data_processing(data):
           # 100 linhas de lógica
       ```

     - *Bom Exemplo:*

       ```python
       @task
       def processa_dado(dado):
           dado_limpo = limpeza(data)
           dados_transformados = transformacao(dado_limpo)
           dado_carregado = load(dados_transformados)
           return dado_carregado
       ```

5. Cada função deve ter pelo menos dois ***asserts***.
   - ***Justificativa:*** Asserts são uma ferramenta poderosa para validar precondições e pós-condições de tarefas. Aumentar a densidade de assertivas ajuda na detecção precoce de problemas durante a execução de DAGs.

     - *Exemplo Ruim:*

       ```python
       def processa_dado(dado):
           # lógica de processamento sem assertivas
       ```

     - *Bom Exemplo:*

       ```python
       def processa_dado(dado):
           assert dado is not None, "Dado nao pode ser None"
           assert isinstance(dado, pd.DataFrame), "Dado deve ser do tipo DataFrame"
           # lógica de processamento com assertivas significativas
       ```

6. Declarar todos os objetos de dados no menor escopo possível.
   - ***Justificativa:*** Declarar objetos de dados localmente em cada tarefa favorece a modularidade e evita compartilhamento global, o que pode levar a efeitos colaterais inesperados.
     - *Exemplo Ruim:*

       ```python
       data = []

       def process_data():
           # lógica de processamento usando dados globais
           pass
       ```

     - *Bom Exemplo:*

       ```python
       def process_data(dados):
           local_data = [limpeza(dado) for dado in dados]
           # lógica de processamento usando local_data
       ```

7. Cada função, com o retorno não-none, chamada deve ter verificado o valor de retorno.
   - ***Justificativa:*** Verificar os retornos de tarefas é crucial para identificar falhas e realizar ações adequadas em caso de erros. Ignorar valores de retorno pode resultar em execuções não detectadas.
     - *Exemplo Ruim:*

       ```python
       result = process_data(data)
       # Não há verificação para o valor de retorno
       ```

     - *Bom Exemplo:*

       ```python
       result = process_data(data)
       if result is None:
           raise ValueError("Error in processing data")
       ```

8. Todo código deve ter seu arquivo de testes e deve passar na análise estática do Mypy.
    - ***Justificativa:*** Incorporar testes unitários e análise estática com o MyPy é crucial para assegurar a robustez do código. O MyPy, ao realizar verificações estáticas de tipo, ajuda a identificar potenciais bugs antes que o código entre em produção, aumentando a confiança na qualidade do software.

### Pre-commit

Para ajudar o desenvolvedor com os gargalos que podem ser gerados durante os testes de integração contínua, a ferramenta [pre-commit](https://pre-commit.com/) analisa o código que será commitado e realiza algumas alterações na formatação desse código, seguindo padrões de boas práticas python.

Para instalar as dependências utilize o comando:

```bash
pip install -r requirements.txt
```

Para inicializar o pre-commit:

```bash
pre-commit install
```

Agora tudo que for commitado irá passar pelo pre-commit, e as alterações necessárias serão feitas pela ferramenta. Após as alterações é necessário adicionar e commitar novamente.
