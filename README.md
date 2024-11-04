
# Sincronização de Bancos de Dados com Python

## Descrição do Projeto

Este projeto consiste em um script em Python (`rpa-sync.py`) para sincronizar dados entre dois bancos de dados PostgreSQ.

## Estrutura do Projeto

### 1. Configuração e Conexão

- **Bibliotecas Utilizadas**:
  - `psycopg2`: Conexão e manipulação dos bancos de dados PostgreSQL.
  - `requests`: Requisições HTTP a serviços externos.
  - `dotenv`: Carrega variáveis de ambiente a partir de um arquivo `.env`.
  - `datetime` e `dateutil.relativedelta`: Manipulação de datas.

- **Conexão com Bancos de Dados**:
  - **Ambientes**:
    - **Banco de Dados 1**: Utilizado pelo 1º ano EM.
    - **Banco de Dados 2**: Utilizado pelo 2º ano EM.
  - **Hospedagem**: Ambos os bancos estão hospedados na Aiven.
  - Função `conectar_banco(uri)`: Estabelece a conexão com o banco de dados utilizando a URI fornecida e retorna a conexão e o cursor para operações subsequentes.

### 2. Integração com Serviços Externos

- **API Externa Local**: API em Flask (`app.py`) desenvolvida especificamente para este projeto.
  - **Endpoints**:
    - `http://127.0.0.1:5000/get-password`: Obtém a senha do usuário com base no email.
    - `http://127.0.0.1:5000/get-photo`: Obtém a URL da foto de perfil do usuário com base no email.
  - **Funções no Script**:
    - `get_senha(email)`: Requisição POST para obter a senha.
    - `get_foto_perfil(email)`: Requisição POST para obter a foto de perfil.

### 3. Sincronização de Tabelas

As tabelas sincronizadas incluem `plano`, `plano_vantagens`, `universitario`, `anunciante` e `pagamento`. O processo de sincronização realiza:

- **Atualizações**: Verifica se o registro existe no banco de destino e atualiza os campos necessários.
- **Inserções**: Caso o registro não exista no banco de destino, realiza a inserção com os dados correspondentes.

Cada tabela tem uma função de sincronização específica, como `sync_plano` e `sync_universitario`. As operações são confirmadas com `commit` após cada operação bem-sucedida, enquanto `rollback` é usado para manter a integridade dos dados em caso de erros.

### 4. Execução Principal

A função `main()` executa o script de sincronização em etapas:

1. **Carregamento das Variáveis de Ambiente**: Utiliza `load_dotenv()` para carregar as variáveis do arquivo `.env`.
2. **Conexão com os Bancos**: Conecta-se aos dois bancos de dados usando as URIs.
3. **Sincronização**: Chama as funções de sincronização de cada tabela na ordem necessária.
4. **Mensagens de Status**: Exibe mensagens de sucesso ou erro durante a execução.
5. **Fechamento das Conexões**: Fecha as conexões com os bancos de dados ao final do processo.

### 5. Agendamento da Execução

- **Hospedagem**: O script será hospedado na AWS.
- **Frequência**: A execução será agendada a cada 10 minutos usando `cron`, garantindo a sincronização periódica e automatizada.

## Arquivos do Projeto

- **rpa-sync.py**: Script principal para sincronização de bancos de dados.
- **app.py**: API Flask para integração com serviços externos.

## Executando o Projeto

1. **Pré-requisitos**: Instalar as bibliotecas necessárias:
   ```bash
   pip install -r requirements.txt
   ```
2. **Configurar Variáveis de Ambiente**: Criar um arquivo `.env` com as URIs dos bancos de dados:
   ```
   URI_BANCO_1="sua_uri_banco_1"
   URI_BANCO_2="sua_uri_banco_2"
   ```

3. **Iniciar a API Flask**:
   ```bash
   python app.py
   ```

4. **Executar o Script de Sincronização**:
   ```bash
   python rpa-sync.py
   ```