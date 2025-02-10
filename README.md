# WalletMap

Projeto para escanear a blockchain do Bitcoin e consolidar saldos de endereços. O scanner utiliza chamadas RPC para obter dados dos blocos e processa transações para atualizar saldos, armazenando as informações em um banco de dados (SQLite ou PostgreSQL).

## Componentes Principais

- **Blockchain Scanner**  
  Implementado como [`BitcoinScanner`](bitcoin_scanner/scanner.py) em [bitcoin_scanner/scanner.py](bitcoin_scanner/scanner.py), este módulo processa blocos, gerencia transações e atualiza o status dos blocos processados.

- **Cliente RPC**  
  A comunicação com o nó Bitcoin é feita por meio do [`RPCClient`](bitcoin_scanner/rpc_client.py) em [bitcoin_scanner/rpc_client.py](bitcoin_scanner/rpc_client.py), que gerencia as chamadas JSON-RPC com tratamento de erros e retentativas.

- **Gerenciamento de Banco de Dados**  
  O [`DatabaseManager`](bitcoin_scanner/database.py) em [bitcoin_scanner/database.py](bitcoin_scanner/database.py) é responsável por conexões e operações CRUD no banco de dados, incluindo tabelas para saldos, utxos, metadados e blocos processados.

- **Derivação de Endereços**  
  A função [`derive_address`](bitcoin_scanner/address.py) em [bitcoin_scanner/address.py](bitcoin_scanner/address.py) realiza a derivação dos endereços Bitcoin a partir de scripts, suportando diversos formatos (P2PKH, P2SH, P2WPKH, P2WSH e P2TR).

- **Interface de Linha de Comando (CLI)**  
  O módulo [`cli.py`](bitcoin_scanner/cli.py) em [bitcoin_scanner/cli.py](bitcoin_scanner/cli.py) configura a execução e registra logs, iniciando a varredura dos blocos.

## Requisitos

- Python 3.10 ou superior
- Bibliotecas:
  - requests
  - base58
  - bech32
  - psycopg2 (para suporte PostgreSQL)

## Configuração

1. Configure as variáveis de ambiente ou passe os argumentos via linha de comando para definir:

   - `--network` (ex.: mainnet ou testnet4)
   - `--rpc-user` e `--rpc-password`
   - `--rpc-url` (opcional)

2. Para PostgreSQL, verifique a configuração em [bitcoin_scanner/database.py](bitcoin_scanner/database.py).

## Uso

Para iniciar o scanner via CLI, execute:

```sh
python main.py

```

Esta chamada utiliza o main.py, que importa e executa a função principal definida em cli.py.

## Estrutura do Projeto

main\_\_.pyx / main.py – Pontos de entrada para diferentes implementações.
bitcoin_scanner – Contém a lógica principal:
scanner.py: Lógica da varredura e processamento de blocos.
rpc_client.py: Comunicação RPC.
database.py: Gerenciamento do banco de dados.
address.py: Função de derivação de endereços.
cli.py: Interface de linha de comando.
Logs e Monitoramento
Os logs são gravados no arquivo bitcoin_scanner.log e também exibidos no console. O scanner registra o progresso detalhado, incluindo o número de blocos processados, falhas e o último bloco confirmado.

## Contribuição

Contribuições são bem-vindas. Sinta-se à vontade para abrir issues ou enviar pull requests para melhorias e correção de bugs.

## Licença
