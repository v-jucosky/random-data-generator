# Gerador de dados tabulares aleatórios

Este script recria a estrutura de tabelas em formato `.csv` com dados aleatórios.

Informações dos arquivos gerados:
* Codificação: UTF-8
* Delimitador: `;`
* Quotechar: `"`

## Instalação

Este script foi desenvolvido em Python 3.8. Para instalar as dependências necessárias:

```s
$ pip install -r requirements.txt
```

Certifique-se de utilizar a versão de 64 bits do Python.

## Configuração

Os arquivos `fields.json` e `tables.json` possuem o mapeamento das funções e tabelas, respectivamente. Altere-os para criar as tabelas no formato desejado.

Altere o arquivo `app.py` para refletir as tabelas descritas em `tables.json`; ou importe a classe `Worker` em seu projeto.

Estruturas suportadas:
* Tabela simples
* Chaves primárias simples e compostas
* Chaves estrangeiras simples e compostas
* Estruturas de árvore, com chaves simples ou compostas

## Execução

Para executar:

```s
$ py app.py
```

Ou utilize em seu projeto:

```python
from worker import Worker

estrutura = {
    "tabela": {
        "index_fields": [
            "cpf"
        ],
        "data_fields": [
            "nome"
            "endereco",
            "dt_carga"
        ]
    }
}

w = Worker(estrutura, 'tabela', 1000)
```
