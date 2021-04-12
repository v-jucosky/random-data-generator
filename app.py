import json


from app.worker import Worker


if __name__ == '__main__':
    with open('./tables.json', mode='r') as json_file:
        tables = json.load(json_file)

    # Cria 100.000 registros "empresa" e aguarda a conclusão:
    empresas = Worker(tables, 'empresas', 10**5)
    empresas.start()
    empresas.join()

    # Cria 1.000.000 de registros "funcionario", com chave estrangeira para "empresa":
    funcionarios = Worker(tables, 'funcionarios', 10**6)
    funcionarios.start()

    # Cria 10.000.000 de registros "cliente", com chave estrangeira para "empresa":
    clientes = Worker(tables, 'clientes', 10**7)
    clientes.start()

    # Cria 1.000.000 de registros contendo uma estrutura hierárquica do tipo "hierarquia":
    hierarquias = Worker(tables, 'hierarquias', 10**6)
    hierarquias.start()

    # Aguarda a conclusão dos processos "funcionario", "cliente" e "hierarquia":
    funcionarios.join()
    clientes.join()
    hierarquias.join()
