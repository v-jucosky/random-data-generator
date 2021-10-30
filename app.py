import json


from app.worker import Worker


if __name__ == '__main__':
    with open('./tables.json', mode='r', encoding='UTF-8') as json_file:
        tables = json.load(json_file)

    # Cria 100.000 registros "empresa" e aguarda a conclusão:
    companies = Worker(tables, 'empresas', 10**5)
    companies.start()
    companies.join()

    # Cria 1.000.000 de registros "funcionario", com chave estrangeira para "empresa":
    workers = Worker(tables, 'funcionarios', 10**6)
    workers.start()

    # Cria 10.000.000 de registros "cliente", com chave estrangeira para "empresa":
    clients = Worker(tables, 'clientes', 10**7)
    clients.start()

    # Cria 1.000.000 de registros contendo uma estrutura hierárquica do tipo "hierarquia":
    hierarchies = Worker(tables, 'hierarquias', 10**6)
    hierarchies.start()

    # Aguarda a conclusão dos processos "funcionario", "cliente" e "hierarquia":
    workers.join()
    clients.join()
    hierarchies.join()
