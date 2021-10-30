import logging
import mimesis
import pandas
import json


from multiprocessing import Process, log_to_stderr
from mimesis.builtins import BrazilSpecProvider
from pathlib import Path


MAX_INDEX_RETRIES = 10
TREE_ROOT_PROPORTION = 0.3


class Worker(Process):
    def __init__(self, structure: dict, table: str, limit: int, chunk_size: int=10000):
        '''
        Gerador de dados tabulares aleatórios.

        Para reduzir o consumo de memória, os campos de dados são gerados em blocos.
        Chaves primárias são geradas de uma vez, para garantir unicidade dos registros.
        Chaves externas são carregadas na íntegra, para reduzir acesso ao disco.

        Parâmetros
        ----------
        structure: estrutura das tabelas
        table: nome da tabela a ser gerada
        limit: tamanho do dataset a ser gerado, em linhas
        chunk_size: tamanho dos blocos de dados a serem gerados

        Constantes
        ----------
        MAX_INDEX_RETRIES: limite de iterações na criação das chaves primárias (previne
            a execução do código caso não sejam gerados registros únicos suficientes
            para satisfazer o índice no tamanho indicado)
        TREE_ROOT_PROPORTION: proporção de registros raíz ao criar estruturas de árvore
            (onde a chave estrangeira aponta para ua chave primária na mesma tabela)
        '''

        super().__init__()

        self._provider = mimesis.Generic('pt-br')
        self._logger = log_to_stderr()

        self._provider.add_provider(BrazilSpecProvider)
        self._logger.setLevel(logging.INFO)

        self._file = Path('./' + table + '.csv')
        self._limit = limit
        self._chunk_size = chunk_size
        self._remaining = limit

        self._generators = {}

        try:
            self._index_fields = structure[table]['index_fields']
        except KeyError:
            self._index_fields = []

        try:
            foreign_table = structure[table]['foreign_table']

            if foreign_table == table:
                self._foreign_file = None
            else:
                self._foreign_file = Path('./' + foreign_table + '.csv')
        except KeyError:
            self._foreign_fields = []
        else:
            self._foreign_fields = structure[foreign_table]['index_fields']

            if not self._foreign_fields:
                raise ValueError('Foreign table does not have primary keys')

        try:
            self._data_fields = structure[table]['data_fields']
        except KeyError:
            self._data_fields = []

        self._index_dataframe = pandas.DataFrame(columns=self._index_fields)
        self._foreign_dataframe = pandas.DataFrame(columns=self._foreign_fields)
        self._data_dataframe = pandas.DataFrame(columns=self._data_fields)

        with open('./fields.json', mode='r', encoding='UTF-8') as json_file:
            fields_configuration = json.load(json_file)

            for field in set(self._index_fields + self._foreign_fields + self._data_fields):
                m_class, m_function = fields_configuration[field]['type'].split('.')

                generator = getattr(
                    getattr(self._provider, m_class),
                    m_function
                )

                try:
                    parameters = fields_configuration[field]['params']
                except KeyError:
                    parameters = ()

                self._generators[field] = (generator, parameters)

    def run(self):
        '''Rotina de execução principal, chamada ao iniciar o processo.'''

        if self._index_fields:
            self._logger.info('building primary keys')

            index_retries = 0

            while len(self._index_dataframe.index) < self._limit:
                self._index_dataframe = pandas.concat([
                    self._index_dataframe,
                    self._generate(self._index_fields, self._limit - len(self._index_dataframe.index))], ignore_index=True
                )

                self._index_dataframe = self._index_dataframe.drop_duplicates().reset_index(drop=True)

                index_retries += 1

                if index_retries == MAX_INDEX_RETRIES:
                    self._limit = self._remaining = len(self._index_dataframe.index)

                    self._logger.warning(f'dataset will be limit to {self._limit} records due to primary key collision')

                    break

        if self._foreign_fields:
            self._logger.info('loading foreign keys')

            if self._foreign_file:
                self._foreign_dataframe = pandas.read_csv(self._foreign_file, ';', header=0, usecols=self._foreign_fields, encoding='UTF-8')
            else:
                self._foreign_dataframe = self._index_dataframe.sample(frac=1 - TREE_ROOT_PROPORTION).reset_index(drop=True)

                self._foreign_dataframe = pandas.concat([
                    self._foreign_dataframe,
                    pandas.DataFrame([None for _ in range(len(self._index_dataframe.index) - len(self._foreign_dataframe.index))], columns=self._foreign_fields)], ignore_index=True
                )

        self._logger.info('generating data')

        while self._remaining > 0:
            subset = self._generate(self._data_fields, self._chunk_size)

            for column in self._data_dataframe.columns:
                self._data_dataframe = pandas.concat([
                    self._data_dataframe,
                    subset], ignore_index=True
                )

                if len(self._data_dataframe.index) < self._remaining:
                    subset[column] = subset[column].sample(frac=1).reset_index(drop=True)
                else:
                    self._data_dataframe = self._data_dataframe.head(self._remaining)

                    break

            self._save_chunk()

        self._logger.info(f'{self._file.name} completed')

    def _generate(self, fields: list, size: int):
        '''
        Gera um dataframe a partir dos campos fornecidos.

        Parâmetros
        ----------
        fields: lista de colunas a gerar
        size: tamanho do dataframe
        '''

        return pandas.DataFrame([{field: self._generators[field][0](*self._generators[field][1]) for field in fields} for _ in range(size)])

    def _save_chunk(self):
        '''Une os índices, chaves externas e dados gerados e os salva no arquivo de destino.'''

        chunk_size = len(self._data_dataframe.index)

        if self._foreign_fields:
            chunk = pandas.concat([
                self._index_dataframe.head(chunk_size),
                self._foreign_dataframe.sample(chunk_size, replace=True).reset_index(drop=True),
                self._data_dataframe], axis=1
            )
        else:
            chunk = pandas.concat([
                self._index_dataframe.head(chunk_size),
                self._data_dataframe], axis=1
            )

        if self._file.exists():
            chunk.to_csv(self._file, ';', header=False, index=False, mode='a', encoding='UTF-8')
        else:
            chunk.to_csv(self._file, ';', index=False, encoding='UTF-8')

        self._index_dataframe.drop(self._index_dataframe.index[:chunk_size], inplace=True)
        self._index_dataframe.reset_index(drop=True, inplace=True)

        self._data_dataframe.drop(self._data_dataframe.index[0:0], inplace=True)
        self._data_dataframe.reset_index(drop=True, inplace=True)

        self._remaining -= chunk_size
