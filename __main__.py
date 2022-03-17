import argparse
from asyncio.log import logger
import datetime
import logging
import sys
import time

from ajuste_buffer_step import AjusteBufferStep
from apaga_buffer_temp_step import ApagaBufferTempStep
from auto_dependencias_step import AutoDependenciasStep
from criacao_buffer_step import CriacaoBufferStep
from exclusao_step import ExclusaoStep
from melhorias_modelagem_step import MelhoriasModelagemStep
from permissoes_nasajon_step import PermissoesNasajonStep
from popula_pks_step import PopularPKsStep
from selecao_dados_step import SelecaoDadosStep
from selecao_dados_incremental_step import SelecaoDadosIncrementalStep

from database_config import create_pool
from db_adapter2 import DBAdapter2

STEPS = {
    'exclusao': ExclusaoStep,
    'criacao_buffer': CriacaoBufferStep,
    'auto_dependencias': AutoDependenciasStep,
    'selecao_dados': SelecaoDadosStep,
    'selecao_dados_incremental': SelecaoDadosIncrementalStep,
    'ajuste_buffer': AjusteBufferStep,
    'melhorias_modelagem': MelhoriasModelagemStep,
    'apaga_buffer_temp': ApagaBufferTempStep,
    'permissoes_nasajon': PermissoesNasajonStep,
    'popula_pks': PopularPKsStep
}

LISTA_STEPS = ['processo_basico', 'melhorias_modelagem', 'auto_dependencias', 'criacao_buffer',
               'selecao_dados', 'selecao_dados_incremental', 'exclusao', 'ajuste_buffer',
               'apaga_buffer_temp', 'permissoes_nasajon', 'popula_pks']

STEPS_PROCESSO_BASICO = ['melhorias_modelagem', 'criacao_buffer',
                         'selecao_dados', 'ajuste_buffer', 'exclusao',
                         'apaga_buffer_temp']


def config_logger():
    # Configuring logger
    data = datetime.datetime.now()
    log_file_name = f"exclusao - {data.year}-{data.month}-{data.day}-{data.hour}-{data.minute}.log"

    logger = logging.getLogger('exclusao_empresas')
    logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler(sys.stdout)
    file_handler = logging.FileHandler(log_file_name)

    console_format = logging.Formatter(
        '%(name)s - %(levelname)s - %(message)s')
    file_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    file_handler.setFormatter(file_format)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


def config_logger_fks():
    # Configuring logger
    data = datetime.datetime.now()
    log_file_name = f"fks - {data.year}-{data.month}-{data.day}-{data.hour}-{data.minute}.log"

    logger = logging.getLogger('log_fks')
    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(log_file_name)

    file_format = logging.Formatter('%(message)s')
    file_handler.setFormatter(file_format)

    logger.addHandler(file_handler)


def internal_main(
    database_name: str,
    database_user: str,
    database_pass: str,
    database_host: str,
    database_port: str,
    step_id: str,
    empresas: str
):
    config_logger()
    config_logger_fks()

    logger = logging.getLogger('exclusao_empresas')
    logger.info('Abrindo conexão com o banco de dados...')

    start_time = time.time()
    try:
        # Criando o pool de conexoes
        pool = create_pool(
            database_user,
            database_pass,
            database_host,
            database_port,
            database_name,
            1
        )

        # Abrindo conexao com o BD depois
        with pool.connect() as conn:
            # Instanciando o DBAdapter
            db_adapter = DBAdapter2(conn)

            # Resolvendo os passos a executar
            steps = [step_id]
            if not(step_id in LISTA_STEPS):
                logger.warning(
                    f"Parâmetro step inválido {step_id}. Use: {LISTA_STEPS}")
                sys.exit(4)
            if step_id == 'processo_basico':
                steps = STEPS_PROCESSO_BASICO

            # Executando cada step
            for id in steps:
                step_obj = STEPS[id](db_adapter)
                step_obj.main(empresas)

    finally:
        logger.info("--- TEMPO TOTAL GERAL %s seconds ---" %
                    (time.time() - start_time))


def main():
    try:
        # Initialize parser
        parser = argparse.ArgumentParser(
            description="""
Utilitário para exclusão de empresas de um banco de dados.
            """)

        # Adding arguments
        parser.add_argument(
            "-d", "--database", help="Nome do banco de dados para conexão", required=True)
        parser.add_argument(
            "-u", "--user", help="Usuário para conexão com o banco de dados", required=False, default='postgres')
        parser.add_argument(
            "-p", "--password", help="Senha para conexão com o banco de dados", required=False, default='postgres')
        parser.add_argument(
            "-t", "--host", help="IP ou nome do servidor do banco de dados", required=False, default='localhost')
        parser.add_argument(
            "-o", "--port", help="Porta para conexão com o banco de dados", required=False, default='5432')

        parser.add_argument(
            "-e", "--empresas", help="Lista dos códigos das empresas a excluir (separados por vírgulas)", required=False, default='')

        parser.add_argument(
            "-s",
            "--step",
            help=f"Etapa do processo de exclusão a ser executada. Válidos: {LISTA_STEPS}.",
            required=False,
            default='processo_basico'
        )

        # Read arguments from command line
        args = parser.parse_args()

        internal_main(
            args.database,
            args.user,
            args.password,
            args.host,
            args.port,
            args.step,
            args.empresas
        )
        sys.exit(0)
    except Exception as e:
        logger.exception(
            f'Erro fatal não identificado. Mensagem original do erro {e}')
        sys.exit(5)


if __name__ == '__main__':
    main()
