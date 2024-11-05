# Importacoes
import psycopg2
import logging
from os import getenv
from dotenv import load_dotenv
from pymongo import MongoClient

# Configuracao do logging
logging.basicConfig(
    filename='rpa_log.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

load_dotenv()

client = MongoClient(getenv('URI_MONGODB'))
db = client[getenv('MONGO_DBNAME')]
collection = db[getenv('MONGO_COLLECTION')]

# Funcao para conectar ao banco de dados
def conectar_banco(uri):
    try:
        conexao = psycopg2.connect(uri)
        cursor = conexao.cursor()
        logging.info("Conexao com o banco de dados estabelecida com sucesso.")
        return conexao, cursor
    except Exception as e:
        logging.error(f"Erro ao conectar ao banco de dados: {e}")
        return None, None
    

# Funcao principal
def main():

    db1_uri = getenv('URI_BANCO_1')
    db2_uri = getenv('URI_BANCO_2')

    connection_db1, cursor_db1 = conectar_banco(db1_uri)
    connection_db2, cursor_db2 = conectar_banco(db2_uri)

    if cursor_db1 and cursor_db2:
        try:
            logging.info("Sincronizacao completa com sucesso.")
        except Exception as error:
            logging.error(f"Erro ao realizar a sincronizacao: {error}")
        finally:
            cursor_db1.close()
            connection_db1.close()
            cursor_db2.close()
            connection_db2.close()
            logging.info("Conexoes com os bancos de dados fechadas.")
    else:
        logging.error("Falha na conexao com os bancos de dados.")

if __name__ == "__main__":
    main()