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

# Funcao para sincronizar a tabela plano
def sync_plano(cursor_db1, cursor_db2, connection_db2):
    try:
        cursor_db1.execute("SELECT uId, cNome, cTipoUsuario, nValor, cDescricao FROM Plano;")
        plano_records_db1 = cursor_db1.fetchall()
        logging.info("Dados de plano obtidos do Banco 1 para sincronizacao.")

        for plano_record in plano_records_db1:
            plano_id, plano_nome, plano_tipo_usuario, plano_valor, plano_descricao = plano_record

            cursor_db2.execute("SELECT id FROM plano WHERE id = %s;", (plano_id,))
            plano_record_db2 = cursor_db2.fetchone()

            if plano_record_db2:
                cursor_db2.execute("""
                    UPDATE plano
                    SET nome = %s, tipo_conta = %s, descricao = %s, valor = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s;
                """, (plano_nome, plano_tipo_usuario, plano_descricao, plano_valor, plano_id))
                logging.info(f"Registro do plano com UUID {plano_id} atualizado no Banco 2.")
            else:
                cursor_db2.execute("""
                    INSERT INTO plano (id, nome, tipo_conta, descricao, valor, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
                """, (plano_id, plano_nome, plano_tipo_usuario, plano_descricao, plano_valor))
                logging.info(f"Novo registro do plano com UUID {plano_id} inserido no Banco 2.")

        connection_db2.commit()
        logging.info('Sincronizacao de plano finalizada.')

    except Exception as e:
        connection_db2.rollback()
        logging.error(f"Erro ao sincronizar tabela Plano: {e}")

# Faz a sincronizacao da tabela plano_vantagens
def sync_plano_vantagens(cursor_db1, cursor_db2, connection_db2):
    try:
        cursor_db1.execute("SELECT uId, cVantagem, cAtivo, uId_Plano FROM Plano_vantagem;")
        vantagem_records_db1 = cursor_db1.fetchall()
        logging.info("Dados de vantagens de plano obtidos do Banco 1 para sincronizacao.")

        for vantagem_record in vantagem_records_db1:
            vantagem_id, vantagem_nome, vantagem_ativo, plano_id_db1 = vantagem_record
            vantagem_ativo_bool = True if vantagem_ativo == '1' else False

            cursor_db2.execute("SELECT id FROM plano_vantagens WHERE id = %s;", (vantagem_id,))
            vantagem_record_db2 = cursor_db2.fetchone()

            if vantagem_record_db2:
                cursor_db2.execute("""
                    UPDATE plano_vantagens
                    SET vantagem = %s, ativo = %s, plano_id = %s
                    WHERE id = %s;
                """, (vantagem_nome, vantagem_ativo_bool, plano_id_db1, vantagem_id))
                logging.info(f"Registro da vantagem com UUID {vantagem_id} atualizado no Banco 2.")
            else:
                cursor_db2.execute("""
                    INSERT INTO plano_vantagens (id, vantagem, ativo, plano_id)
                    VALUES (%s, %s, %s, %s);
                """, (vantagem_id, vantagem_nome, vantagem_ativo_bool, plano_id_db1))
                logging.info(f"Novo registro da vantagem com UUID {vantagem_id} inserido no Banco 2.")

        connection_db2.commit()
        logging.info('Sincronizacao de plano_vantagens finalizada.')

    except Exception as e:
        connection_db2.rollback()
        logging.error(f"Erro ao sincronizar tabela Plano_vantagens: {e}")

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
            sync_plano(cursor_db1, cursor_db2, connection_db2)
            sync_plano_vantagens(cursor_db1, cursor_db2, connection_db2)

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