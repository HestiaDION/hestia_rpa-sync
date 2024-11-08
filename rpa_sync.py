# Importacoes
import psycopg2
import logging
from os import getenv
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta
from random import randint
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
    
# Funcao para obter a senha do usuário a partir do e-mail
def get_senha(email):
    try:
        user = collection.find_one({'email': email})
        return user['senha']
    except Exception as e:
        logging.error(f"Erro ao obter a senha para o email {email}: {e}")
        return ""
    
# Funcao para obter a foto de perfil do usuário a partir do e-mail
def get_foto_perfil(email):
    try:
        user = collection.find_one({'email': email})
        return user['urlFoto']
    except Exception as e:
        logging.error(f"Erro ao obter a foto de perfil para o email {email}: {e}")
        return ""

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

# Funcao para sincronizar a tabela universitario
def sync_universitario(cursor_db1, cursor_db2, connection_db1, connection_db2):
    try:
        # Captura os dados da tabela do banco 1
        cursor_db1.execute("SELECT * FROM Universitario;")
        universitario_records_db1 = cursor_db1.fetchall()
        logging.info("Dados de Universitário obtidos do Banco 1 para sincronizacao.")

        # Captura os dados da tabela do banco 2
        cursor_db2.execute("SELECT * FROM universitario;")
        universitario_records_db2 = cursor_db2.fetchall()
        logging.info("Dados de Universitário obtidos do Banco 2 para comparacao.")

        ids_db1 = [x[0] for x in universitario_records_db1]

        for universitario in universitario_records_db2:
            (uid, email, nome, dne, dt_nascimento, genero, prefixo, telefone, municipio, universidade,
             plano_id, bio, tipo_conta, created_at, updated_at) = universitario
            
            if uid not in ids_db1:
                senha = get_senha(email)
                foto_perfil = get_foto_perfil(email)
                plano = '1' if plano_id else '0'
                
                username = f'{nome}{randint(10000, 99999)}'

                cursor_db1.execute("""
                    INSERT INTO Universitario 
                    (uId, cDne, cNome, cUsername, cEmail, cSenha, dDtNascimento, cGenero,
                     cMunicipio, cPrefixo, cTel, cPlano, cFotoPerfil, cDescricao, cNmFaculdade)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, (uid, dne, nome, username, email, senha, dt_nascimento, genero, municipio, prefixo, telefone,
                      plano, foto_perfil, bio, universidade))
                logging.info(f"Novo registro de universitario com UUID {uid} inserido no Banco 1.")

        for universitario in universitario_records_db1:
            (uId, cDne, cNome, cUsername, cEmail, cSenha, dDtNascimento, cGenero, cMunicipio,
             cPrefixo, cTel, cPlano, cFotoPerfil, cDescricao, uId_Anuncio, cNmFaculdade) = universitario

            plano = None if cPlano == '0' else cursor_db2.execute(
                "SELECT plano_id FROM universitario WHERE id = %s", (uId,)
            ).fetchone()[0]

            cursor_db2.execute("""
                UPDATE universitario SET
                email = %s, nome = %s, dne = %s, dt_nascimento = %s, genero = %s, prefixo = %s,
                telefone = %s, municipio = %s, universidade = %s, plano_id = %s, bio = %s
                WHERE id = %s;
            """, (cEmail, cNome, cDne, dDtNascimento, cGenero, cPrefixo, cTel, cMunicipio, cNmFaculdade,
                  plano, cDescricao, uId))
            logging.info(f"Registro de universitario com UUID {uId} atualizado no Banco 2.")

        connection_db1.commit()
        connection_db2.commit()
        logging.info("Sincronizacao de universitário finalizada.")

    except Exception as e:
        connection_db1.rollback()
        connection_db2.rollback()
        logging.error(f"Erro ao sincronizar tabela Universitario: {e}")

# Funcao para sincronizar a tabela anunciante
def sync_anunciante(cursor_db1, cursor_db2, connection_db1, connection_db2):
    try:
        cursor_db1.execute("SELECT * FROM Anunciante;")
        anunciante_records_db1 = cursor_db1.fetchall()
        logging.info("Dados de Anunciante obtidos do Banco 1 para sincronizacao.")

        cursor_db2.execute("SELECT * FROM anunciante;")
        anunciante_records_db2 = cursor_db2.fetchall()
        logging.info("Dados de Anunciante obtidos do Banco 2 para comparacao.")

        ids_db1 = [x[0] for x in anunciante_records_db1]

        for anunciante in anunciante_records_db2:
            (uid, email, nome, dt_nascimento, genero, municipio, prefixo, telefone,
             bio, tipo_conta, plano_id, created_at, updated_at) = anunciante

            if uid not in ids_db1:
                senha = get_senha(email)
                foto_perfil = get_foto_perfil(email)
                plano = '1' if plano_id else '0'

                username = f'{nome}{randint(10000, 99999)}'

                cursor_db1.execute("""
                    INSERT INTO Anunciante 
                    (uId, cNome, cUsername, cEmail, cSenha, dDtNascimento, cGenero,
                     cMunicipio, cPrefixo, cTel, cPlano, cFotoPerfil, cDescricao)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, (uid, nome, username, email, senha, dt_nascimento, genero, municipio, prefixo, telefone,
                      plano, foto_perfil, bio))
                logging.info(f"Novo registro de anunciante com UUID {uid} inserido no Banco 1.")

        for anunciante in anunciante_records_db1:
            (uId, cNome, cUsername, cEmail, cSenha, dDtNascimento, cGenero, cMunicipio,
             cPrefixo, cTel, cPlano, cDescricao, cFotoPerfil) = anunciante

            plano = None if cPlano == '0' else cursor_db2.execute(
                "SELECT plano_id FROM anunciante WHERE id = %s", (uId,)
            ).fetchone()[0]

            cursor_db2.execute("""
                UPDATE anunciante SET
                email = %s, nome = %s, dt_nascimento = %s, genero = %s, prefixo = %s,
                telefone = %s, municipio = %s, plano_id = %s, bio = %s
                WHERE id = %s;
            """, (cEmail, cNome, dDtNascimento, cGenero, cPrefixo, cTel, cMunicipio,
                  plano, cDescricao, uId))
            logging.info(f"Registro de anunciante com UUID {uId} atualizado no Banco 2.")

        connection_db1.commit()
        connection_db2.commit()
        logging.info("Sincronizacao de anunciante finalizada.")

    except Exception as e:
        connection_db1.rollback()
        connection_db2.rollback()
        logging.error(f"Erro ao sincronizar tabela Anunciante: {e}")

# Funcao para sincronizar a tabela pagamento
def sync_pagamento(cursor_db1, cursor_db2, connection_db1, connection_db2):
    try:
        cursor_db1.execute("SELECT * FROM Pagamento;")
        pagamento_records_db1 = cursor_db1.fetchall()
        logging.info("Dados de Pagamento obtidos do Banco 1 para sincronizacao.")

        cursor_db2.execute("SELECT * FROM pagamento_plano;")
        pagamento_records_db2 = cursor_db2.fetchall()
        logging.info("Dados de Pagamento obtidos do Banco 2 para comparacao.")

        ids_db1 = [x[0] for x in pagamento_records_db1]

        for pagamento in pagamento_records_db2:
            (uid, nome, email, plano_id, pago, created_at, updated_at) = pagamento
            if uid not in ids_db1:
                ativo = '0'
                cursor_db2.execute("SELECT * FROM get_user_uuid_by_email(%s);", (email,))
                info_user = cursor_db2.fetchone()
                tipo_user, uid_user = info_user

                dt_fim = datetime.now() + relativedelta(months=1)
                cursor_db2.execute("SELECT valor FROM plano WHERE id = %s;", (plano_id,))
                total = cursor_db2.fetchone()[0]

                if tipo_user == 'anunciante':
                    cursor_db1.execute("""
                        INSERT INTO Pagamento 
                        (uId, cAtivo, dDtFim, nPctDesconto, nTotal, uId_Anunciante, uId_Plano, uId_Universitario)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                    """, (uid, ativo, dt_fim, 0, total, uid_user, plano_id, None))
                else:
                    cursor_db1.execute("""
                        INSERT INTO Pagamento 
                        (uId, cAtivo, dDtFim, nPctDesconto, nTotal, uId_Anunciante, uId_Plano, uId_Universitario)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                    """, (uid, ativo, dt_fim, 0, total, None, plano_id, uid_user))

                logging.info(f"Novo registro de pagamento com UUID {uid} inserido no Banco 1.")

        for pagamento in pagamento_records_db1:
            (uid, cAtivo, dDtFim, nPctDesconto, nTotal, uId_Anunciante, uId_Plano, uId_Universitario) = pagamento
            pago = cAtivo == '1'

            cursor_db2.execute("""
                UPDATE pagamento_plano SET
                pago = %s
                WHERE id = %s;
            """, (pago, uid))
            logging.info(f"Registro de pagamento com UUID {uid} atualizado no Banco 2.")

        connection_db1.commit()
        connection_db2.commit()
        logging.info("Sincronizacao de pagamento finalizada.")

    except Exception as e:
        connection_db1.rollback()
        connection_db2.rollback()
        logging.error(f"Erro ao sincronizar tabela Pagamento: {e}")

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
            sync_universitario(cursor_db1, cursor_db2, connection_db1, connection_db2)
            sync_anunciante(cursor_db1, cursor_db2, connection_db1, connection_db2)
            sync_pagamento(cursor_db1, cursor_db2, connection_db1, connection_db2)

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