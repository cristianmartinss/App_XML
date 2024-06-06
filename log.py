import mysql.connector

def conectar_banco():
    config = {
        'host': '',
        'user': '',
        'password': '',
        'database': '',
        'port': '',
    }
    conexao = mysql.connector.connect(**config)
    return conexao
    
def insert_arquivo(dt,tp,evento,caminho,chave,devolucao):
    caminho_certo = caminho.replace("\\", "\\\\")
    conexao = conectar_banco()
    cursor = conexao.cursor()
    sql = f"INSERT INTO log (DATA_REG,TIPO_DCTO,EVENTO,CAMINHO,CHAVE,DEVOLUCAO) VALUES('{dt}','{tp}','{evento}','{caminho_certo}','{chave}','{devolucao}')"
    # print(sql)
    cursor.execute(sql)
    conexao.commit()
    cursor.close()
    conexao.close()

def update_email(att,path):
    conexao = conectar_banco()
    cursor = conexao.cursor()
    sql = "UPDATE log SET email = %s WHERE caminho = %s"
    cursor.execute(sql, (att, path))
    # print(sql) 
    conexao.commit()
    cursor.close()
    conexao.close()
