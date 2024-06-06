import shutil
import xml.etree.ElementTree as ET
import pymssql
import logging
from datetime import datetime   
import os
from log import insert_arquivo
logging.basicConfig(filename='logfile.log', level=logging.INFO)



def processar_arquivo_xml(nome_arquivo):
    namespaces = {
        'nfe': 'http://www.portalfiscal.inf.br/nfe',
    }
    teste = {
        'cte': 'http://www.portalfiscal.inf.br/cte'
    }
    try:
        tree = ET.parse(nome_arquivo)
        root = tree.getroot()

        sla = []
        L_Cfops      = ['5201','6201','5411','6411','5202','6202','5410','6410'] #CFOPs saida devolução
        L_Cfop       = [] #valor extraido do xml
        L_Cnpj       = [] #valor extraido do xml
        L_Ref        = [] #valor extraido do xml
        L_Dados      = ['Peso Bruto','CNPJ', 'Valor Total', 'Nota Referência', 'Email', 'ChaveNFe', 'Area', 'NomeV'   ] #key para dicionário
        dicionario   = {chave: None for chave in L_Dados}
        
        Peso     = root.findall(".//nfe:pesoB"             , namespaces)
        Valor    = root.findall(".//nfe:vNF"               , namespaces)
        CNPJ     = root.findall(".//nfe:emit/nfe:CNPJ"     , namespaces)
        Nref     = root.findall(".//nfe:refNFe"            , namespaces)
        FinNFe   = root.findall(".//nfe:finNFe"            , namespaces)
        Cfop     = root.findall(".//nfe:CFOP"              , namespaces)
        ChaveNFe = root.findall(".//nfe:chNFe"             , namespaces)
        ChaveCte = root.findall(".//cte:chCTe"             , teste)
        dt_hr = datetime.now()
        dt_hr = dt_hr.strftime("%Y-%m-%d %H:%M:%S")
        
        for val in ChaveNFe:
            chave_insert = (val.text)
        for val in Cfop:
            L_Cfop.append(val.text)       
        # Valida se é NFe
        if ChaveNFe:
            # POSSUI CFOP?   
            if L_Cfop:
                # O CFOP É DE DEVOLUÇÃO? 
                if L_Cfop[0] in L_Cfops:
                        for val in FinNFe:
                            if val.text == '4':
                                for val in Peso:
                                    dicionario['Peso Bruto'] = (val.text)

                                for val in Valor:
                                    dicionario['Valor Total'] = (val.text)
                                    
                                for val in CNPJ:
                                    if val.text != "": #CNPJ PRÓPRIO
                                        L_Cnpj.append(val.text)

                                for val in Nref:
                                    L_Ref.append(val.text)
                                    
                                for val in ChaveNFe:
                                    dicionario['ChaveNFe'] = (val.text)
                                    
                                dicionario['CNPJ'] = (L_Cnpj)
                                dicionario['Nota Referência'] = ', '.join(L_Ref)
                                    
                                Dados = ['Peso Bruto', 'CNPJ', 'Valor Total', 'Nota Referência']

                                for chave in Dados:
                                    if chave not in dicionario and dicionario[chave] is None:
                                        dicionario[chave] = "Não Informado"

                                dicionario['CNPJ'] = ', '.join(L_Cnpj)
                                dicionario['Email'] = ', '.join(L_Cnpj)
                                dicionario['Nota Referência'] = ', '.join(L_Ref)

                                #Conexão Banco
                                conn = pymssql.connect(server='', user='', password='', database='')
                                #Email
                                query = f"SELECT RTRIM(a1_email) FROM sa1010 WHERE a1_cgc = '{dicionario['CNPJ']}'"
                                cursor = conn.cursor()  
                                cursor.execute(query)
                                email_cliente = cursor.fetchall()
                                if not email_cliente:
                                    error_mail = (f"{dt_hr} - Arquivo: Nfe.py - Erro: Nenhum E-mail encontrato para o cliente de CNPJ {dicionario['CNPJ']} ")
                                    logging.info(error_mail)
                                    return None
                                else:
                                    #Nome Cliente
                                    query = f"SELECT RTRIM(a1_nome) FROM sa1010 WHERE a1_cgc = '{dicionario['CNPJ']}'"
                                    cursor = conn.cursor()  
                                    cursor.execute(query)
                                    nome = cursor.fetchall()
                                    if nome:
                                        dicionario['Nome'] = ', '.join(nome[0])
                                    else:
                                        return None
                                    #Cod Vendedor
                                    query = f"SELECT RTRIM(a1_vend) FROM sa1010 WHERE a1_cgc = '{dicionario['CNPJ']}'"
                                    cursor = conn.cursor()  
                                    cursor.execute(query)
                                    cod = cursor.fetchall()
                                    if cod:
                                        dicionario['Area'] = ', '.join(cod[0])
                                    else:
                                        return None
                                    #Cod Superior
                                    query = f"SELECT RTRIM(zf_codsupe) FROM szf010 WHERE zf_codvend = '{dicionario['Area']}'"
                                    cursor = conn.cursor()  
                                    cursor.execute(query)
                                    codsup = cursor.fetchall()
                                    if codsup:
                                        dicionario['CodSup'] = ', '.join(codsup[0])
                                        print(dicionario['CodSup'])
                                    else:
                                        return None
                                    #E-mail Superior
                                    query = f"SELECT RTRIM(ze_email) FROM sze010 WHERE ze_codigo = '{dicionario['CodSup']}' AND ze_email <> ' '"
                                    cursor = conn.cursor()  
                                    cursor.execute(query)
                                    emailsup = cursor.fetchall()
                                    if emailsup:
                                        dicionario['EmailS'] = ', '.join(emailsup[0])
                                    else:
                                        return None
                                    #Nome Vendedor
                                    query = f"SELECT RTRIM(a3_nome) FROM sa3010 WHERE a3_cod = '{dicionario['Area']}'"
                                    cursor = conn.cursor()  
                                    cursor.execute(query)
                                    nomev = cursor.fetchall()
                                    if nomev:
                                        dicionario['NomeV'] = ', '.join(nomev[0])
                                    else:
                                        return None
                                    #E-mail Vendedor
                                    query = f"SELECT RTRIM(a3_email) FROM sa3010 WHERE a3_cod = '{dicionario['Area']}'"
                                    cursor = conn.cursor()  
                                    cursor.execute(query)
                                    emailv = cursor.fetchall()
                                    if nomev:
                                        dicionario['Emailv'] = ', '.join(emailv[0])
                                    else:
                                        return None                            
                                conn.close()
                                insert_arquivo(dt=dt_hr,
                                            tp="NFE",
                                            evento="Arquivo Criado",
                                            caminho=nome_arquivo,
                                            chave=chave_insert,
                                            devolucao="SIM",
                                    )
                                return dicionario
                # Se tiver CFOP mas n for de devolução
                else:
                    insert_arquivo(dt=dt_hr,
                                tp="NFE",
                                evento="Arquivo Criado",
                                caminho=nome_arquivo,
                                chave=chave_insert,
                                devolucao="NAO")  
                    return None
            # Se n tiver cfop dar insert como cancelamento
            else:
                insert_arquivo(dt=dt_hr,
                            tp="Cancelamento",
                            evento="Arquivo Criado",
                            caminho=nome_arquivo,
                            chave=chave_insert,
                            devolucao="NAO",
                    )
                return None

        # Se não for NFe roda insert como CTe 
        else:
            for val in ChaveCte:
                ChaveCte = (val.text)
            insert_arquivo(dt=dt_hr,
                        tp="CTE",
                        evento="Arquivo Criado",
                        caminho=nome_arquivo,
                        chave=ChaveCte,
                        devolucao="NAO")  
            return None

    except ET.ParseError:
            xml_erro = (f"{dt_hr} - Arquivo: Nfe.py - Erro ao abrir o arquivo: {nome_arquivo}")
            logging.error(xml_erro)
            print(f"{xml_erro}")
            return None


