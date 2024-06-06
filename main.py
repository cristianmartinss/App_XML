import os
import threading
from time import sleep
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from nfe import processar_arquivo_xml
from time import sleep
from Email import send_email
from Email import get_acess_token
from datetime import datetime 
import logging
import shutil

logging.basicConfig(filename='logfile.log', level=logging.INFO)

dt_hr = datetime.now()
dt_hr = dt_hr.strftime("%Y-%m-%d %H:%M:%S")

class WatchdogHandler(FileSystemEventHandler):

    def __init__(self, nfe_processor):
        self.nfe_processor = nfe_processor
        self.timeout = 10
        self.name = None
        self.arquivos_processados = []

    def on_created(self, event):
        if event.is_directory:
            return
        sleep(5)
        if event.src_path in self.arquivos_processados:
            return
        print(f"Arquivo Criado: {event.src_path}")
        destino = '\\\\' #Caminho pasta Protheus
        self.arquivos_processados.append(event.src_path)
        self.name = event.src_path
        processing_thread = threading.Thread(target=self.process_file, args=(event.src_path,))
        file_name, file_extension = os.path.splitext(self.name)
        if file_extension == '.xml':
             shutil.copy(self.name, destino)
            
        processing_thread.start()
        
    def process_file(self, file_path):
        resultado_processamento = self.nfe_processor(file_path)

        if resultado_processamento:
            nome_arquivo = os.path.basename(self.name)
            emailVendedor = (resultado_processamento["Emailv"])
            emailSuperior = (resultado_processamento["EmailS"])
            attachment_path = (self.name)
            send_email(subject = (f"Devolução {resultado_processamento['Nome']} {resultado_processamento['ChaveNFe']} "),
                       body = (
                           f'Nota de Devolução:\n'
                           f'Número: {resultado_processamento["ChaveNFe"]}\n'
                           f'Valor: {resultado_processamento["Valor Total"]}\n'
                           f'Nota (s) de Referência: {resultado_processamento["Nota Referência"]}\n'
                           f'Peso: {resultado_processamento["Peso Bruto"]}\n'
                           f'Área: {resultado_processamento["Area"]} Vendedor: {resultado_processamento["NomeV"]}'
                           ),
                       to_email = (emailVendedor),
                       superior = (emailSuperior),
                       attachment_path = attachment_path,
                       access_token = get_acess_token(),
                       name_file=(nome_arquivo)
                     )

class FolderWatchDog:
    def __init__(self, handler, watch_path):
        self.handler = handler
        self.watch_path = watch_path
        self.observer = Observer()

    def start(self):
        self.observer.schedule(self.handler,
                           path=self.watch_path,
                           recursive=True)
        try:
            print(f"Observando:{self.watch_path}")
            logging.info(f'{dt_hr} - Serviço iniciado')
            self.observer.start()
            while True:
                sleep(5)

        except:
            self.stop()
    
    def stop(self):
        self.observer.join()
        self.observer.stop()

if __name__== "__main__":
    watch_path =    (f"C:\\watch") 
    handler1 = WatchdogHandler(processar_arquivo_xml)
    watchdog1 = FolderWatchDog(handler1, watch_path1)
    thread1 = threading.Thread(target=watchdog1.start)

    try:
        thread1.start()
        thread1.join()

    except Exception as e:
        print(f"Erro: {e}")
        exception_error = (f"{dt_hr} - Main.py - Exceção: {e}")
        logging.error(exception_error)
        pass

    finally:
        pass
        print("Serviço Finalizado")
