import threading
from time import sleep
from typing import Generator
from singleton import Singleton
from start import LOGGER, get_grpc_uri, DIR
import grpc, solvers_dataset_pb2, api_pb2, gateway_pb2_grpc, regresion_pb2_grpc, gateway_pb2

class Session(metaclass=Singleton):

    def __init__(self, ENVS) -> None:
        """
        with open(DIR + 'regresion.service', 'rb') as file:
            self.definition = gateway_pb2.hyweb__pb2.Service()
            self.definition.ParseFromString(file.read())
        self.config = gateway_pb2.hyweb__pb2.Configuration()        
        """

        self.GATEWAY_MAIN_DIR = ENVS['GATEWAY_MAIN_DIR']
        self.CONNECTION_ERRORS = ENVS['CONNECTION_ERRORS']
        self.START_AVR_TIMEOUT = ENVS['START_AVR_TIMEOUT']
        
        self.gateway_stub = gateway_pb2_grpc.GatewayStub(
            grpc.insecure_channel(self.GATEWAY_MAIN_DIR)
            )
        self.stub = None
        self.token = None
        self.lock = threading.Lock()
        self.semaphore = threading.Semaphore(ENVS['MAX_REGRESION_WORKERS']-1) # One worker for stream logs.
        self.connection_errors = 0
        self.init_service()

    def service_extended(self):
        config = True
        transport = gateway_pb2.ServiceTransport()
        for hash in self.definition.hashtag.hash:
            transport.hash.CopyFrom(hash)
            if config:  # Solo hace falta enviar la configuracion en el primer paquete.
                transport.config.CopyFrom(self.config)
                config = False
            yield transport
        transport.ClearField('hash')
        if config: transport.config.CopyFrom(self.config)
        transport.service.CopyFrom(self.definition)
        yield transport

    def init_service(self):
        LOGGER('Launching regresion service instance.')
        """
        while True:
            try:
                instance = self.gateway_stub.StartService(
                    self.service_extended()
                    )
                break
            except grpc.RpcError as e:
                LOGGER('GRPC ERROR.' + str(e))
                sleep(1)
        uri = get_grpc_uri(instance.instance)
        self.stub = regresion_pb2_grpc.RegresionStub(
            grpc.insecure_channel(
                uri.ip + ':' + str(uri.port)
            )
        )
        self.token = instance.token        
        """
        self.stub = regresion_pb2_grpc.RegresionStub(
            grpc.insecure_channel(
                'localhost:9999'
            )
        )

        try:
            with open('dataset.bin', 'rb') as f:
                self.add_data(
                    new_data_set = f.read().ParseFromString()
                )
        except: pass # Si no tenemos dataset no pasa nada.

    def stop(self):
        LOGGER('Stopping regresion service instance.')
        """
        while True:
            try:
                self.gateway_stub.StopService(
                    gateway_pb2.TokenMessage(
                        token = self.token
                    )
                )
                break
            except grpc.RpcError as e:
                LOGGER('Grpc Error stopping regresion ' + str(e))
                sleep(1)        
        """


    def error_control(self, e):
        # Si se acaba de lanzar otra instancia los que quedaron esperando no deberían 
        # marcar el error, pues si hay mas de CONNECTION_ERRORS originaria un bucle al renovar
        # Regresion todo el tiempo.
        self.lock.acquire()
        if self.connection_errors < self.CONNECTION_ERRORS:
            self.connection_errors = self.connection_errors + 1
            sleep(1)  # Evita condiciones de carrera si lo ejecuta tras recibir la instancia.
        else:
            self.connection_errors = 0
            LOGGER('Errors occurs on regresion method --> ' + str(e))
            LOGGER('Vamos a cambiar el servicio de regresion')
            self.stop()
            self.init_service()
            LOGGER('listo. ahora vamos a probar otra vez.')
        self.lock.release()  

    def maintenance(self):
        # Guarda el data-set en el Clasificador cada cierto tiempo.
        LOGGER('Save dataset on memory.')
        with open('dataset.bin', 'wb') as f:
            f.write(
                self.get_data_set().SerializeToString()
            )

    # --> Grpc methods <--
    # Add new data
    def add_data(self, new_data_set: solvers_dataset_pb2.DataSet) -> None:
        self.semaphore.acquire()
        while True:
            try:
                LOGGER('Send data to regresion.')
                self.stub.AddDataSet(
                    request=new_data_set,
                    timeout=self.START_AVR_TIMEOUT
                )
                break
            except (grpc.RpcError, TimeoutError) as e:
                self.error_control(e)
        self.semaphore.release()

    # Return the tensor from the grpc stream method.
    def get_tensor(self) -> tuple: # return ONNX.
        self.semaphore.acquire()
        itents = 0
        while itents < (self.CONNECTION_ERRORS - self.connection_errors):
            try:
                LOGGER('Get tensor from regresion.')
                return self.stub.GetTensor(
                    request=api_pb2.Empty(),
                    timeout=self.START_AVR_TIMEOUT
                ), lambda: self.semaphore.release()
            except (TimeoutError) as e:
                self.error_control(e)
                itents += 1
                # Si retorna se han  realizado demasiados intentos salta una excepción, 
                # de lo contrario esperaría a una nueva instancia del Regresion, subirle 
                # el dataset y que este dijera que no tiene el tensor.
            except:
                # Si retorna None salta una excepción al serializar.
                break
        self.semaphore.release()
        raise Exception


    def get_data_set(self) -> tuple: # return DataSet
        self.semaphore.acquire()
        while True:
            try:
                LOGGER('Get dataset from regresion.')
                return self.stub.GetDataSet(
                    request=api_pb2.Empty(),
                    timeout=self.START_AVR_TIMEOUT
                ), lambda: self.semaphore.release()
            except (grpc.RpcError, TimeoutError) as e:
                self.error_control(e)

    def stream_logs(self) -> Generator[api_pb2.File, None, None]:
        while True:
            try:
                for file in self.stub.StreamLogs(
                    request=api_pb2.Empty(),
                    timeout=self.START_AVR_TIMEOUT
                ): yield file
                
            except (grpc.RpcError, TimeoutError) as e:
                self.error_control(e)
        
        
