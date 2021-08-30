import threading
from time import sleep
from typing import Iterator
from singleton import Singleton
from start import LOGGER, get_grpc_uri, DIR
import grpc, solvers_dataset_pb2, api_pb2, gateway_pb2_grpc, regresion_pb2_grpc, gateway_pb2

class Session(metaclass=Singleton):

    def __init__(self, ENVS) -> None:
        with open(DIR + 'regresion.service', 'rb') as file:
            self.definition = gateway_pb2.hyweb__pb2.Service()
            self.definition.ParseFromString(file.read())
        self.config = gateway_pb2.hyweb__pb2.Configuration()

        self.GATEWAY_MAIN_DIR = ENVS['GATEWAY_MAIN_DIR']
        self.CONNECTION_ERRORS = ENVS['CONNECTION_ERRORS']
        self.START_AVR_TIMEOUT = ENVS['START_AVR_TIMEOUT']
        
        self.gateway_stub = gateway_pb2_grpc.GatewayStub(
            grpc.insecure_channel(self.GATEWAY_MAIN_DIR)
            )
        self.stub = None
        self.token = None
        self.lock = threading.Lock() #TODO
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

    def stop(self):
        LOGGER('Stopping regresion service instance.')
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

    def error_control(self, e):
        self.lock.acquire()
        if self.connection_errors < self.CONNECTION_ERRORS:
            self.connection_errors = self.connection_errors + 1
            sleep(1)  # Evita condiciones de carrera si lo ejecuta tras recibir la instancia. TODO
        else:
            self.connection_errors = 0
            self.signal.
            LOGGER('Errors occurs on regresion method --> ' + str(e))
            LOGGER('Vamos a cambiar el servicio de regresion')
            self.stop()
            self.init_service()
            LOGGER('listo. ahora vamos a probar otra vez.')
        self.lock.release()  


    # --> Grpc methods <--
    # Add new data
    def add_data(self, new_data_set: solvers_dataset_pb2.DataSet) -> None:
        while True:
            try:
                LOGGER('Send data to regresion.')
                self.stub.AddDataSet(
                    request=new_data_set,
                    timeout=self.START_AVR_TIMEOUT
                )
            except (grpc.RpcError, TimeoutError) as e:
                self.error_control(e)

    # Return the tensor from the grpc stream method.
    def get_tensor(self) -> Iterator[api_pb2.onnx__pb2.ONNX, None, None]:
        while True:
            try:
                LOGGER('Get tensor from regresion.')
                for t in self.stub.GetTensor(
                    request=api_pb2.Empty(),
                    timeout=self.START_AVR_TIMEOUT
                ): yield t
            except (grpc.RpcError, TimeoutError) as e:
                self.error_control(e)

    # Hasta que se implemente AddTensor en el clasificador.
    def get_data_set(self) -> solvers_dataset_pb2.DataSet:
        while True:
            try:
                LOGGER('Get dataset from regresion.')
                return self.stub.GetDataSet(
                    request=api_pb2.Empty(),
                    timeout=self.START_AVR_TIMEOUT
                )
            except (grpc.RpcError, TimeoutError) as e:
                self.error_control(e)

    def stream_logs(self) -> Iterator[api_pb2.File, None, None]:
        while True:
            try:
                LOGGER('Streaming logs from regresion.')
                for file in self.stub.StreamLogs(
                    request=api_pb2.Empty(),
                    timeout=self.START_AVR_TIMEOUT
                ): yield file
            except (grpc.RpcError, TimeoutError) as e:
                self.error_control(e)
