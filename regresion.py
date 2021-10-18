import threading
from time import sleep
from typing import Generator
from singleton import Singleton
from start import LOGGER, SHA3_256, get_grpc_uri, DIR
import grpc, solvers_dataset_pb2, api_pb2, gateway_pb2_grpc, regresion_pb2_grpc, gateway_pb2, onnx_pb2, regresion_pb2, os

class Session(metaclass = Singleton):

    def __init__(self, ENVS) -> None:
        self.data_set = solvers_dataset_pb2.DataSet()

        with open(DIR + 'regresion.service', 'rb') as file:
            service_with_meta = gateway_pb2.celaut__pb2.Any()
            service_with_meta.ParseFromString(file.read())

        self.definition = gateway_pb2.celaut__pb2.Service()
        self.definition.ParseFromString(service_with_meta.value)

        self.metadata = service_with_meta.metadata

        self.config = gateway_pb2.celaut__pb2.Configuration()  

        # set used envs on variables.       
        self.GATEWAY_MAIN_DIR = ENVS['GATEWAY_MAIN_DIR']
        self.CONNECTION_ERRORS = ENVS['CONNECTION_ERRORS']
        self.START_AVR_TIMEOUT = ENVS['START_AVR_TIMEOUT']
        self.TIME_FOR_EACH_LOOP = ENVS['TIME_FOR_EACH_REGRESSION_LOOP']
        
        self.gateway_stub = gateway_pb2_grpc.GatewayStub(
            grpc.insecure_channel(self.GATEWAY_MAIN_DIR)
            )
        self.stub = None
        self.token = None
        self.dataset_lock = threading.Lock()
        self.connection_errors = 0
        self.init_service()

        # for maintain.
        self.data_set_hash = ""
        threading.Thread(target = self.maintenance, name = 'Regresion').start()
    
    def service_extended(self):
        config = True
        transport = gateway_pb2.ServiceTransport()
        for hash in self.metadata.hashtag.hash:
            transport.hash.CopyFrom(hash)
            if config:  # Solo hace falta enviar la configuracion en el primer paquete.
                transport.config.CopyFrom(self.config)
                config = False
            yield transport
        transport.ClearField('hash')
        if config: transport.config.CopyFrom(self.config)
        transport.service.service.CopyFrom(self.definition)
        transport.service.meta.CopyFrom(self.metadata)
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
        # Si se acaba de lanzar otra instancia los que quedaron esperando no deberían 
        # marcar el error, pues si hay mas de CONNECTION_ERRORS originaria un bucle al renovar
        # Regresion todo el tiempo.
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

    def maintenance(self):
        while True:
            sleep(self.TIME_FOR_EACH_LOOP)
            # Obtiene una hash del dataset para saber si se han añadido datos.
            actual_hash = SHA3_256(
                value = self.data_set.SerializeToString()
                ).hex()
            LOGGER('Check if dataset was modified ' + actual_hash + self.data_set_hash)
            if actual_hash != self.data_set_hash:
                LOGGER('Perform other regresion.')
                self.data_set_hash = actual_hash
                
                # Se evita crear condiciones de carrera.
                self.dataset_lock.acquire()
                data_set = solvers_dataset_pb2.DataSet()
                data_set.CopyFrom(self.data_set)
                self.dataset_lock.release()

                LOGGER('..........')
                try:
                    open('__tensor__', 'wb').write(
                        self.iterate_regression(
                            data_set = data_set
                        ).SerializeToString()
                    )
                except Exception as e:
                    LOGGER('Exception with regresion service, ' + str(e))

    def get_tensor(self) -> regresion_pb2.Tensor:
        # No hay condiciones de carrera aunque lo reescriba en ese momento.
        if os.path.isfile('__tensor__'):
            tensor = regresion_pb2.Tensor()
            tensor.ParseFromString(
                open('__tensor__', 'rb').read()
            )
            return tensor
        else:
            raise Exception('__tensor__ does not exist.')

    # Add new data
    def add_data(self, new_data_set: solvers_dataset_pb2.DataSet) -> None:
        self.dataset_lock.acquire()
        for hash, solver_data in new_data_set.data.items():
            if hash in self.data_set.data:
                for cnf, data in solver_data.data.items():
                    if cnf in self.data_set.data[hash].data:
                        self.data_set.data[hash].data[cnf].score = sum([
                            (self.data_set.data[hash].data[cnf].index * self.data_set.data[hash].data[cnf].score),
                            data.index * data.score,
                        ]) / (self.data_set.data[hash].data[cnf].index + data.index)
                        self.data_set.data[hash].data[cnf].index = self.data_set.data[hash].data[cnf].index + data.index
                        
                    else:
                        self.data_set.data[hash].data[cnf].CopyFrom(data)
            else:
                self.data_set.data[hash].CopyFrom(solver_data)
        self.dataset_lock.release()
        LOGGER('Dataset updated. ')

    # Hasta que se implemente AddTensor en el clasificador.
    def get_data_set(self) -> solvers_dataset_pb2.DataSet:
        return self.data_set

    # Stream logs Grpc method.
    def stream_logs(self) -> Generator[api_pb2.File, None, None]:
        while True:
            try:
                for file in self.stub.StreamLogs(
                    request = api_pb2.Empty(),
                    timeout = self.START_AVR_TIMEOUT
                ): yield file
                
            except (grpc.RpcError, TimeoutError) as e:
                self.error_control(e)
        
    # Make regresion Grpc method.
    def iterate_regression(self, data_set: solvers_dataset_pb2.DataSet) -> regresion_pb2.Tensor:
        try:
            return self.stub.MakeRegresion(
                request = data_set
                )
        except (grpc.RpcError, TimeoutError) as e:
            self.error_control(e)
            raise Exception