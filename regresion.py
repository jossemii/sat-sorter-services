from asyncio import shield
from doctest import FAIL_FAST
import shutil
import threading, gc
from time import sleep
from typing import Generator
from gateway_pb2_grpcbf import StartService_input
from singleton import Singleton
from start import LOGGER, SHA3_256, get_grpc_uri, DIR
import grpc, solvers_dataset_pb2, api_pb2, gateway_pb2_grpc, regresion_pb2_grpc, gateway_pb2, regresion_pb2, os
from utils import read_file
from grpcbigbuffer import client_grpc, Dir

class Session(metaclass = Singleton):

    def __init__(self, ENVS) -> None:
        self.data_set = None
        
        self.hashes=[
            gateway_pb2.celaut__pb2.Any.Metadata.HashTag.Hash(
                type = bytes.fromhex("a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a"),
                value = bytes.fromhex("5638617436764bfd22e1f2634eac88230e4c51359314791924c33faea8af092d")
            )
        ]
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

        # for maintain.
        self.data_set_hash = ""
        threading.Thread(target = self.maintenance, name = 'Regresion').start()
    
    def service_extended(self):
        config = True
        for hash in self.hashes:
            if config:  # Solo hace falta enviar la configuracion en el primer paquete.
                config = False
                yield gateway_pb2.HashWithConfig(
                    hash = hash,
                    config = self.config,
                    min_sysreq = gateway_pb2.celaut__pb2.Sysresources(
                        mem_limit = 120*pow(10, 6)
                    )
                )
            yield hash
        yield (gateway_pb2.ServiceWithMeta, Dir(DIR + 'regresion.service'))

    def init_service(self):
        LOGGER('Launching regresion service instance.')
        while True:
            try:
                instance  = None
                for i in client_grpc(
                    method = self.gateway_stub.StartService,
                    input = self.service_extended(),
                    indices_parser = gateway_pb2.Instance,
                    partitions_message_mode_parser=True,
                    indices_serializer = StartService_input,
                    # timeout = self.START_AVR_TIMEOUT
                ): instance = i
                break
            except grpc.RpcError as e:
                LOGGER('GRPC ERROR.' + str(e))
                sleep(10)
        uri = get_grpc_uri(instance.instance)        

        self.stub = regresion_pb2_grpc.RegresionStub(
            grpc.insecure_channel(
                uri.ip + ':' + str(uri.port)
            )
        )
        self.token = instance.token
        LOGGER('Regression service instance was recived.')

    def stop(self):
        LOGGER('Stopping regresion service instance.')
        while True:
            try:
                next(client_grpc(
                    method = self.gateway_stub.StopService,
                    input = gateway_pb2.TokenMessage(
                            token = self.token
                        ),
                    indices_serializer = gateway_pb2.TokenMessage,
                ))
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
        self.init_service()
        while True:
            sleep(self.TIME_FOR_EACH_LOOP)
            # Obtiene una hash del dataset para saber si se han añadido datos.
            if self.data_set:
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
                        shutil.move(
                            self.iterate_regression(
                                data_set = data_set
                            ),
                            '__tensor__'
                        )
                    except Exception as e:
                        LOGGER('Exception with regresion service, ' + str(e))
                        continue

    def get_tensor(self) -> regresion_pb2.Tensor:
        # No hay condiciones de carrera aunque lo reescriba en ese momento.
        if os.path.isfile('__tensor__'):
            tensor = regresion_pb2.Tensor()
            tensor.ParseFromString(
                read_file('__tensor__')
            )
            return tensor
        else:
            raise Exception('__tensor__ does not exist.')

    # Add new data
    def add_data(self, new_data_set: solvers_dataset_pb2.DataSet) -> None:
        self.dataset_lock.acquire()
        if not self.data_set: self.data_set = solvers_dataset_pb2.DataSet()
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
                for file in client_grpc(
                    method = self.stub.StreamLogs,
                    indices_parser = regresion_pb2.File,
                    partitions_message_mode_parser = True,
                    timeout = self.START_AVR_TIMEOUT
                ): yield file
                
            except (grpc.RpcError, TimeoutError) as e:
                self.error_control(e)
        
    # Make regresion Grpc method. Return the Tensor buffer.
    def iterate_regression(self, data_set: solvers_dataset_pb2.DataSet) -> str:
        try:
            return next(client_grpc(
                method= self.stub.MakeRegresion,
                input = data_set,
                indices_serializer = solvers_dataset_pb2.DataSet,
                indices_parser = regresion_pb2.Tensor,
                partitions_message_mode_parser = False,
            ))
        except (grpc.RpcError, TimeoutError) as e:
            self.error_control(e)
            raise e