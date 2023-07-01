import shutil
import threading
from time import sleep

import os

import celaut_framework.protos.celaut_pb2
from celaut_framework.dependency_manager.service_interface import ServiceInterface
from celaut_framework.dependency_manager.dependency_manager import DependencyManager
from celaut_framework.dependency_manager.service_instance import ServiceInstance
from grpcbigbuffer.client import client_grpc
from typing import Generator

from protos import api_pb2, regresion_pb2_grpc, solvers_dataset_pb2, regresion_pb2
from src.envs import REGRESSION_SHA3_256, LOGGER, SHA3_256
from src.utils.singleton import Singleton
from src.utils.general import read_file


class Session(metaclass = Singleton):

    def __init__(self, time_for_each_regression_loop: int) -> None:
        self.data_set = None

        # set used envs on variables.
        self.TIME_FOR_EACH_LOOP: int = time_for_each_regression_loop

        self.dataset_lock = threading.Lock()

        self.service: ServiceInterface = DependencyManager().add_service(
            service_hash = REGRESSION_SHA3_256,
            config = celaut_framework.protos.celaut_pb2.Configuration(),
            stub_class = regresion_pb2_grpc.RegresionStub,
            dynamic = False
        )

        # for maintain.
        self.data_set_hash = ""
        threading.Thread(target = self.maintenance, name = 'Regresion').start()

    def maintenance(self):

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


    # Stream logs Grpc method. TODO CHECK
    def stream_logs(self) -> Generator[api_pb2.File, None, None]:

        instance: ServiceInstance = self.service.get_instance()
        while True:
            try:
                for i in range(1):
                    try:
                        for file in client_grpc(
                            method = instance.stub.StreamLogs,
                            indices_parser = regresion_pb2.File,
                            partitions_message_mode_parser = True,
                            timeout = self.service.sc.timeout
                        ): yield file

                    except Exception as e:
                        instance.compute_exception(e=e)
            finally:
                self.service.push_instance(instance)

        
    # Make regresion Grpc method. Return the Tensor buffer.
    def iterate_regression(self, data_set: solvers_dataset_pb2.DataSet) -> str:
        instance: ServiceInstance = self.service.get_instance()
        try:
            dataset_it = client_grpc(
                method=instance.stub.MakeRegresion,
                input=data_set,
                indices_serializer=solvers_dataset_pb2.DataSet,
                indices_parser=regresion_pb2.Tensor,
                partitions_message_mode_parser=False,
            )
            dataset_obj = next(dataset_it)
            if dataset_obj != str:
                dataset_obj = next(dataset_it)
            return dataset_obj
        except Exception as e:
            instance.compute_exception(e)

        self.service.push_instance(instance)