import shutil
import threading
from time import sleep

import os

from node_driver.gateway.protos import celaut_pb2
from node_driver.dependency_manager.service_interface import ServiceInterface
from node_driver.dependency_manager.dependency_manager import DependencyManager
from node_driver.dependency_manager.service_instance import ServiceInstance
from grpcbigbuffer.client import client_grpc, Dir
from typing import Generator, Optional, Final, List, Dict

from protos import api_pb2, regresion_pb2_grpc, solvers_dataset_pb2, regresion_pb2
from src.envs import REGRESSION_SHA3_256, LOGGER, SHA3_256
from src.utils.singleton import Singleton
from src.utils.general import read_file


class Session(metaclass=Singleton):

    def __init__(self, time_for_each_regression_loop: int) -> None:
        self.data_set: Optional[solvers_dataset_pb2.DataSet] = None

        # set used envs on variables.
        self.TIME_FOR_EACH_LOOP: int = time_for_each_regression_loop

        self.dataset_lock = threading.Lock()

        self.service: ServiceInterface = DependencyManager().add_service(
            service_hash=REGRESSION_SHA3_256,
            config=celaut_pb2.Configuration(),
            stub_class=regresion_pb2_grpc.RegresionStub,
            dynamic=False
        )

        # for maintain.
        self.data_set_hash = ""
        threading.Thread(target=self.maintenance, name='Regresion').start()

    def maintenance(self):

        while True:
            sleep(self.TIME_FOR_EACH_LOOP)
            # Obtiene una hash del dataset para saber si se han añadido datos.
            if self.data_set:
                actual_hash = SHA3_256(
                    value=self.data_set.SerializeToString()
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
                                data_set=data_set
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
        with self.dataset_lock:
            if not self.data_set:
                self.data_set = solvers_dataset_pb2.DataSet()

            prev_instances: Dict[bytes: solvers_dataset_pb2.DataSetInstance] = {instance.configuration_hash: instance
                                                                                for instance in self.data_set.data}

            # Add the new data set to the dataset on regression module.
            for new_instance in new_data_set.data:
                if new_instance.configuration_hash in prev_instances:
                    for cnf, new_data in new_instance.data.items():
                        prev_data_instance: solvers_dataset_pb2.DataSetInstance = prev_instances[
                                                                                      new_instance.configuration_hash
                                                                                  ]
                        if cnf in prev_data_instance.data:
                            prev_data_instance.data[cnf].score = sum([
                                (prev_data_instance.data[cnf].index * prev_data_instance.data[cnf].score),
                                new_data.index * new_data.score,
                            ]) / (prev_data_instance.data[cnf].index + new_data.index)
                            prev_data_instance.data[cnf].index = prev_data_instance.data[cnf].index + new_data.index

                        else:
                            prev_data_instance.data[cnf].CopyFrom(new_data)

                else:
                    self.data_set.data.append(new_instance)
                    prev_instances[new_instance.configuration_hash] = new_instance

        LOGGER(f'Dataset updated. size: {self.data_set.ByteSize()}')

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
                        yield from client_grpc(
                                method=instance.stub.StreamLogs,
                                indices_parser=regresion_pb2.File,
                                partitions_message_mode_parser=True,
                                timeout=self.service.sc.timeout
                        )

                    except Exception as e:
                        instance.compute_exception(e=e)
            finally:
                self.service.push_instance(instance)

    # Make regression Grpc method. Return the Tensor buffer.
    def iterate_regression(self, data_set: solvers_dataset_pb2.DataSet) -> str:
        instance: ServiceInstance = self.service.get_instance()
        try:
            dataset: Dir = client_grpc(
                method=instance.stub.MakeRegresion,
                input=data_set,
                indices_serializer=solvers_dataset_pb2.DataSet,
                indices_parser=regresion_pb2.Tensor,
                partitions_message_mode_parser=False,
            )
            if dataset.type != regresion_pb2.Tensor:
                raise Exception("Incorrect regression type.")
            return dataset.dir
        except Exception as e:
            instance.compute_exception(e)

        self.service.push_instance(instance)
