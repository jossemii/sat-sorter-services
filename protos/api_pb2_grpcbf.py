import celaut_framework.gateway.protos.buffer_pb2 as buffer_pb2

# This is part of the transport protocol (slot) data.
UploadService_input_partitions = [
            buffer_pb2.Buffer.Head.Partition(index={
                1 : buffer_pb2.Buffer.Head.Partition(),
                2 : buffer_pb2.Buffer.Head.Partition(index={
                    1 : buffer_pb2.Buffer.Head.Partition(index={
                        3 : buffer_pb2.Buffer.Head.Partition(),
                        4 : buffer_pb2.Buffer.Head.Partition(),
                    }),
                    2 : buffer_pb2.Buffer.Head.Partition(),
                    3 : buffer_pb2.Buffer.Head.Partition(),
                    4 : buffer_pb2.Buffer.Head.Partition(),
                })
            }),
            buffer_pb2.Buffer.Head.Partition(index={
                2 : buffer_pb2.Buffer.Head.Partition(index={
                    1 : buffer_pb2.Buffer.Head.Partition(index={
                        1 : buffer_pb2.Buffer.Head.Partition(),
                        2 : buffer_pb2.Buffer.Head.Partition()
                    }),
                })
            })
        ]