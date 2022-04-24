class Singleton(type):
    _instances = {}

    def __call__(cls, ENVS):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(ENVS)
        return cls._instances[cls]