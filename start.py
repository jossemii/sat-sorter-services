import gotzilla
import sys
import os

if __name__ == "__main__":

    gateway = os.environ['GATEWAY']
    refresh = os.environ['REFRESH']

    gotzilla.start(gateway, refresh)