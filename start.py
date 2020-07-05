import gotzilla
import sys

if __name__ == "__main__":

    gateway = sys.argv[1]
    refresh = sys.argv[2]

    gotzilla.start(gateway, refresh)