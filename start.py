import gotzilla
import argparse

if __name__ == "__main__":

    gateway = None # $GATEWAY
    refresh = None # $REFRESH

    gotzilla.start(gateway, refresh)