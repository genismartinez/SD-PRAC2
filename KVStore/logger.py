import logging.config
import os
import sys

def setup_logger():

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(filename)s:%(lineno)s -- %(message)s")
    handler.setFormatter(formatter)
    root.addHandler(handler)

