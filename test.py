# -*- coding: utf-8 -*-
"""
Created on Fri Feb 28 09:42:07 2020

@author: Blanca.Alonso
"""

# --------------------------------------------------------------------------#
#  0.- LIBRERÄ‚ÂAS Y DIRECTORIO --------------------------------------------
# --------------------------------------------------------------------------# 

import os

os.chdir("E:\\0_Blanca\\7_Cloud_Squad")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "E:\\0_Blanca\\7_Cloud_Squad\\key.json"


#import time

from google.cloud import pubsub_v1


# Variables ---------------------------------------------------------------
# --------------------------------------------------------------------------#

# Definimos el nombre del proyecto y el tema de gcp
project_id = "mbd-analytics"
topic_name = "prueba"

# Publisher y ruta del tema
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)



# --------------------------------------------------------------------------#
#  1.- FUNCIONES ----------------------------------------------------------
# --------------------------------------------------------------------------#


# Publicar mensajes -------------------------------------------------------
# --------------------------------------------------------------------------#


# Configure the batch to publish as soon as there is one kilobyte
# of data or one second has passed.
batch_settings = pubsub_v1.types.BatchSettings(
    max_bytes = 1024, max_latency = 1  # One kilobyte  # One second
)

publisher = pubsub_v1.PublisherClient(batch_settings)
topic_path = publisher.topic_path(project_id, topic_name)

for n in range(1, 10):
    data = u"Message number {}".format(n)
    # Data must be a bytestring
    data = data.encode("utf-8")
    future = publisher.publish(topic_path, data=data)
    print(future.result())

print("Published messages with batch settings.")
