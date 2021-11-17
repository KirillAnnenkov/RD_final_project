# -*- coding: utf-8 -*-

import json
import os

def config(file_json, var):
    with open(os.path.join(file_json), "r") as read_file:
        return json.load(read_file)[var]