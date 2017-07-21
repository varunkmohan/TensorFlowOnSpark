# Copyright 2017 Yahoo Inc.
# Licensed under the terms of the Apache 2.0 license.
# Please see LICENSE file in the project root for terms.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from pyspark.context import SparkContext
from pyspark.conf import SparkConf

import argparse
import os
import numpy
import sys
import tensorflow as tf
import threading
from datetime import datetime

from tensorflowonspark import TFCluster
import resnet_dist

sc = SparkContext(conf=SparkConf().setAppName("mnist_tf"))
executors = sc._conf.get("spark.executor.instances")
#  num_executors = int(executors) if executors is not None else 1
num_executors = 4 # hardcoded for now since
num_ps = 1

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--dataset", help="use either cifar10 or cifar100", default="cifar10")
parser.add_argument("-X", "--mode", help="train|eval", default="train")
parser.add_argument("-tdp", "--train_data_path", help="training data path")
parser.add_argument("-edp", "--eval_data_path", help="eval data path")
parser.add_argument("-is", "--image_size", help="Image side length", type=int, default=32)
parser.add_argument("-td", "--train_dir", help="Directory to keep training outputs.")
parser.add_argument("-ed", "--eval_dir", help="Directory to keep eval outputs.")
parser.add_argument("-ebc", "--eval_batch_count", help="Directory to keep eval outputs.", type=int, default=50)
parser.add_argument("-eo", "--eval_once", help="Whether evaluate model only once.", type=bool, default=False)
parser.add_argument("-lr", "--log_root", help="Directory to keep the checkpoints. Should be a parent directory of Flags.train_dir/eval_dir.", type=bool, default=False)
parser.add_argument("-c", "--rdma", help="use rdma connection", default=False)
parser.add_argument("-n", "--cluster_size", help="number of nodes in the cluster (for Spark Standalone)", type=int, default=num_executors)
parser.add_argument("-tb", "--tensorboard", help="launch tensorboard process", action="store_true")
args = parser.parse_args()
print("args:",args)


print("{0} ===== Start".format(datetime.now().isoformat()))
cluster = TFCluster.run(sc, resnet_dist.map_fun, args, args.cluster_size, num_ps, args.tensorboard, TFCluster.InputMode.TENSORFLOW)
cluster.shutdown()

print("{0} ===== Stop".format(datetime.now().isoformat()))

