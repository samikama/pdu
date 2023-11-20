import asyncio
import asyncio.subprocess
from asyncio import Queue as aQueue
from argparse import ArgumentParser as AP
from typing import List
from dataclasses import dataclass
import os, functools, signal
import logging, sys
from multiprocessing import JoinableQueue as Queue, cpu_count, Process, freeze_support, Event
from multiprocessing.managers import BaseManager, SyncManager
import multiprocessing
import time
import queue, io
import gzip, csv
import pyarrow as pa
import pyarrow.dataset as ds
import resource, humanize
import pyarrow as pa
import pandas, numpy as np

logging.basicConfig(
    format=
    '%(asctime)s.%(msecs)03d %(name)s %(filename)s:%(funcName)s:%(lineno)d %(message)s',
    datefmt='%Y-%m-%d,%H:%M:%S',
    level=logging.INFO)
logger = logging.getLogger()
STOP_SCAN = False


class QueueManager(SyncManager):
  pass


def run_safe(fun, *args, **kwargs):
  logger.info("Wrapping {func} to run".format(func=fun.__name__))
  try:
    ret = fun(*args, **kwargs)
  except Exception as e:
    logger.error(
        "Caught exception while running {func}. Exception was {exc}".format(
            func=fun.__name__, exc=str(e)))
    return None
  return ret


def get_csv_df(filename):
  df = pandas.read_csv(filename)
  #logging.info("Parsing csv {f} completed".format(f=filename))
  return df


def get_arrow_df(filename):
  with gzip.open(filename=filename, mode="rb") as reader:
    read_opts = pa.ipc.IpcReadOptions(use_threads=True)
    with pa.ipc.open_file(reader, options=read_opts) as stream:
      df = stream.read_pandas()
      return df


def db_reader(filename):
  fdate = os.path.splitext(
      os.path.basename(filename).replace(".arrow",
                                         "").replace(".csv", ""))[0].split('-')
  if len(fdate) < 2:
    fdate = int(time.time())
  else:
    fdate = fdate[1]
  logging.info("Opening {f}".format(f=filename))
  if ".csv." in filename:
    df = get_csv_df(filename)
  else:
    df = get_arrow_df(filename)

  return df.groupby(['uid'], sort=False)


def feed_queue(filename,
               process_queue: multiprocessing.Queue,
               wait_event: multiprocessing.Event,
               chunk_size=4096):
  df = db_reader(filename=filename)
  chunk_count = 0
  file_count = 0
  for uid in df.groups.keys():
    files = df.get_group(uid)
    for batch in files.groupby(np.arange(len(files)) // chunk_size):
      l = []
      chunk_count += 1
      file_count += len(batch[1])
      for f in batch[1].itertuples(index=False):
        l.append((f.filename, f.uid, f.gid))
      process_queue.put((uid, l))
      if chunk_count % 100 == 0:
        logger.info("Feeder processed {c} chunks containing {f} entries".format(
            c=humanize.intcomma(chunk_count), f=humanize.intcomma(file_count)))
  counter = 0
  while not process_queue.empty() and counter < 10:
    time.sleep(10)
  logger.info("Queue Feeder is done. queue is {e}".format(
      e="Empty" if process_queue.empty() else "Full"))
  wait_event.set()


# class LocalWorker(Process):

#   def __init__(self, queue: multiprocessing.Queue,
#                termination_event: multiprocessing.Event, uid_mapping: dict,
#                gid_mapping: dict):
#     self._queue = queue
#     self._term_event = termination_event
#     self._uids = uid_mapping
#     self._gids = gid_mapping


def run(queue_: multiprocessing.Queue, termination_event: multiprocessing.Event,
        uid_mapping: dict, gid_mapping: dict, name: str):
  _term_event = termination_event
  _queue = queue_
  _uids = uid_mapping
  _gids = gid_mapping
  succ = 0
  fail = 0
  skip = 0
  already = 0
  attempts = 0
  queue_empty = False
  while not _term_event.is_set() or not queue_empty:
    try:
      items = _queue.get(True, timeout=5)
      attempts = 0
    except queue.Empty as e:
      if _term_event.is_set():
        attempts += 1
        queue_empty = attempts > 2
      continue
    if items is None:
      break
    old_id = int(items[0])
    files = items[1]
    if old_id not in _uids:
      logger.error(
          "Unknown mapping for old uid {uid}. Skipping".format(uid=old_id))
      continue
    new_id = _uids[old_id]
    for f, u, g in files:
      if os.path.exists(f):
        ng = _gids.get(g, int(g))
        if u == new_id:
          already += 1
          continue
        if u != old_id:
          logger.warning(
              "File {f} has different old uid({u}) then expected({old})".format(
                  f=f, u=u, old=old_id))
        try:
          os.chown(f, uid=new_id, gid=ng, follow_symlinks=False)
          succ += 1
        except Exception as e:
          fail += 1
          logger.error("While processing {f} caught exception {e}".format(f=f,
                                                                          e=e))
      else:
        skip += 1
  logger.info(
      "Worker {n} exiting. Success={succ}, Fail={fail}, Skipped={skip}, Already Updated={al} total={tot}"
      .format(succ=succ,
              fail=fail,
              skip=skip,
              al=already,
              n=name,
              tot=succ + fail + skip))


def read_mapping(filename) -> dict:
  with open(filename, "rt") as f:
    lines = f.readlines()
    return dict([
        (int(x[0]), int(x[1])) for y in lines for x in y.strip().split(',')
    ])


def node_main(args, manager: QueueManager, num_local_workers=16):
  local_queue = Queue(maxsize=5 * num_local_workers)
  local_event = Event()
  local_event.clear()
  workers: List[Process] = []
  uid_mapping = read_mapping(args.uid_mapping)
  gid_mapping = read_mapping(args.gid_mapping)
  logger.info("UID Mapping={u} gid_mapping={g}".format(u=uid_mapping,
                                                       g=gid_mapping))
  for i in range(num_local_workers):
    w = Process(target=run,
                args=(local_queue, local_event, uid_mapping, gid_mapping,
                      f"Worker-{i}"),
                name=f"Worker-{i}")
    # w.name = f"Worker-{i}"
    workers.append(w)
  for w in workers:
    w.start()
  proc_queue = manager.get_input_queue()
  term_event = manager.get_event()
  queue_empty = False
  attempt = 0
  while not term_event.is_set() or not queue_empty:
    try:
      l = proc_queue.get(True, timeout=5)
    except queue.Empty as e:
      if term_event.is_set():
        attempt += 1
        queue_empty = attempt > 2
      continue
    local_queue.put(l)
  local_event.set()
  for w in workers:
    w.join(timeout=12)
  for w in workers:
    if w.is_alive():
      w.kill()
      logger.info("Child {c} exited with {e}".format(c=w.name, e=w.exitcode))
    else:
      logger.info("Child {c} exited with {e}".format(c=w.name, e=w.exitcode))


def parse_arguments():
  parser = AP("Parallel chown")
  parser.add_argument('-d', '--database-file', default=None, required=True)
  parser.add_argument('-n',
                      '--num-workers',
                      default=cpu_count(),
                      type=int,
                      required=True)
  parser.add_argument('-c',
                      '--chunk_size',
                      default=4096,
                      type=int,
                      required=True)
  parser.add_argument('-u', '--uid-mapping', default=None, required=True)
  parser.add_argument('-g', '--gid-mapping', default=None, required=True)
  parser.add_argument('-m', "--master-node", default="127.0.0.1")
  parser.add_argument("-r",
                      "--rank",
                      default=os.environ.get("SLURM_NODEID", 0),
                      type=int)
  return parser.parse_known_args()


def main():
  args, unknown = parse_arguments()
  print(args)
  #dir_queue = Queue()
  #output_queue = Queue()
  if not os.path.isfile(args.database_file.encode()):
    logger.fatal("Error database file {path} does not exists".format(
        path=args.database_file))
    sys.exit(1)
  logger.info("Starting to process {path} with {nproc} workers".format(
      path=os.path.abspath(args.database_file), nproc=args.num_workers))

  if args.rank == 0:
    processing_queue = Queue(1024)
    wait_event = multiprocessing.Event()
    QueueManager.register('get_input_queue', callable=lambda: processing_queue)
    QueueManager.register('get_event', callable=lambda: wait_event)
    manager = QueueManager(address=(args.master_node, 56776),
                           authkey=os.environ.get("SLURM_JOB_ID",
                                                  "verySecret!").encode())
    manager.start()
    reader_proc = Process(target=feed_queue,
                          args=(args.database_file, processing_queue,
                                wait_event, args.chunk_size),
                          name="DBReader")
    node_proc = Process(target=node_main,
                        args=(args, manager, args.num_workers),
                        name="Master NodeProcessor")
    reader_proc.start()
    node_proc.start()
    logger.info("Waiting for queue processing!")
    reader_proc.join()
    logger.info("Waiting for node processing!")
    node_proc.join()
  else:
    QueueManager.register('get_input_queue')
    QueueManager.register('get_event')
    manager = QueueManager(address=(args.master_node, 56776),
                           authkey=os.environ.get("SLURM_JOB_ID",
                                                  "verySecret!").encode())
    #sleep 10s to let master process start
    connected = False
    for _ in range(12):
      try:
        manager.connect()
        connected = True
      except:
        time.sleep(10)
    if not connected:
      logger.error(
          "Failed to connect to manager at {mgr_address}. Exiting".format(
              mgr_address=args.master_node))
      sys.exit(1)
    logger.info("connected to manager at {mgr_address}".format(
        mgr_address=args.master_node))
    logger.info("Waiting for processing of the input, queue rank={r}".format(
        r=args.rank))
    node_main(args, manager, args.num_workers)
    logger.info("Input queue processing donequeue rank={r}".format(r=args.rank))
  if args.rank == 0:
    manager.shutdown()
  logger.info("worker={worker} bye bye".format(
      worker=multiprocessing.current_process()))


def format_rusage(ru: resource.struct_rusage):
  ret = [
      "User  : {}s".format(ru.ru_utime),
      "System: {}s".format(ru.ru_stime),
      "MaxRSS: {}".format(humanize.naturalsize(ru.ru_maxrss * 1024,
                                               binary=True)),
  ]
  return ", ".join(ret)


if "__main__" in __name__:
  tstart = time.perf_counter()
  freeze_support()
  ret = main()
  tend = time.perf_counter()
  args, unknown = parse_arguments()
  logger.info(
      "Rank={rank} total time={t:.3f}s, Resource use Self={rusage}, Children={child}"
      .format(rank=args.rank,
              rusage=format_rusage(resource.getrusage(resource.RUSAGE_SELF)),
              child=format_rusage(resource.getrusage(resource.RUSAGE_CHILDREN)),
              t=(tend - tstart)))
  sys.exit(ret)
