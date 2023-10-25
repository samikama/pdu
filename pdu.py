import asyncio
import asyncio.subprocess
from asyncio import Queue as aQueue
from argparse import ArgumentParser as AP
from dataclasses import dataclass
import os, functools, signal
import logging, sys
from multiprocessing import JoinableQueue as Queue, cpu_count, Process, freeze_support
from multiprocessing.managers import BaseManager, SyncManager
import multiprocessing
import time
import queue, io
import gzip, csv
import pyarrow as pa
import pyarrow.dataset as ds
import resource, humanize

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


@dataclass(slots=True)
class ObjStats:
  name: str
  uid: int
  gid: int
  size: int
  blocks: int


class Parser(asyncio.SubprocessProtocol):
  exit_future: asyncio.Future
  queue: asyncio.Queue
  dir_queue: Queue
  level: int

  def __init__(self, exit_future: asyncio.Future, output_queue: Queue,
               dir_queue: Queue, level: int, curr_dir: str):
    self.exit_future = exit_future
    self.rem = None
    self.queue = output_queue
    self.dir_queue = dir_queue
    self.level = level
    self.dir = curr_dir
    self.done = False

  def pipe_data_received(self, fd, data):
    l = []
    if fd == 2:
      logger.debug("got stderr from subprocess {d} for {cd}".format(
          d=data.decode("utf-8", "backslashreplace"), cd=self.dir))
      return
    if self.rem:
      data = self.rem + data
      self.rem = None
    bio = data.split(b'\0')
    if not data.endswith(b'\0'):
      self.rem = bio[-1]
    bio.pop()
    for dr in bio:
      d = dr
      try:
        if os.path.isdir(d):
          d = d.decode()
          if d != self.dir:
            self.dir_queue.put((d, self.level))
          continue

        if os.path.islink(d):
          continue
      except Exception as e:
        logger.error("Encountered with exception {exc} dir={d}".format(exc=e,
                                                                       d=dr))

      try:
        s = os.stat(d)
      except:
        continue

      l.append(
          ObjStats(name=d,
                   uid=s.st_uid,
                   gid=s.st_gid,
                   size=s.st_size,
                   blocks=s.st_blocks))
    if l:
      self.queue.put((l, self.level))

  def pipe_connection_lost(self, fd, exc):
    if fd == 1:
      if self.done:
        self.exit_future.set_result(True)
      self.done = True

  def process_exited(self):
    if self.done:
      self.exit_future.set_result(True)
    self.done = True


def ArrowWriter(output_queue, filename):
  count = 0
  count_log = 10000000
  schema = pa.schema([("filename", pa.string()), ("uid", pa.int32()),
                      ("gid", pa.int32()), ("file_size", pa.int64()),
                      ("num_blocks", pa.int64())])
  struct = pa.struct([("filename", pa.string()), ("uid", pa.int32()),
                      ("gid", pa.int32()), ("file_size", pa.int64()),
                      ("num_blocks", pa.int64())])
  with pa.OSFile(filename, 'wb') as sink:
    with pa.CompressedOutputStream(sink, 'gzip') as csink:
      with pa.ipc.new_file(csink, schema=schema) as writer:
        while True:
          try:
            l, level = output_queue.get(True, timeout=30)
          except queue.Empty:
            logger.warning(
                "Writer did not get any new data in past 30s. Assuming finished"
            )
            break
          if l is None:
            output_queue.task_done()
            logger.warning("Got None. exiting")
            break
          if l:
            rows = [[
                x.name.decode("utf-8", "backslashreplace"), x.uid, x.gid,
                x.size, x.blocks
            ] for x in l]
            batch = pa.record_batch(list(zip(*rows)), schema=schema)
            writer.write(batch)
            count += len(l)
          output_queue.task_done()
          if count >= count_log:
            count_log += 10000000
            logger.info("Processed {count} files".format(
                count=humanize.intcomma(count)))
      logger.info("Finishing writer process! Processed {nrec} files".format(
          nrec=humanize.intcomma(count)))


def Writer(output_queue, filename):
  count = 0
  count_log = 10000000
  with gzip.open(filename, mode='wt', newline="") as output:
    csv_writer = csv.writer(output, dialect='unix')
    csv_writer.writerow(["filename", "uid", "gid", "file_size", "num_blocks"])
    while True:
      try:
        l, level = output_queue.get(True, timeout=30)
      except queue.Empty:
        logger.warning(
            "Writer did not get any new data in past 30s. Assuming finished")
        break
      if l is None:
        output_queue.task_done()
        logger.warning("Got None. exiting")
        break
      if l:
        rows = [[
            x.name.decode("utf-8", "backslashreplace"), x.uid, x.gid, x.size,
            x.blocks
        ] for x in l]
        csv_writer.writerows(rows)
        count += len(l)
      output_queue.task_done()
      if count >= count_log:
        count_log += 10000000
        logger.info("Processed {count} files".format(count=count))
    logger.info("Finishing writer process! Processed {nrec} files".format(
        nrec=humanize.intcomma(count)))


async def check_lsf(dir_queue: Queue, output_queue: Queue, is_lustre: bool,
                    wait_event):
  # dir_queue and output_queue is across processes
  loop = asyncio.get_running_loop()
  wait_count = 0
  while True and not STOP_SCAN:
    try:
      dir, level = dir_queue.get(True, timeout=1)
    except queue.Empty:
      if wait_event.is_set():
        logger.info("Writing finished. Exiting ")
        break
      continue
    if dir is None:
      logger.info("{name} got dir=None queue size={qs} exiting".format(
          name=multiprocessing.current_process().name, qs=dir_queue.qsize()))
      dir_queue.task_done()
      break
    if not dir:
      logger.error("Encountered with empty dir at level {l}".format(l=level))
      dir_queue.task_done()
      continue
    future = asyncio.Future(loop=loop)
    cmd = '{dir}'.format(dir=os.path.abspath(dir))
    #logger.info("{worker} processing {dir}".format(worker=multiprocessing.current_process(),dir=cmd))
    if not is_lustre:
      transport, protocol = await loop.subprocess_exec(
          lambda: Parser(exit_future=future,
                         output_queue=output_queue,
                         dir_queue=dir_queue,
                         level=level + 1,
                         curr_dir=cmd),
          '/usr/bin/find',
          cmd,
          '-maxdepth',
          '1',
          '-print0',
          stdin=None)
    else:
      transport, protocol = await loop.subprocess_exec(
          lambda: Parser(exit_future=future,
                         output_queue=output_queue,
                         dir_queue=dir_queue,
                         level=level,
                         curr_dir=cmd),
          '/usr/bin/lfs',
          'find',
          cmd,
          '-maxdepth',
          '1',
          '-print0',
          '--lazy',
          stdin=None)

    # transport,protocol=await loop.subprocess_exec(
    #   lambda: Parser(exit_future=future,output_queue=output_queue,dir_queue=dir_queue,level=level,curr_dir=cmd),
    #     executable,*args,
    #       stdin=None, stderr=None)

    await future
    transport.close()
    dir_queue.task_done()


async def scan_loop(dir_queue, output_queue, lustrefs, wait_event):
  await check_lsf(dir_queue=dir_queue,
                  output_queue=output_queue,
                  is_lustre=lustrefs,
                  wait_event=wait_event)


def parse_arguments():
  parser = AP("Parallel DU")
  parser.add_argument('-s', '--scan-dir', default=None, required=True)
  parser.add_argument('-n',
                      '--num-workers',
                      default=cpu_count(),
                      type=int,
                      required=True)
  parser.add_argument('-f', '--filename', default=None, required=True)
  parser.add_argument('-l', '--lustrefs', default=True, action='store_false')
  parser.add_argument('-m', "--master-node", default="127.0.0.1")
  parser.add_argument("-r",
                      "--rank",
                      default=os.environ.get("SLURM_NODEID", 0),
                      type=int)
  parser.add_argument('-c', '--csv-file', default=False, action='store_true')
  return parser.parse_known_args()


def main():
  args, unknown = parse_arguments()
  print(args)
  #dir_queue = Queue()
  #output_queue = Queue()
  if not os.path.isdir(args.scan_dir.encode()):
    logger.fatal("Error {path} is not a directory".format(path=args.scan_dir))
    sys.exit(1)
  logger.info("Starting to process {path} with {nproc} workers".format(
      path=os.path.abspath(args.scan_dir), nproc=args.num_workers))

  if args.rank == 0:
    multi_node_queue = Queue()
    output_queue = Queue()
    wait_event = multiprocessing.Event()
    dir_queue = multi_node_queue
    dir_queue.put((args.scan_dir, 0))
    QueueManager.register('get_input_queue', callable=lambda: multi_node_queue)
    QueueManager.register('get_output_queue', callable=lambda: output_queue)
    QueueManager.register('get_event', callable=lambda: wait_event)
    manager = QueueManager(address=(args.master_node, 56776),
                           authkey=os.environ.get("SLURM_JOB_ID",
                                                  "verySecret!").encode())
    manager.start()
    if args.csv_file:
      writer = Process(target=Writer,
                       args=(output_queue, args.filename),
                       name="Writer")
    else:
      writer = Process(target=ArrowWriter,
                       args=(output_queue, args.filename),
                       name="Writer")
  else:
    QueueManager.register('get_input_queue')
    QueueManager.register('get_output_queue')
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
    dir_queue = manager.get_input_queue()
    output_queue = manager.get_output_queue()
    wait_event = manager.get_event()

  workers = [
      Process(target=amain,
              args=(dir_queue, output_queue, args.lustrefs, wait_event),
              name=f"worker-{i}") for i in range(args.num_workers)
  ]
  try:
    for w in workers:
      w.start()
    if args.rank == 0:
      writer.start()
      writer.join()
      wait_event.set()
    else:
      logger.info("Waiting on event")
      wait_event.wait()
      logger.info("Master node event triggered")
  except KeyboardInterrupt:
    for w in workers:
      w.terminate()

  for _ in workers:
    dir_queue.put((None, 0))
  for _ in range(10):
    active = multiprocessing.active_children()
    logger.info("Num active children={active}".format(active=len(active)))
    if not len(active):
      break
    # for a in active:
    # try:
    #   dir_queue.get_nowait()
    # except:
    #   pass
    # a.join()
    time.sleep(4)

  for w in workers:
    w.kill()
  time.sleep(10)
  if args.rank == 0:
    manager.shutdown()


def amain(dir_queue, output_queue, lustrefs, wait_event):
  loop = asyncio.new_event_loop()
  # for signame in {'SIGINT', 'SIGTERM'}:
  #   loop.add_signal_handler(getattr(signal, signame),
  #                           functools.partial(handler, signame))
  try:
    loop.run_until_complete(
        scan_loop(dir_queue, output_queue, lustrefs, wait_event))
  except KeyboardInterrupt:
    global STOP_SCAN
    STOP_SCAN = True
    pass
  except asyncio.CancelledError as e:
    logger.info("Scan cancelled, cleaning up!")
  loop.close()
  logger.info("worker={worker} bye bye".format(
      worker=multiprocessing.current_process()))
  return


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
