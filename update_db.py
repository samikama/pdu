from __future__ import annotations
import gzip, pandas
#import dask
#import dask.dataframe as dsk
from argparse import ArgumentParser as AP
import humanize
import os, time, sys
from typing import Any, List, Dict, TypeVar
from dataclasses import dataclass, field
import logging
import multiprocessing
import pwd, grp
from multiprocessing import Pool
import pyarrow as pa
import resource
import sqlite3

logging.basicConfig(
    format=
    '%(asctime)s.%(msecs)03d %(name)s %(filename)s:%(funcName)s:%(lineno)d %(message)s',
    datefmt='%Y-%m-%d,%H:%M:%S',
    level=logging.INFO)
logger = logging.getLogger()


class DB():

  def __init__(self, filename) -> None:
    self._filename = filename
    self._connection = sqlite3.connect(self._filename)
    self._cursor = self._connection.cursor()
    self._cursor.execute(
        "CREATE TABLE IF NOT EXISTS users(uid integer PRIMARY KEY, name text NOT NULL)"
    )

  def add_to_user(self, uid, scan_date, num_files, file_size, top_dirs):
    user_table = "user_{id}".format(id=uid)
    self._cursor.execute(
        "CREATE TABLE IF NOT EXISTS {table}(date integer PRIMARY KEY, file_count integer, size integer, top_dirs text NOT NULL)"
        .format(table=user_table))
    self._cursor.execute(
        "INSERT INTO {table} VALUES({scand},{count},{size},'{topd}')".format(
            table=user_table,
            scand=scan_date,
            count=num_files,
            size=file_size,
            topd=top_dirs))
    self._connection.commit()


class DBSingleton(type):
  _instances = {}

  def __call__(cls, *args: Any, **kwds: Any) -> DB:
    if cls not in cls._instances:
      cls._instances[cls] = super(DBSingleton, cls).__call__(*args, **kwds)
    return cls._instances[cls]


class DBConnection(DB, metaclass=DBSingleton):
  pass


@dataclass
class TFile:
  name: str
  uid: int
  gid: int
  size: int = 0
  blocks: int = 0


TDirectorySelf = TypeVar("TDirectorySelf", bound="TDirectory")


@dataclass
class TDirectory:
  name: str
  size: int = 0
  self_size: int = 0
  blocks: int = 0
  parent: TDirectorySelf = None
  file_count: int = 0
  files: List[TFile] = field(default_factory=list)
  dirs: dict[str, TDirectorySelf] = field(default_factory=dict)

  def __or__(self, other: TDirectory):
    assert type(self) == type(other)
    assert self.name == other.name
    assert self.parent == other.parent
    self.files.extend(other.files)
    self.size += other.size
    self.blocks += other.blocks
    self.self_size += other.self_size
    self.file_count += other.file_count
    for n, d in other.dirs.items():
      m = self.dirs.setdefault(n, TDirectory(name=n))
      m | d


def process_files(df, relative='/'):
  root = TDirectory(name=relative)
  counter = 0
  logcount = 99999999
  pp_old = None
  for it in df.itertuples():
    uid = it.uid
    gid = it.gid
    f = it.filename
    size = it.file_size
    blocks = it.num_blocks
    counter += 1
    if counter > logcount:
      logging.info("processed {counter} items".format(
          counter=humanize.intcomma(counter)))
      logcount += 10000000
    fp, fname = os.path.split(f)
    pp = fp
    if fp != pp_old:
      dir_stack = []
      while pp != relative and pp != "/":
        pp, d = os.path.split(pp)
        dir_stack.append(d)
      dd = root
      for d in reversed(dir_stack):
        n = dd.dirs.setdefault(d, TDirectory(name=d, parent=dd))
        dd = n
      pp_old = fp
    #dd.files.append(TFile(fname, uid, gid, size, blocks))
    dd.file_count += 1
    dd.self_size += size
    par = dd.parent
    while par:
      par.size += size
      par.blocks += blocks
      par = par.parent
  logging.info("Processed {ctr} files".format(ctr=humanize.intcomma(counter)))
  return root


def dirstats(root: TDirectory, depth=2):
  dirs = []
  if depth == 0:
    return [[root.name, root.size, root.blocks]]
  dirs.append([root.name, root.self_size, root.blocks])
  for d in root.dirs.values():
    ret = dirstats(d, depth - 1)
    for r in ret:
      r[0] = os.path.join(root.name, r[0])
    dirs.extend(ret)
  return dirs


def max_dirs(root, depth=2):
  dirs = dirstats(root, depth=depth)
  sorted_dirs = sorted(dirs, key=lambda x: x[1], reverse=True)
  return sorted_dirs


def get_topdirs(id, df2, max_dir_depth, top_d):
  logger.info("Processing user {uid} pid={pid} name={name}".format(
      uid=id, pid=os.getpid(), name=multiprocessing.current_process().name))
  files = df2.get_group(id).sort_values(by='filename')
  tree = process_files(files)
  sorted_dirs = max_dirs(tree, max_dir_depth)
  top_dirs = [
      x for x in sorted_dirs[:min(len(sorted_dirs), top_d)] if x[1] != 0
  ]
  del tree
  return id, top_dirs


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


def stats(
    filename,
    sql_file,
    top_d,
    max_dir_depth,
):
  # with gzip.open(filename, mode='rt', newline="") as input:
  #   df = pandas.read_csv(input)

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
    df = get_arrow_df(filename=filename)
  logging.info("Parsing {f} completed".format(f=filename))
  df2 = df.groupby(['uid'], sort=False)
  counts = df2['filename'].count().sort_values()
  sizes = df2['file_size'].sum().sort_values()
  #total_size = df2['num_blocks'].sum().mul(512).nlargest(top_n)
  #counts, sizes, total_size = dask.compute(counts, sizes, total_size)
  logger.info("Top_n computation is done")
  # most_files = [
  #     "----------Users with most files------------",
  #     "{uid:>15s} {gid:>15s} {count:>30s}".format(uid="uid",
  #                                                 gid="gid",
  #                                                 count="Number of files")
  # ]

  # for p, c in counts.items():
  #   uid = p[0]
  #   gid = p[1]
  #   try:
  #     uid = pwd.getpwuid(p[0]).pw_name
  #     gid = grp.getgrgid(p[1]).gr_name
  #   except:
  #     pass
  #   most_files.append("{uid:>15} {gid:>15} {count:>30s}".format(
  #       uid=uid, gid=gid, count=humanize.intcomma(c)))
  # print("\n".join(most_files))

  # most_volume = [
  #     "----------Users with most disk usage------------",
  #     "{uid:>15s} {gid:>15s} {count:>30s}: {dirs}".format(
  #         uid="uid", gid="gid", count="Total File Size", dirs="Top dirs")
  # ]
  # if n_proc > 0:
  #   with Pool(n_proc) as pool:
  #     uids = [p[0] for p, _ in sizes.items()]
  #     nusers = len(uids)
  #     logger.info(
  #         "Starting per directory disk utilization calculation using {proc} processes for {nusers} users"
  #         .format(proc=n_proc, nusers=nusers))
  #     args = zip(uids, [df] * nusers, [max_dir_depth] * nusers,
  #                [top_d] * nusers)
  #     #print(list(args))
  #     top_dirs_all = pool.starmap(get_topdirs, args, chunksize=1)
  #     logger.info("Per directory disk utilization calculation finished")
  #     tdict = dict(top_dirs_all)
  db_connection = DBConnection(sql_file)
  for p, c in sizes.items():
    uid = p
    fcount = counts.get(uid)
    fsize = sizes.get(uid)

    #gid = p[1]
    # if n_proc > 0:
    #   top_dirs = tdict[uid]
    # else:å

    id, top_dirs = get_topdirs(uid,
                               df2,
                               max_dir_depth=max_dir_depth,
                               top_d=top_d)
    dirs = ", ".join([
        "{dir}={size}".format(dir=x[0],
                              size=humanize.naturalsize(x[1], binary=True))
        for x in top_dirs
    ])
    db_connection.add_to_user(uid=uid,
                              scan_date=fdate,
                              num_files=fcount,
                              file_size=fsize,
                              top_dirs=dirs)
  #   try:
  #     uid = pwd.getpwuid(p[0]).pw_name
  #     # gid = grp.getgrgid(p[1]).gr_name
  #   except:
  #     pass
  #   most_volume.append("{uid:>15} {gid:>15} {count:>30s}: {dirs}".format(
  #       uid=uid,
  #       gid=gid,
  #       count=humanize.naturalsize(c, binary=True),
  #       dirs=", ".join([
  #           "{dir}={size}".format(dir=x[0],
  #                                 size=humanize.naturalsize(x[1], binary=True))
  #           for x in top_dirs
  #       ])))

  # print("\n".join(most_volume))


def parse_arguments():
  parser = AP("DUStats")
  parser.add_argument('-f', '--filename', default=None, type=str, required=True)
  parser.add_argument('-d', '--dirs-per-user', default=10, type=int)
  parser.add_argument('-m', '--max-dir-depth', default=4, type=int)
  parser.add_argument('-o',
                      '--output',
                      default="/fsx/devops/du_stats.sqlite",
                      type=str)
  return parser.parse_known_args()


def format_rusage(ru: resource.struct_rusage):
  ret = [
      "User  : {}s".format(ru.ru_utime),
      "System: {}s".format(ru.ru_stime),
      "MaxRSS: {}".format(humanize.naturalsize(ru.ru_maxrss * 1024,
                                               binary=True)),
  ]
  return ", ".join(ret)


def main():
  args, unk = parse_arguments()
  logger.info(args)
  print("Stats from {file}".format(file=args.filename))
  stats(args.filename, args.output, args.dirs_per_user, args.max_dir_depth)


if "__main__" in __name__:
  tstart = time.perf_counter()
  ret = main()
  tend = time.perf_counter()
  logger.info(
      "Total time={t:.3f}s, Resource use Self={rusage}, Children={child}".
      format(rusage=format_rusage(resource.getrusage(resource.RUSAGE_SELF)),
             child=format_rusage(resource.getrusage(resource.RUSAGE_CHILDREN)),
             t=(tend - tstart)))
  sys.exit(ret)
