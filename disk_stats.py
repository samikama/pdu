from __future__ import annotations
import gzip, pandas
from argparse import ArgumentParser as AP
import humanize
import os
from typing import List, Dict, TypeVar
from dataclasses import dataclass, field
import logging

logging.basicConfig(
    format=
    '%(asctime)s.%(msecs)03d %(name)s %(filename)s:%(funcName)s:%(lineno)d %(message)s',
    datefmt='%Y-%m-%d,%H:%M:%S',
    level=logging.INFO)
logger = logging.getLogger()


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
  files: List[TFile] = field(default_factory=list)
  dirs: dict[str, TDirectorySelf] = field(default_factory=dict)


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
      logging.info("processed", counter, "items")
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
    dd.files.append(TFile(fname, uid, gid, size, blocks))
    dd.self_size += size
    par = dd.parent
    while par:
      par.size += size
      par.blocks += blocks
      par = par.parent
  logging.info("Processed {ctr} files".format(ctr=counter))
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


def stats(filename, top_n, top_d, max_dir_depth):
  with gzip.open(filename, mode='rt', newline="") as input:
    df = pandas.read_csv(input, engine='pyarrow')
  df2 = df.groupby(['uid', 'gid'], sort=False)
  counts = df2['filename'].count().nlargest(top_n)
  sizes = df2['file_size'].sum().nlargest(top_n)
  total_size = df2['num_blocks'].sum().mul(512).nlargest(top_n)
  most_files = [
      "----------Users with most files------------",
      "{uid:>15s} {gid:>15s} {count:>30s}".format(uid="uid",
                                                  gid="gid",
                                                  count="Number of files")
  ]
  for p, c in counts.items():
    most_files.append("{uid:>15d} {gid:>15d} {count:>30s}".format(
        uid=p[0], gid=p[1], count=humanize.intcomma(c)))
  print("\n".join(most_files))

  most_volume = [
      "----------Users with most disk usage------------",
      "{uid:>15s} {gid:>15s} {count:>30s}: {dirs}".format(
          uid="uid", gid="gid", count="Total File Size", dirs="Top dirs")
  ]

  for p, c in sizes.items():
    tree = process_files(df[df['uid'] == p[0]].sort_values(by=['filename']))
    sorted_dirs = max_dirs(tree, max_dir_depth)
    top_dirs = [
        x for x in sorted_dirs[:min(len(sorted_dirs), top_d)] if x[1] != 0
    ]
    most_volume.append("{uid:>15d} {gid:>15d} {count:>30s}: {dirs}".format(
        uid=p[0],
        gid=p[1],
        count=humanize.naturalsize(c, binary=True),
        dirs=", ".join([
            "{dir}={size}".format(dir=x[0],
                                  size=humanize.naturalsize(x[1], binary=True))
            for x in top_dirs
        ])))

  print("\n".join(most_volume))

  most_actual = [
      "----------Users with most actual disk usage------------",
      "{uid:>15s} {gid:>15s} {count:>30s}".format(uid="uid",
                                                  gid="gid",
                                                  count="Actual Disk Usage")
  ]

  for p, c in total_size.items():
    most_actual.append("{uid:>15d} {gid:>15d} {count:>30s}".format(
        uid=p[0], gid=p[1], count=humanize.naturalsize(c, binary=True)))

  print("\n".join(most_actual))


def parse_arguments():
  parser = AP("DUStats")
  parser.add_argument('-f', '--filename', default=None, type=str, required=True)
  parser.add_argument('-t', '--top-n', default=20, type=int)
  parser.add_argument('-d', '--dirs-per-user', default=10, type=int)
  parser.add_argument('-m', '--max-dir-depth', default=4, type=int)

  return parser.parse_known_args()


def main():
  args, unk = parse_arguments()
  stats(args.filename, args.top_n, args.dirs_per_user, args.max_dir_depth)


if "__main__" in __name__:
  main()
